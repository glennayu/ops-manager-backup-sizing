package components

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"sync"
	"time"
	"github.com/golang/snappy/snappy"
	"github.com/mongodb/slogger/v1/slogger"
	"gopkg.in/mgo.v2/bson"
)

// A Compressor is a reusable, not necessarily threadsafe interface
// that will consume a series of byte slice inputs (bson documents) and
// output compressed bytes.
// Compressor types declared and implemented below.
type Compressor interface {
	NewCompressor() error
	Write(data []byte) (int, error)
	Bytes() ([]byte, error)
	Name() string
}

type Slicer struct {
	Docs         chan *bson.D
	Slices       chan *Slice
	errorChan    chan error
	shutdownChan chan int
	SlicesMade   int
	MinSliceSize int
	MaxSliceSize int
	Compressor   Compressor
	Timeout      time.Duration
	bufferDoc    *bson.D
	lastDocSent  *bson.D
	slogger.Logger
	sync.Mutex
}

// `maxSliceSize` (typically set to 15MB) is intended to keep
// slices below the 16MB limit as metadata can (and compression can
// sometimes) increase the size of what's inserted into the
// database. This value is not enforced on the first document of a
// slice.
func NewSlicer(inputCapacity int,
outputCapacity int,
minSliceSize int,
maxSliceSize int,
compressor Compressor,
timeout time.Duration,
oplogStart *OplogID,
//logger slogger.Logger,
) *Slicer {

	// Initialize FirstDoc and LastDoc with reasonable dummy values
	var lastDocSent *bson.D
	if oplogStart != nil {
		lastDocSent = &bson.D{{"ts", oplogStart.TS}, {"h", oplogStart.Hash}}
	}

	return &Slicer{
		Docs:         make(chan *bson.D, inputCapacity),
		Slices:       make(chan *Slice, outputCapacity),
		errorChan:    make(chan error, 1),
		shutdownChan: make(chan int, 1),
		MinSliceSize: minSliceSize,
		MaxSliceSize: maxSliceSize,
		Compressor:   compressor,
		Timeout:      timeout,
		lastDocSent:  lastDocSent,
//		Logger:       logger,
	}
}

func (slicer *Slicer) Close() {
	// Graceful shutdown by signaling there are no more
	// incoming docs and letting the channels naturally
	// drain.
	close(slicer.Docs)
}

func (slicer *Slicer) Kill() {
	// Hard shutdown by closing the channels as soon
	// as possible. The functions responsible for
	// pushing to channels will no longer accept
	// writes, speeding up the time until they
	// see the shutdown channel.

	// Holding a lock since closing a channel
	// that is already closed causes a panic.
	slicer.Lock()
	defer slicer.Unlock()

	select {
	case <-slicer.shutdownChan:
		return
	default:
		close(slicer.shutdownChan)
	}
}

func (slicer *Slicer) Wait() error {
	return <-slicer.errorChan
}

func (slicer *Slicer) WriteDoc(doc *bson.D) bool {
	// Optimization to let clients know to stop
	// sending docs. Since there is no order on
	// selects it would otherwise not hit the
	// shutdown channel possibly until the Docs
	// channel was full.
	select {
	case <-slicer.shutdownChan:
		return false
	default:
	}

	select {
	case slicer.Docs <- doc:
		return true
	case <-slicer.shutdownChan:
		return false
	}
}

func (slicer *Slicer) writeSlice(slice *Slice) bool {
	// Optimization to let clients know to stop
	// sending docs. Since there is no order on
	// selects it would otherwise not hit the
	// shutdown channel possilbly until the Slices
	//channel was full.
	select {
	case <-slicer.shutdownChan:
		return false
	default:
	}

	select {
	case slicer.Slices <- slice:
		<- slicer.Slices
		return true
	case <-slicer.shutdownChan:
		return false
	}
}

func (slicer *Slicer) Stream(sliceChan chan float32) {
	defer func() {
		// Let consumers of this channel know that there
		// aren't any more slices coming.
		close(slicer.Slices)
		close(sliceChan)
	}()

	uncompressedSize := 0
	compressedSize := 0

	for {
		slice, moreSlices, err := slicer.Slice()
		if err != nil {
			slicer.errorChan <- err
			slicer.Kill()
			return
		}

		if slice != nil && slicer.writeSlice(slice) == false {
			break
		}

		uncompressedSize += slice.UnzippedSize
		compressedSize += len(slice.Buffer.Bytes())

		if moreSlices == false {
			break
		}

		if len(slicer.Docs) != 0 {
			slicer.Logf(slogger.DEBUG, "%v document(s) in channel to be compressed", len(slicer.Docs))
		}
	}
//	fmt.Println("slicer pushing val to sliceChan")
	sliceChan <- float32(uncompressedSize) / float32(compressedSize)
}

func (slicer *Slicer) Consume(consumeFunc SliceConsumer, maxSlicesBeforeSend int) {
	// After the last slice is sent (this function is exiting)
	// it is safe to close to channel because there can not be
	// anymore errors.
	// Any error that occurs inside this funciton will be written
	// to the channel beforehand.
	defer close(slicer.errorChan)

	// Send the ready slices whenever `maxSlicesBeforeSend` is
	// fulfilled or five seconds since the last slice was produced,
	// whichever comes first.
	var slices []*Slice = nil
	for {
		select {
		case slice, channelOpen := <-slicer.Slices:
			if channelOpen == false {
				return
			}

			slices = append(slices, slice)
			if len(slices) < maxSlicesBeforeSend {
				continue
			}

			if err := consumeFunc(slices); err != nil {
				slicer.errorChan <- err
				slicer.Kill()
				return
			}

			slices = nil
		case <-time.After(5 * time.Second):
			if slices == nil {
				continue
			}

			if err := consumeFunc(slices); err != nil {
				slicer.errorChan <- err
				slicer.Kill()
				return
			}

			slices = nil
		case <-slicer.shutdownChan:
			return
		}

		if len(slicer.Slices) != 0 {
			slicer.Logf(slogger.DEBUG, "%v slice(s) in channel to be pushed", len(slicer.Slices))
		}
	}
}

func (slicer *Slicer) NewSlice() *Slice {
	ret := &Slice{CompressionFormat: slicer.Compressor.Name()}

	if slicer.lastDocSent != nil {
		ret.FirstDoc = slicer.lastDocSent
		ret.LastDoc = slicer.lastDocSent
	}

	return ret
}

// Return a new slice, any errors and whether the slicer should
// continue operating
func (slicer *Slicer) Slice() (*Slice, bool, error) {
	newSlice := slicer.NewSlice()
	newSlice.SliceNum = slicer.SlicesMade
	slicer.SlicesMade += 1

	compressor := slicer.Compressor
	compressor.NewCompressor()
	defer func() {
		bytes, err := compressor.Bytes()
		if err != nil {
			panic(fmt.Sprintf("Unexpected compression error. Err: %v", err))
		}

		newSlice.Write(bytes)
	}()

	if slicer.bufferDoc != nil {
		docBytes, err := bson.Marshal(slicer.bufferDoc)
		if err != nil {
			return nil, false, slogger.NewStackError("Error marshalling a document. Err: %v", err)
		}

		compressor.Write(docBytes)
		newSlice.addDoc(docBytes, slicer.bufferDoc)
		slicer.bufferDoc = nil
	}

	startTime := time.Now()
	for newSlice.UnzippedSize < slicer.MinSliceSize &&
	time.Since(startTime) < slicer.Timeout {
		var doc *bson.D
		var channelOpen bool

		// Asynchronously poll the input channel.
		select {
		case doc, channelOpen = <-slicer.Docs:
			if channelOpen == false {
				return newSlice, false, nil
			}
		// Still need to break out of the select when the timeout is reached
		case <-time.After(slicer.Timeout - time.Since(startTime)):
			continue
		case <-slicer.shutdownChan:
			return nil, false, nil
		}

		docBytes, err := bson.Marshal(doc)
		if err != nil {
			return nil, false, slogger.NewStackError("Error marshalling a document. Err: %v", err)
		}

		if newSlice.NumDocs > 0 &&
		newSlice.UnzippedSize+len(docBytes) > slicer.MaxSliceSize {
			slicer.bufferDoc = doc
			break
		}

		compressor.Write(docBytes)
		newSlice.addDoc(docBytes, doc)
	}

	slicer.lastDocSent = newSlice.LastDoc
	return newSlice, true, nil
}

type GzipCompressor struct {
	buffer *bytes.Buffer
	writer *gzip.Writer
}

func (self *GzipCompressor) NewCompressor() (err error) {
	self.buffer = new(bytes.Buffer)
	self.writer, err = gzip.NewWriterLevel(self.buffer, gzip.BestSpeed)
	return err
}

func (self *GzipCompressor) Write(data []byte) (int, error) {
	return self.writer.Write(data)
}

func (self *GzipCompressor) Bytes() ([]byte, error) {
	if err := self.writer.Close(); err != nil {
		return nil, slogger.NewStackError("Error trying to flush gzip bytes. Err: %s", err)
	}

	return self.buffer.Bytes(), nil
}

func (self *GzipCompressor) Name() string {
	return "gzip"
}

type SnappyCompressor struct {
	concatOplogs *bytes.Buffer
}

func (self *SnappyCompressor) NewCompressor() error {
	self.concatOplogs = new(bytes.Buffer)
	return nil
}

func (self *SnappyCompressor) Write(data []byte) (int, error) {
	return self.concatOplogs.Write(data)
}

func (self *SnappyCompressor) Bytes() ([]byte, error) {
	return snappy.Encode(nil, self.concatOplogs.Bytes())
}

func (self *SnappyCompressor) Name() string {
	return "snappy"
}
