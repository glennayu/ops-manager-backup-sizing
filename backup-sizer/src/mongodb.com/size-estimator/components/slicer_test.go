package components

import (
	"bytes"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

const extraBytes = 30

func generateDocs(output chan<- *bson.D, numBytes uint64) {
	var bytesGenerated uint64 = 0
	bytesVal := bytes.Repeat([]byte{0}, 5*1024)

	for bytesGenerated < numBytes {
		toAdd := &bson.D{
			{"_id", bytesGenerated},
			{"bytes", bytesVal},
		}

		output <- toAdd
		bytesGenerated += 5 * 1024
	}

	close(output)
}

func readOutputSlices(input <-chan *Slice) (int, int, int, []*Slice) {
	numSlices := 0
	numDocs := 0
	compressedSize := 0
	slices := make([]*Slice, 0)

	for slice := range input {
		numSlices++
		numDocs += slice.NumDocs
		compressedSize += slice.Buffer.Len()
		slices = append(slices, slice)
	}

	return numSlices, numDocs, compressedSize, slices
}

const (
	minSlice = 13 * 1024
	maxSlice = 20 * 1024
	timeout  = 100 * time.Millisecond
)

func TestDocumentSlicer(test *testing.T) {
	var startSyncTime OplogID
	slicer := NewSlicer(100, 100,
		minSlice, maxSlice,
		new(GzipCompressor),
		timeout,
		&startSyncTime,
		)


	slice := slicer.NewSlice()
	if slice.FirstDoc == nil || slice.LastDoc == nil {
		test.Fatalf("Slice not initialized")
	}

	source := slicer.Docs
	destination := slicer.Slices
	resChan := make(chan float32)

	/* Generate 100KB out of ~5KB docs */
	go generateDocs(source, 100*1024)
	go slicer.Stream(resChan)

	cr := <- resChan
	numSlices, numDocs, compressedSize, _ := readOutputSlices(destination)
	if numSlices != 7 {
		test.Errorf("Expected slices: 7. Received: %d", numSlices)
	}

	if numDocs != 20 {
		test.Errorf("Expected docs: 20. Received: %d", numDocs)
	}

	totalSize := float32(100*1024 + extraBytes * numDocs)
	expectedCR := totalSize / float32(compressedSize)
	// sanity check - reasonable bounds on
	if cr < 0.95 * expectedCR || cr > 1.05 * expectedCR {
		test.Errorf("Expected compression ratio: %f. Received: %f",
		expectedCR,
		cr)
	}

}

func TestLargeDocuments(test *testing.T) {
	// Given a slicer that maxes out at 3KiB slices, a
	// series of 5, 5KiB documents should result in 5 consecutive
	// slices with data tailed by an empty slice.
	slicer := NewSlicer(100, 100,
		1024, 3*1024,
		new(GzipCompressor),
		timeout,
		nil,
	)

	source := slicer.Docs
	destination := slicer.Slices

	resChan := make(chan float32)

	// Generate 25KiB out of ~5KiB docs
	go generateDocs(source, 25*1024)
	go slicer.Stream(resChan)

	cr := <- resChan
	numSlices, numDocs, compressedSize, slices := readOutputSlices(destination)
	if numSlices != 6 {
		test.Errorf("Expected slices: 6. Received: %d", numSlices)
	}

	for idx, slice := range slices {
		expectedDocs := 1
		// All slices should have 1 document except the last slice
		// with 0.
		if idx == len(slices)-1 {
			expectedDocs = 0
		}

		if slice.NumDocs != expectedDocs {
			test.Errorf("Incorrect number of documents. Idx: %v Received: %v Expected: %v",
				idx, slice.NumDocs, expectedDocs)
		}
	}

	if numDocs != 5 {
		test.Errorf("Expected docs: 5. Received: %d", numDocs)
	}

	totalSize := float32(25*1024 + extraBytes * numDocs) // extra bytes per doc
	// sanity check
	if cr * float32(compressedSize) != totalSize {
		test.Errorf("Expected compression ratio: %f. Received: %f",
			totalSize / float32(compressedSize),
			cr)
	}
}
