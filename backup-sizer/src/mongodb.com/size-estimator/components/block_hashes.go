package components
import (
	"os"
	"fmt"
	"github.com/willf/bloom"
	"io"
	"bytes"
	"compress/zlib"
	"path/filepath"
	"crypto/sha256"
	"math"
	"sync"
	"strconv"
)

const kb = 1024
const blockSizeBytes = 64 * kb
const hashLengthBytes = 32 + 1 // + 1 for new line

func getFileNamesInDir(dir string, readDir bool, out chan string, errC chan error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		fmt.Println(dir, err)
		errC <- err
		return
	}
	dir += "/"

	f, err := os.Open(dir)
	if err != nil {
		fmt.Println(f, err)
		errC <- err
		return
	}

	list, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		fmt.Println(f, err)
		errC <- err
		return
	}

	for _, fi := range list {
		if fi.IsDir() && readDir {    // todo -- how to handle directories?
			path := dir + fi.Name()
				getFileNamesInDir(path, false, out, errC)
			if err != nil {
				fmt.Println("error getting filenames", path)
				errC <- err
				return
			}
		} else if !fi.IsDir(){
			fileAbsPath := dir + fi.Name()
			out <- fileAbsPath
		}
	}
	return
}

type block struct {
	hash           []byte
	compressedSize int
	uncompressedSize int
}

func splitFiles(files chan string, out chan []byte, errC chan error) {
	for fi := range files {
		f, err := os.Open(fi)
		if err != nil {
			errC <- err
			return
		}
		for {
			b := make([]byte, blockSizeBytes)
			_, err := f.Read(b)
			if err != nil {
				if err == io.EOF {
					break
				}
				errC <- err
				return
			}
			out <- b
		}
		f.Close()
	}
	return
}

func hashAndCompressBlocks(blockBytes chan []byte, out, out2 chan block, iteration int, errC chan error) {
	hasher := sha256.New()

	for b := range blockBytes {
		_, err := hasher.Write(b)
		if err != nil {
			errC <- err
			return
		}
		hashed := []byte(hasher.Sum(nil))
		//		hashed := []byte(hex.EncodeToString(hasher.Sum(nil)))
		compressedLen := compressBlock(b)
		b := block{hashed, compressedLen, len(b)}
		out <- b
			out2 <- b
	}
	return
}

func compressBlock(block []byte) (int) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	w.Write(block)
	w.Close()
	return b.Len()
}

func writeHash(hashes chan block, file *os.File, crChan chan float64, errC chan error) {
	defer func() {
		close(crChan)
	}()

	compressedTotal := 0
	uncompressedTotal := 0

	for h := range hashes {
//		 fmt.Println("Receiving hashed and compressed block")
		compressedTotal += h.compressedSize
		uncompressedTotal += h.uncompressedSize

		_, err := file.Write(h.hash)
		if err != nil {
			fmt.Println("WOMP ", err)
			errC <- err
			return
		}
		file.WriteString("\n")
	}
	cr := float64(uncompressedTotal) / float64(compressedTotal)
	crChan <- cr
	return
}

func loadPrevHashes(fileName string, m, k uint) (*bloom.BloomFilter, error) {
	// figure out these parameters
	bloomFilter := bloom.New(m, k)

	// add previous hashes to bloom filter
	prevFile, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	for {
		h := make([]byte, hashLengthBytes)
		_, err := prevFile.Read(h)
		if err != nil {
			if err == io.EOF {
				return bloomFilter, nil
			}
			return nil, err
		}
		bloomFilter.Add(h[:len(h)-1])
	}
	return bloomFilter, nil
}

func compareHashes(filter *bloom.BloomFilter, curHashes chan block) float64 {
	dupHashes := 0
	totalHashes := 0
	for h := range curHashes {
		totalHashes++
		if filter.Test(h.hash) {
			dupHashes++
		}
	}
	return float64(dupHashes) / float64(totalHashes)
}

type BlockStats struct {
	DedupRate 				float64
	DataCompressionRatio 	float64
}

func checkExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func GetBlockHashes(dbpath string, hashpath string, iteration int) (*BlockStats, error) {
	const numFileSplitters = 1
	const numblockHashers = 1

	dbpath, err := filepath.Abs(dbpath)
	if err != nil {
		fmt.Println(dbpath, err)
		return nil, err
	}
	dbpath = dbpath + "/"

	hashpath, err = filepath.Abs(hashpath)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	hashpath += "/"
	exists, err := checkExists(hashpath)
	if err != nil {
		return nil, err
	}
	if !exists {
		os.Mkdir(hashpath, 0777)
	}

	fnCh := make(chan string)
	errCh := make(chan error)
	defer func() {
		close(errCh)
	}()

	// load up all the filenames into fileNamesChannel
	go func() {
		getFileNamesInDir(dbpath, true, fnCh, errCh)
		close(fnCh)
	} ()

	blocksCh := make(chan []byte)

	var blocksWG sync.WaitGroup
	for i := 0; i < numFileSplitters; i++ {
		go func() {
			blocksWG.Add(1)
			splitFiles(fnCh, blocksCh, errCh)
			blocksWG.Done()
		}()
	}
	go func() {
		blocksWG.Wait()
		close(blocksCh)
	} ()


	hashCh1 := make(chan block)
	hashCh2 := make(chan block)

	var hashWG sync.WaitGroup
	for i := 0; i < numblockHashers; i++ {
		go func() {
			hashWG.Add(1)
			hashAndCompressBlocks(blocksCh, hashCh1, hashCh2, iteration, errCh)
			hashWG.Done()
		} ()
	}
	go func() {
		hashWG.Wait()
		close(hashCh1)
		close(hashCh2)
	} ()

	var dedupRate, cr float64

	hashFileName := strconv.AppendInt([]byte(hashpath), int64(iteration), 10)
	hashFile, err := os.Create(string(hashFileName))
	if err != nil {
		return nil, fmt.Errorf("Failed creating file %s to write hashes, iteration %d. Err: %v",
			string(hashFileName), iteration, err)
	}

	crResChan := make(chan float64)
	go writeHash(hashCh1, hashFile, crResChan, errCh)

	// todo: CLEAN THIS UP
	if iteration != 0 {
		fmt.Println("Hey ;)")
		prevHashFileName := string(strconv.AppendInt([]byte(hashpath), int64(iteration - 1), 10))
		m := uint(5 * 1000)
		k := uint(5)
		var compareWG sync.WaitGroup
		compareWG.Add(1)
		go func() {
			bloomFilter, err := loadPrevHashes(prevHashFileName, m, k)
			if err != nil {
				errCh <- fmt.Errorf("Failed loading previous hashes from %s, iteration %d. Err: %v",
					string(strconv.AppendInt([]byte(dbpath), int64(iteration - 1), 10)), iteration, err)
				return
			}
			dedupRate = compareHashes(bloomFilter, hashCh2)
			compareWG.Done()
		} ()
		compareWG.Wait()
	} else {
		for _ = range hashCh2 {
		}
		dedupRate = math.NaN()
	}

	// need to check errCh

	cr = <- crResChan

	return &BlockStats{
		DedupRate:dedupRate,
		DataCompressionRatio: cr,
	}, nil
}

