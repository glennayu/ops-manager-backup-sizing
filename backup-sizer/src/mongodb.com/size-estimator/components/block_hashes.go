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
	"encoding/hex"
	"bufio"
)

const kb = 1024
const blockSizeBytes = 64 * kb
const hashLengthBytes = 64 + 1 // + 1 for new line

func readFileNamesToChannel(dir string) (fnCh chan string, errCh chan error) {
	errCh = make(chan error)

	files, err := getFilesInDir(dir, true)
	if err != nil {
		errCh <- err
		return
	}

	fnCh = make(chan string, len(files))
	defer close(fnCh)

	for _, fname := range files {
		fnCh <- fname
	}
	return
}

type Block struct {
	hash           string
	compressedSize int
	uncompressedSize int
}

func splitFiles(files chan string, out chan []byte, errC chan error) {
	for {
		//	for fi := range files {
		fname := <- files
		if fname == "" {
			return
		}

		f, err := os.Open(fname)
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

func hashAndCompressBlocks(blockBytes chan []byte, out, out2 chan Block, iteration int, errC chan error) {
	hasher := sha256.New()

	for {
		b := <- blockBytes
		if b == nil {
			return
		}
		_, err := hasher.Write(b)
		if err != nil {
			errC <- err
			return
		}
		hashed := hex.EncodeToString(hasher.Sum(nil))
		compressedLen, err := getCompressedSize(b)
		if err != nil {
			errC <- err
			return
		}
		block := Block{hashed, compressedLen, len(b)}
		out <- block
		out2 <- block
		hasher.Reset()
	}
	return
}

func getCompressedSize(block []byte) (int, error) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, err := w.Write(block)
	if err != nil {
		return 0, err
	}
	err = w.Close()
	if err != nil {
		return 0, err
	}
	return b.Len(), nil
}

func writeHash(hashes chan Block, file *os.File, crChan chan float64, errC chan error) {
	defer func() {
		close(crChan)
	}()

	compressedTotal := 0
	uncompressedTotal := 0

	for {
		h, open := <- hashes
		if !open {
			break
		}

		compressedTotal += h.compressedSize
		uncompressedTotal += h.uncompressedSize

		_, err := file.WriteString(h.hash + "\n")
		if err != nil {
			errC <- err
			return
		}
	}
	cr := float64(uncompressedTotal) / float64(compressedTotal)
	crChan <- cr
	return
}

func loadPrevHashes(fileName string, m, k uint) (*bloom.BloomFilter, error) {
	prevFile, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(prevFile)

	bloomFilter := bloom.New(m, k)

	for scanner.Scan() {
		h := scanner.Text()
		bloomFilter.AddString(h)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return bloomFilter, nil
}


func compareHashes(filter *bloom.BloomFilter, curHashes chan Block) float64 {
	dupHashes := 0
	totalHashes := 0
	for {
		h, open := <- curHashes
		if !open {
			break
		}
		totalHashes++
		if filter.TestString(h.hash) {
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
	const numFileSplitters = 3
	const numBlockHashers = 3

	dbpath, err := filepath.Abs(dbpath)
	if err != nil {
		return nil, err
	}
	dbpath = dbpath + "/"

	hashpath, err = filepath.Abs(hashpath)
	if err != nil {
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

	// load up all the filenames into fnCh
	fnCh, errCh := readFileNamesToChannel(dbpath)

	blocksCh := make(chan []byte)
	hashCh1 := make(chan Block)
	hashCh2 := make(chan Block)
	crResChan := make(chan float64, 1)

	var blocksWG sync.WaitGroup
	var hashWG sync.WaitGroup
	var compareWG sync.WaitGroup


	for i := 0; i < numFileSplitters; i++ {
		blocksWG.Add(1)
		go func() {
			splitFiles(fnCh, blocksCh, errCh)
			blocksWG.Done()
		}()
	}


	for i := 0; i < numBlockHashers; i++ {
		hashWG.Add(1)
		go func() {
			hashAndCompressBlocks(blocksCh, hashCh1, hashCh2, iteration, errCh)
			hashWG.Done()
		} ()
	}

	hashFileName := strconv.AppendInt([]byte(hashpath), int64(iteration), 10)
	hashFile, err := os.Create(string(hashFileName))
	if err != nil {
		return nil, fmt.Errorf("Failed creating file %s to write hashes, iteration %d. Err: %v",
			string(hashFileName), iteration, err)
	}
	go writeHash(hashCh1, hashFile, crResChan, errCh)

	var dedupRate float64

	compareWG.Add(1)
	go func() {
		if iteration != 0 {
			prevHashFileName := string(strconv.AppendInt([]byte(hashpath), int64(iteration - 1), 10))

			// figure out these parameters
			m := uint(5 * 4000)
			k := uint(5)
			bloomFilter, err := loadPrevHashes(prevHashFileName, m, k)
			if err != nil {
				errCh <- fmt.Errorf("Failed loading previous hashes from %s, iteration %d. Err: %v",
					string(strconv.AppendInt([]byte(dbpath), int64(iteration - 1), 10)), iteration, err)
				return
			}
			dedupRate = compareHashes(bloomFilter, hashCh2)
		} else {
			for _ = range hashCh2 {
			}
			dedupRate = math.NaN()
		}
		compareWG.Done()
	} ()


	blocksWG.Wait()
	close(blocksCh)

	hashWG.Wait()
	close(hashCh1)
	close(hashCh2)

	compareWG.Wait()

	cr := <- crResChan

	select {
	case err = <- errCh :
		return nil, err
	default :
		return &BlockStats{
			DedupRate:dedupRate,
			DataCompressionRatio: cr,
		}, nil
	}
}


