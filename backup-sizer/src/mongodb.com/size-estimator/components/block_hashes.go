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
	"sync"
	"strconv"
	"encoding/hex"
	"bufio"
)

const kb = 1024
const blockSizeBytes = 64 * kb

func readFileNamesToChannel(dir string, errCh chan error) (fnCh chan string) {
	files, err := getFilesInDir(dir, true)
	if err != nil {
		errCh <- err
		fnCh = make(chan string)
		close(fnCh)
		return
	}
	fnCh = make(chan string, len(files))
	defer close(fnCh)


	for _, fname := range files {
		fnCh <- fname
	}
	return fnCh
}

type Block struct {
	hash           string
	compressedSize int
	uncompressedSize int
}

func splitFiles(fname string) ([][]byte, error) {
	blocks := make([][]byte, 0)
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	for {
		b := make([]byte, blockSizeBytes)
		_, err := f.Read(b)
		if err != nil {
			if err == io.EOF {
				break
			}
		}
		blocks = append(blocks, b)
	}
	f.Close()
	return blocks, nil
}

func hashAndCompressBlocks(b []byte) (Block, error) {
	hasher := sha256.New()

	_, err := hasher.Write(b)
	if err != nil {
		return Block{}, err
	}
	hashed := hex.EncodeToString(hasher.Sum(nil))
	compressedLen, err := getCompressedSize(b)
	if err != nil {
		return Block{}, err
	}
	block := Block{hashed, compressedLen, len(b)}

	return block, nil
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

func writeHash(h Block, file *os.File) error {
	_, err := file.WriteString(h.hash + "\n")
	if err != nil {
		return err
	}

	return nil
}

func loadPrevHashes(fileName string, m, k uint) (*bloom.BloomFilter, error) {
	exists, err := checkExists(fileName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return bloom.New(m, k), nil
	}

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


func compareHashes(filter *bloom.BloomFilter, h Block) bool {
	return filter.TestString(h.hash)
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

	errCh := make(chan error)

	finalErr := make(chan error)

	go func() {
		const maxErrors = 5
		errors := []byte("Errors:\n")
		numErrors := 0

		for {
			e := <- errCh
			if e == nil {
				break
			}
			numErrors++
			s := fmt.Sprintf("Error %d: %v\n", numErrors, e.Error())
			if numErrors <= maxErrors {
				errors = append(errors, []byte(s) ...)
			}
		}
		if numErrors > maxErrors {
			s := fmt.Sprintf("Total %d errors; not printing rest\n", numErrors)
			errors = append(errors, []byte(s) ...)
		}
		finalErr <- fmt.Errorf(string(errors))
		close(finalErr)
		return
	} ()


	// load up all the filenames into fnCh
	fnCh := readFileNamesToChannel(dbpath, errCh)

	blocksCh := make(chan []byte)
	hashCh := make(chan Block)
	crResChan := make(chan BlockStats)

	var blocksWG sync.WaitGroup
	var hashWG sync.WaitGroup

	for i := 0; i < numFileSplitters; i++ {
		blocksWG.Add(1)
		go func() {
			for {
				fname := <-fnCh
				if fname == "" {
					break
				}

				blocks, err := splitFiles(fname)
				if err != nil {
					fmt.Println(err)
					errCh <- err
					break
				}
				for _, block := range blocks {
					blocksCh <- block

				}
			}
			blocksWG.Done()
		}()
	}

	for i := 0; i < numBlockHashers; i++ {
		hashWG.Add(1)
		go func() {
			for {
				b := <-blocksCh
				if b == nil {
					break
				}

				hashed, err := hashAndCompressBlocks(b)
				if err != nil {
					errCh <- err
					break
				}
				hashCh <- hashed
			}
			hashWG.Done()
		} ()
	}

	hashFileName := strconv.AppendInt([]byte(hashpath), int64(iteration), 10)
	hashFile, err := os.Create(string(hashFileName))
	if err != nil {
		e := fmt.Errorf("Failed creating file %s to write hashes, iteration %d. Err: %v",
			string(hashFileName), iteration, err)
		errCh <- e
	}

	go func() {
		compressedTotal := 0
		uncompressedTotal := 0
		totalHashes := 0
		totalDupeCount := 0

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

		for {
			h, open := <-hashCh
			if !open {
				break
			}
			totalHashes++
			compressedTotal += h.compressedSize
			uncompressedTotal += h.uncompressedSize

			err := writeHash(h, hashFile)
			if err != nil {
				errCh <- err
				break
			}

			if compareHashes(bloomFilter, h) {
				totalDupeCount++
			}
		}

		cr := float64(uncompressedTotal) / float64(compressedTotal)
		dedupRate := float64(totalDupeCount) / float64(totalHashes)
		crResChan <- BlockStats{dedupRate, cr}
		return
	} ()

	blocksWG.Wait()
	close(blocksCh)

	hashWG.Wait()
	close(hashCh)

	close(errCh)


	err = <- finalErr
	if err != nil {
		return nil, err
	}
	res := <- crResChan
	return &res, nil
}



