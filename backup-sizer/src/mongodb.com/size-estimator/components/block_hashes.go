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
	"math"
)

const kb = 1024
const blockSizeBytes = 64 * kb
const hashSize = 65
const bfErrorRate = 0.01

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
	hash             string
	compressedSize   int
	uncompressedSize int
}

func splitFiles(fname string) (func() ([]byte, error), error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	fun := func() ([]byte, error) {
		b := make([]byte, blockSizeBytes)
		_, err := f.Read(b)
		if err != nil {
			if err == io.EOF {
				f.Close()
				return nil, nil
			}
			return nil, err
		}
		return b, nil
	}
	return fun, nil
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

func loadPrevHashes(fileName string) (*bloom.BloomFilter, error) {
	exists, err := checkExists(fileName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return bloom.New(0, 0), nil
	}

	n, err := numHashes(fileName)
	if err != nil {
		return nil, err
	}

	m, k := bloomFilterParams(n, bfErrorRate)

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

type BlockStats struct {
	DedupRate            float64
	DataCompressionRatio float64
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

// fn stores hashes from previous iteration
func numHashes(fn string) (int64, error) {
	fi, err := os.Stat(fn)
	if err != nil {
		return 0, err
	}
	size := fi.Size()
	return size / hashSize, nil
}

// n is the size of the set, p is the false positive rate
// calculated as explained in http://www.cs.utexas.edu/users/lam/386p/slides/Bloom%20Filters.pdf
func bloomFilterParams(n int64, p float64) (m, k uint) {
	c := 0.6185 // 0.5 ^ (m/n * ln 2) ~= 0.6185 ^ (m/n)
	nf := float64(n)

	mf := math.Log(p) / math.Log(c) * nf
	m = uint(math.Ceil(mf))

	kf := mf/nf * math.Log(2)
	k = uint(math.Floor(kf))

	return
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


	prevHashFileName := string(strconv.AppendInt([]byte(hashpath), int64(iteration - 1), 10))
	bloomFilter, err := loadPrevHashes(prevHashFileName)
	if err != nil {
		return nil, fmt.Errorf("Failed loading previous hashes from %s, iteration %d. Err: %v",
			string(strconv.AppendInt([]byte(dbpath), int64(iteration - 1), 10)), iteration, err)
	}

	hashFileName := strconv.AppendInt([]byte(hashpath), int64(iteration), 10)
	hashFile, err := os.Create(string(hashFileName))
	if err != nil {
		return nil, fmt.Errorf("Failed creating file %s to write hashes, iteration %d. Err: %v",
			string(hashFileName), iteration, err)
	}

	errCh := make(chan error)

	finalErr := make(chan error)

	go func() {
		const maxErrors = 5
		errors := make([]byte, 0)
		numErrors := 0

		for {
			e := <-errCh
			if e == nil {
				break
			}
			numErrors++
			s := fmt.Sprintf("Error %d: %v\n", numErrors, e.Error())
			if numErrors <= maxErrors {
				errors = append(errors, []byte(s) ...)
			}
		}
		s := fmt.Sprintf("Encountered %d errors. Printing first %d.\n", numErrors, maxErrors)
		errors = append([]byte(s), errors ... )

		if numErrors > 0 {
			finalErr <- fmt.Errorf(string(errors))
		}
		close(finalErr)
	}()

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
			defer blocksWG.Done()
			for {
				fname := <-fnCh
				if fname == "" {
					break
				}
				blocks, err := splitFiles(fname)
				if err != nil {
					errCh <- err
					break
				}
				for {
					block, err := blocks()
					if block == nil {
						if err != nil {
							errCh <- err
						}
						break
					}
					blocksCh <- block
				}
			}
		}()
	}

	for i := 0; i < numBlockHashers; i++ {
		hashWG.Add(1)
		go func() {
			defer hashWG.Done()
			for {
				b := <-blocksCh
				if b == nil {
					return
				}

				hashed, err := hashAndCompressBlocks(b)
				if err != nil {
					errCh <- err
				} else {
					hashCh <- hashed
				}
			}
		}()
	}

	go func() {
		compressedTotal := 0
		uncompressedTotal := 0
		totalHashes := 0
		totalDupeCount := 0

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
			}

			if bloomFilter.TestString(h.hash) {
				totalDupeCount++
			}
		}

		cr := float64(uncompressedTotal) / float64(compressedTotal)
		dedupRate := float64(totalDupeCount) / float64(totalHashes)
		crResChan <- BlockStats{dedupRate, cr}
		return
	}()

	blocksWG.Wait()
	close(blocksCh)

	hashWG.Wait()
	close(hashCh)

	res := <-crResChan
	close(errCh)

	err = <-finalErr
	if err != nil {
		return nil, err
	}
	return &res, nil
}



