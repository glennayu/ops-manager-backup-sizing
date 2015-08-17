package components

import (
	"bufio"
	"compress/zlib"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/willf/bloom"
	"gopkg.in/mgo.v2"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

const kb = 1024
const blockSizeBytes = 64 * kb
const hashSize = 65

func readFileNamesToChannel(session *mgo.Session, errCh chan error) (fnCh chan string) {
	files, err := GetDBFiles(session)
	if err != nil {
		errCh <- err
		fnCh = make(chan string)
		close(fnCh)
		return
	}
	fnCh = make(chan string, len(*files))
	defer close(fnCh)

	for _, fname := range *files {
		fnCh <- fname
	}
	return fnCh
}

func splitFiles(fname string) (func([]byte) ([]byte, error), error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	fun := func(b []byte) ([]byte, error) {
		n, err := f.Read(b)
		if err != nil {
			if err == io.EOF {
				f.Close()
				return nil, nil
			}
			return nil, err
		}
		return b[:n], nil // remove padding at end of file so smaller blocksizes don't have multiple empty blocks
	}
	return fun, nil
}

type Block struct {
	blockSize        int
	hash             string
	compressedSize   int
	uncompressedSize int
}

func hashAndCompressBlocks(b []byte, blocksize int) (*[]Block, error) {
	hasher := sha256.New()

	blocks := make([]Block, int64(math.Ceil(float64(len(b))/float64(blocksize))))

	bi := 0
	i := 0
	for bi < len(b) {
		blockSizeSlice := int(math.Min(float64(blocksize), float64(len(b)-bi)))
		slice := b[bi : bi+blockSizeSlice]

		_, err := hasher.Write(slice)
		if err != nil {
			return nil, err
		}
		hashed := hex.EncodeToString(hasher.Sum(nil))
		compressedLen, err := getCompressedSize(slice)
		if err != nil {
			return nil, err
		}

		block := Block{blocksize, hashed, compressedLen, len(slice)}
		blocks[i] = block

		hasher.Reset()
		bi += blockSizeSlice
		i++
	}

	return &blocks, nil
}

type CountingBuffer struct {
	count int
}

func (cb *CountingBuffer) Write(p []byte) (n int, err error) {
	cb.count += len(p)
	return len(p), nil
}

func (cb *CountingBuffer) Len() (n int) {
	return cb.count
}

func getCompressedSize(block []byte) (int, error) {
	var cb CountingBuffer
	w := zlib.NewWriter(&cb)
	_, err := w.Write(block)
	if err != nil {
		return 0, err
	}
	err = w.Close()
	if err != nil {
		return 0, err
	}
	return cb.Len(), nil
}

func writeHash(h Block, file *os.File) error {
	_, err := file.WriteString(h.hash + "\n")
	if err != nil {
		return err
	}
	return nil
}

func loadPrevHashes(fileName string, falsePosRate float64) (*bloom.BloomFilter, error) {
	exists, err := CheckExists(fileName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return bloom.New(10, 10), nil
	}

	n, err := numHashes(fileName)
	if err != nil {
		return nil, err
	}

	m, k := bloomFilterParams(n, falsePosRate)

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

type AllBlockSizeStats map[int]*BlockStats

type BlockStats struct {
	compressedTotal      int
	uncompressedTotal    int
	totalHashes          int
	totalDupeCount       int
	DedupRate            float64
	DataCompressionRatio float64
}

func CheckExists(path string) (bool, error) {
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
	if n <= 0 || p <= 0 || p >= 1 {
		return 1, 0
	}
	c := 0.6185 // 0.5 ^ (m/n * ln 2) ~= 0.6185 ^ (m/n)
	nf := float64(n)

	mf := math.Log(p) / math.Log(c) * nf
	m = uint(math.Ceil(mf))

	kf := mf / nf * math.Log(2)
	k = uint(math.Floor(kf))

	return
}

func GetBlockHashes(opts *BackupSizingOpts, blocksizes []int, iteration int) (*AllBlockSizeStats,
	error) {
	const numFileSplitters = 3
	const numBlockHashers = 3

	hashpath := opts.HashDir
	bfFalsePos := opts.FalsePosRate

	sort.Ints(blocksizes)
	maxBlockSize := blocksizes[len(blocksizes)-1] // largest block size
	bloomFilters := make(map[int]*bloom.BloomFilter)
	hashFiles := make(map[int]*os.File)

	dbpath, err := GetDbPath(opts.GetSession())
	if err != nil {
		return nil, err
	}
	dbpath, err = filepath.Abs(dbpath)
	if err != nil {
		return nil, err
	}
	dbpath = dbpath + "/"

	hashpath, err = filepath.Abs(hashpath)
	if err != nil {
		return nil, err
	}
	hashpath += "/"

	for _, s := range blocksizes {
		path := hashpath + strconv.Itoa(s)
		exists, err := CheckExists(path)
		if err != nil {
			return nil, err
		}
		if !exists {
			err = os.MkdirAll(path, 0777)
			if err != nil {
				return nil, err
			}
		}
		path += "/"

		hashFileName := strconv.AppendInt([]byte(path), int64(iteration), 10)
		hashFile, err := os.Create(string(hashFileName))
		if err != nil {
			return nil, fmt.Errorf("Failed creating file %s to write hashes, iteration %d. Err: %v",
				string(hashFileName), iteration, err)
		}
		hashFiles[s] = hashFile

		prevHashFileName := string(strconv.AppendInt([]byte(path), int64(iteration-1), 10))
		bloomFilter, err := loadPrevHashes(prevHashFileName, bfFalsePos)
		if err != nil {
			return nil, fmt.Errorf("Failed loading previous hashes from %s, iteration %d. Err: %v",
				string(strconv.AppendInt([]byte(dbpath), int64(iteration-1), 10)), iteration, err)
		}
		bloomFilters[s] = bloomFilter
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
				errors = append(errors, []byte(s)...)
			}
		}
		s := fmt.Sprintf("Encountered %d errors. Printing first %d.\n", numErrors, maxErrors)
		errors = append([]byte(s), errors...)

		if numErrors > 0 {
			finalErr <- fmt.Errorf(string(errors))
		}
		close(finalErr)
	}()

	// load up all the filenames into fnCh
	fnCh := readFileNamesToChannel(opts.GetSession(), errCh)

	// numFileSplitters + len(blocksCh) + numBlockHashers  max number of slices that can be in use at one time
	numSlices := numFileSplitters*2 + numBlockHashers

	emptyBlocksCh := make(chan []byte, numSlices)
	blocksCh := make(chan []byte, numFileSplitters)
	hashCh := make(chan Block, numBlockHashers)
	crResChan := make(chan AllBlockSizeStats)

	var blocksWG sync.WaitGroup
	var hashWG sync.WaitGroup

	for i := 0; i < numSlices; i++ {
		b := make([]byte, maxBlockSize)
		emptyBlocksCh <- b
	}

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
					b := <-emptyBlocksCh
					if len(b) != maxBlockSize || cap(b) != maxBlockSize {
						b = b[:cap(b)]
					}
					block, err := blocks(b)
					if block == nil {
						if err != nil {
							errCh <- err
						}
						emptyBlocksCh <- b
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

				for _, bs := range blocksizes {
					hashed, err := hashAndCompressBlocks(b, bs)
					if err != nil {
						errCh <- err
					} else {
						for _, h := range *hashed {
							hashCh <- h
						}
					}
				}
				emptyBlocksCh <- b
			}
		}()
	}

	go func() {
		allStats := AllBlockSizeStats{}
		for _, bs := range blocksizes {
			allStats[bs] = &BlockStats{}
		}

		for {
			h, open := <-hashCh
			if !open {
				break
			}
			blocksize := h.blockSize
			stat := allStats[blocksize]

			stat.totalHashes++
			stat.compressedTotal += h.compressedSize
			stat.uncompressedTotal += h.uncompressedSize

			hashFile := hashFiles[blocksize]
			err := writeHash(h, hashFile)
			if err != nil {
				errCh <- err
			}

			bloomFilter := bloomFilters[blocksize]
			if bloomFilter.TestString(h.hash) {
				stat.totalDupeCount++
			}
		}

		for _, bs := range blocksizes {
			stat := allStats[bs]
			stat.DataCompressionRatio = float64(stat.uncompressedTotal) / float64(stat.compressedTotal)
			stat.DedupRate = float64(stat.totalDupeCount) / float64(stat.totalHashes)
		}
		crResChan <- allStats
		return
	}()

	blocksWG.Wait()
	close(blocksCh)

	hashWG.Wait()
	close(hashCh)
	close(emptyBlocksCh)

	res := <-crResChan
	close(errCh)

	err = <-finalErr
	if err != nil {
		return nil, err
	}

	return &res, nil
}

/*
For a 5G data file
	Size hash file 	num hashes 	 err rate 	 m 	 	k 	 size of bloomfilter
	5324800 	 	81920 	 	0.01 	 	903385 	 7 	 112952
	5324800 	 	81920 	 	0.01 	 	785200 	 6 	  98176
	5324800 	 	81920 	 	0.02 	 	667016 	 5 	  83408
	5324800 	 	81920 	 	0.05 	 	510785 	 4 	  63880
	5324800 	 	81920 	 	0.10 	 	392600 	 3 	  49104
*/
func getBloomFilterSizes(hashfile string) ([]int64, error) {
	rates := []float64{0.005, 0.01, 0.02, 0.05, 0.1}
	sizes := make([]int64, len(rates))

	fi, err := os.Stat(hashfile)
	if err != nil {
		return nil, err
	}
	hashsize := fi.Size()

	fmt.Printf("Size hash file \t num hashes \t err rate \t m \t k \t size of bloomfilter\n")
	n, err := numHashes(hashfile)
	if err != nil {
		return nil, err
	}

	for i, fp := range rates {
		m, k := bloomFilterParams(n, fp)
		bf, err := loadPrevHashes(hashfile, fp)
		if err != nil {
			return nil, err
		}
		s, err := bf.WriteTo(ioutil.Discard)
		if err != nil {
			return nil, err
		}
		sizes[i] = s
		fmt.Printf("%d \t %d \t %.3f \t %d \t %d \t %d\n", hashsize, n, fp, m, k, sizes[i])
	}
	return sizes, nil
}
