package components
import (
	"os"
	"crypto/sha256"
	"fmt"
	"github.com/willf/bloom"
	"strconv"
	"io"
	"bytes"
	"compress/zlib"
	"path/filepath"
	"math"
)

const kb = 1024
const blockSizeBytes = 64 * kb
const hashLengthBytes = 32 + 1 // + 1 for new line

func getFileNamesInDir(dir string) (*[]os.FileInfo, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		fmt.Println(dir, err)
		return nil, err
	}

	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}

	list, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	return &list, nil
}

type block struct {
	hash           []byte
	compressedSize int
	uncompressedSize int
}

func readFiles(files *[]os.FileInfo, dirPath string, readDir bool) (*[][]byte, error) {
	var blockBytes []([]byte)

	dirPath, err := filepath.Abs(dirPath)
	if err != nil {
		return nil, err
	}
	dirPath += "/"

	for _, fi := range *files {
		if fi.IsDir() && readDir {    // todo -- how to handle directories?
			path := dirPath + fi.Name()
			subDirList, err := getFileNamesInDir(path)
			if err != nil {
				fmt.Println("error getting filenames", path)
				return nil, err
			}
			blocks, err := readFiles(subDirList, path, false)
			if err != nil {
				fmt.Println("error reading files of subdirectory")
				return nil, err
			}
			blockBytes = append(blockBytes, *blocks...)
		} else if !fi.IsDir()  {
			fileName := string(append([]byte(dirPath), fi.Name()...))
			f, err := os.Open(fileName)
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
					return nil, err
				}
				blockBytes = append(blockBytes, b)
			}
			f.Close()
		}
	}

	return &blockBytes, nil
}

func hashAndCompressBlocks(blockBytes *[][]byte) (*[]block, error) {
	hasher := sha256.New()
	var hashedBlocks []block

	for _, b := range *blockBytes {
		_, err := hasher.Write(b)
		if err != nil {
			return nil, err
		}
		hashed := []byte(hasher.Sum(nil))
		//		hashed := []byte(hex.EncodeToString(hasher.Sum(nil)))

		compressedLen := compressBlock(b)

		hashedBlocks = append(hashedBlocks, block{hashed, compressedLen, len(b)})
	}

	return &hashedBlocks, nil
}

func compressBlock(block []byte) (int) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	w.Write(block)
	w.Close()
	return b.Len()
}

func writeHash(hashes *[]block, file *os.File) (float64, error) {
	compressedTotal := 0
	uncompressedTotal := 0

	for _, h := range *hashes {
		compressedTotal += h.compressedSize
		uncompressedTotal += h.uncompressedSize

		_, err := file.Write(h.hash)
		if err != nil {
			return 0, err
		}
		file.WriteString("\n")
	}
	cr := float64(uncompressedTotal) / float64(compressedTotal)
	return cr, nil
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
		//		if n < hashLengthBytes {
		//			break
		//		}
	}
	return bloomFilter, nil
}

func compareHashes(filter *bloom.BloomFilter, curHashes *[]block) float64 {
	dupHashes := 0
	totalHashes := len(*curHashes)
	for _, h := range *curHashes {
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

	fileNames, err := getFileNamesInDir(dbpath)
	if err != nil {
		return nil, fmt.Errorf("Failed getting file names in directory %s, iteration %d. Err: %v",
			dbpath, iteration, err)
	}

	blockBytes, err := readFiles(fileNames, dbpath, true)
	if err != nil {
		return nil, fmt.Errorf("Failed reading in directory %s, iteration %d. Err: %v", dbpath, iteration, err)
	}

	hashedBytes, err := hashAndCompressBlocks(blockBytes)
	if err != nil {
		return nil, fmt.Errorf("Failed hashing blocks, iteration %d. Err: %v", iteration, err)
	}

	hashFileName := strconv.AppendInt([]byte(hashpath), int64(iteration), 10)
	hashFile, err := os.Create(string(hashFileName))
	if err != nil {
		return nil, fmt.Errorf("Failed creating file %s to write hashes, iteration %d. Err: %v",
			string(hashFileName), iteration, err)
	}

	cr, err := writeHash(hashedBytes, hashFile)
	if err != nil {
		return nil, fmt.Errorf("Failed writing hashes to file %son session %v, iteration %d. Err: %v",
			string(hashFileName), iteration, err)
	}

	var dedupRate float64

	prevHashFileName := string(strconv.AppendInt([]byte(hashpath), int64(iteration - 1), 10))
	m := uint(5 * len(*hashedBytes))
	k := uint(5)

	if iteration != 0 {
		bloomFilter, err := loadPrevHashes(prevHashFileName, m, k)
		if err != nil {
			return nil, fmt.Errorf("Failed loading previous hashes from %s, iteration %d. Err: %v",
				string(strconv.AppendInt([]byte(dbpath), int64(iteration - 1), 10)), iteration, err)
		}
		dedupRate = compareHashes(bloomFilter, hashedBytes)
	} else {
		dedupRate = math.NaN()
	}

	return &BlockStats{
		DedupRate:dedupRate,
		DataCompressionRatio:cr,
	}, nil
}
