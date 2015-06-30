package components
import (
	"os"
	"crypto/sha256"
	"fmt"
//	"encoding/hex"
	"github.com/willf/bloom"
	"encoding/hex"
	"strconv"
	"io"
)

const kb = 1024
const blockSizeBytes = 64 * kb
const hashLengthBytes = 64 + 1 // + 1 for new line

func getFileNamesInDir(dir string) (*[]os.FileInfo, error) {
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
	hash 			[]byte
	compressedSize 	int
}

func readFiles(files *[]os.FileInfo, dirPath string) (*[][]byte, error) {
	var blockBytes []([]byte)

	for _, fi := range *files {
		if !fi.IsDir() {	// todo -- how to handle directories?
			fileName := append([]byte(dirPath), fi.Name()...)
			f, err := os.Open(string(fileName))
			if err != nil {
				return nil, err
			}
			for {
				b := make([]byte, blockSizeBytes)
				n, err := f.Read(b)
				if err != nil {
					return nil, err
				}
				blockBytes = append(blockBytes, b)
				if n != blockSizeBytes {
					break
				}
			}
		}
	}

	return &blockBytes, nil
}

func hashBlocks(blockBytes *[][]byte) (*[]block, error) {
	hasher := sha256.New()
	var hashedBlocks []block

	for _, b := range *blockBytes {
		_, err := hasher.Write(b) // todo - do we need to do anything with n?
		if err != nil {
			return nil, err
		}
		hashed := []byte(hex.EncodeToString(hasher.Sum(nil)))
//		hashed := hasher.Sum(nil)
		hashed = append(hashed, '\n')
		compressedLen, err := compressBlock(b)
		if err != nil {
			return nil, err
		}
		hashedBlocks = append(hashedBlocks, block{hashed, compressedLen})
	}
	return &hashedBlocks, nil
}

func compressBlock(block []byte) (int, error) {
	return 0, nil
}

func writeHash(hashes *[]block, file *os.File) error {
	for _, h := range *hashes {
		_, err := file.Write(h.hash)
		if err != nil {
			return err
		}
	}
	return nil
}

func loadPrevHashes(fileName string) (*bloom.BloomFilter, error) {
	// figure out these parameters
	m := uint(10000)
	k := uint(5)
	bloomFilter := bloom.New(m, k)

	// add previous hashes to bloom filter
	prevFile, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	for {
		h := make([]byte, hashLengthBytes)
		n, err := prevFile.Read(h)
		if err != nil {
			if err == io.EOF {
				return bloomFilter, nil
			}
			return nil, err
		}
		bloomFilter.Add(h)
		fmt.Println(n, h)
		if n < hashLengthBytes {
			break
		}
	}
	return bloomFilter, nil
}

func compareHashes(filter *bloom.BloomFilter, curHashes *[]block) float64{
	dupHashes := 0
	totalHashes := len(*curHashes)
	for _, h := range *curHashes {
		fmt.Println(h.hash)
		if filter.Test(h.hash) {
			dupHashes++
		}
	}
	fmt.Println()
	return float64(dupHashes) / float64(totalHashes)
}

type blockStats struct {

}

func GetBlockHashes(iteration int) (*blockStats, error) {
	// todo: change this
	dbpath := "/data/db/"

	fileNames, err := getFileNamesInDir(dbpath)
	if err != nil {
		return nil, err
	}

	blockBytes, err := readFiles(fileNames, dbpath)
	if err != nil {
		return nil, err
	}

	hashedBytes, err := hashBlocks(blockBytes)
	if err != nil {
		return nil, err
	}

	// todo: change this
	baseName := []byte("hashes")

	hashFileName := strconv.AppendInt(baseName, int64(iteration), 10)
	hashFile, err := os.Create(string(hashFileName))
	if err != nil {
		return nil, err
	}

	err = writeHash(hashedBytes, hashFile)

	if iteration != 0 {
		bloomFilter, err := loadPrevHashes(string(strconv.AppendInt(baseName, int64(iteration - 1), 10)))
		if err != nil {
			return nil, err
		}

		dedupRate := compareHashes(bloomFilter, hashedBytes)
		fmt.Println(dedupRate)
	}

	return &blockStats{

	}, nil
}