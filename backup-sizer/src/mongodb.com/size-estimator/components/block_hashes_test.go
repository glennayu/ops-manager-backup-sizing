package components

import (
	"testing"
	"os"
	"io/ioutil"
	"path/filepath"
)

const TestDataDir = "../../../../test_data"
const empty_dir = TestDataDir + "/emptydir"

const emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
const oneBlockHash = "bf718b6f653bebc184e1479f1935b8da974d701b893afcf49e701f3e2f9f9c5a"
const fiveBlocksRandomHash = "a6a832668ff60bcf59f467d97d883ed0a67838512ae3b7a6e6bda0dae961f719"
const partialBlockHash = "a1898b59a192f20e8f0a2a6fec39b171e6df0b19160c49023fca1f63808852b0"

const emptyCompressed = 31
const fiveBlocksCompressed = 369
const oneBlockCompressed = 111
const partialBlockCompressed = 372

const kb = 1024
const mb = 1024*kb

var blocksizes = []int{64 * kb,
128 * kb,
256 * kb,
512 * kb,
1 * mb,
2 * mb,
4 * mb,
8 * mb,
16 * mb}

func TestWritingBlockHashes(test *testing.T) {
	session := dial(wt_port_defPath)

	dbpath, err := GetDbPath(session)
	if err != nil {
		test.Fatalf("Could not get dbpath. err:%v", err)
	}

	falsePosRate := 0.01

	bs, err := GetBlockHashes(dbpath, "hashes", falsePosRate, blocksizes, 0)
	if err != nil {
		test.Errorf("failed. Err: %v", err)
	}
	if bs == nil {
		test.Errorf("Returned nil")
	}

	bs, err = GetBlockHashes(dbpath, "hashes", falsePosRate, blocksizes, 1)
	if err != nil {
		test.Errorf("Failed on iteration2. Err:%v", err)
	}
	if bs == nil {
		test.Errorf("Returned nil")
	}
}

func TestDirPerDB(test *testing.T) {
	session := dial(replset_wt_dirPerDb)

	dbpath, err := GetDbPath(session)
	if err != nil {
		test.Fatalf("Could not get dbpath. err:%v", err)
	}

	falsePosRate := 0.1
	bs, err := GetBlockHashes(dbpath, "hashes", falsePosRate, blocksizes,  0)
	if err != nil {
		test.Errorf("failed. Err: %v", err)
	}
	if bs == nil {
		test.Errorf("Returned nil")
	}

	bs, err = GetBlockHashes(dbpath, "hashes", falsePosRate, blocksizes, 1)
	if err != nil {
		test.Errorf("Failed on iteration2. Err:%v", err)
	}
	if bs == nil {
		test.Errorf("Returned nil")
	}
}

func TestReadFileNames(test *testing.T) {
	// empty directory
	errCh := make(chan error)

	exists, err := CheckExists(empty_dir)
	if err != nil {
		test.Fatalf("Failure to check %s exists", empty_dir)
	}
	if !exists {
		test.Errorf("%s does not exist", empty_dir)
	}

	var fnCh chan string
	go func() {
		fnCh = readFileNamesToChannel("./DoesNotExist", errCh)
	}()
	err = <-errCh
	if err == nil {
		test.Errorf("Expected an error")
	}
	fn, open := <-fnCh
	if open {
		test.Errorf("Failed to close filename channel. File:%s", fn)
	}
	if fn != "" {
		test.Errorf("Unexpected filename in non-existant directory:%s", fn)
	}

	go func() {
		fnCh = readFileNamesToChannel(empty_dir, errCh)
	}()
	fn, open = <-fnCh
	if open {
		test.Errorf("Failed to close filename channel. File:%s", fn)
	}
	if fn != "" {
		test.Errorf("Unexpected filename in empty directory:%s", fn)
	}

	fnCh = readFileNamesToChannel(TestDataDir, errCh)
	fncount := 0
	for fn := range fnCh {
		fi, err := os.Stat(fn)
		if err != nil {
			test.Errorf("Error with file %s. Error: &v", fn, err)
		} else if fi.IsDir() {
			test.Errorf("Unexpected directory in filename channel: %s", fn)
		} else {
			fncount++
		}
	}
	if fncount != 4 {
		test.Errorf("Expected four filenames from directory %s. Received: %d", TestDataDir, fncount)
	}

}

func TestSplitBlocks(test *testing.T) {
	numBlocks := map[string]int {
		"empty.test" : 0,
		"oneblock.test" : 1,
		"subdir.test" : 5,
		"partialblock.test": 6,
	}
	fns, err := getFilesInDir(TestDataDir, false)
	if err != nil {
		test.Fatalf(err.Error())
	}
	for _, fn := range fns {
		blocks, err := splitFiles(fn, blockSizeBytes)
		if err != nil {
			test.Errorf("Failed to split file %s into blocks. Error: %v", fn, err)
		}
		fn = filepath.Base(fn)

		fileNumBlocks := 0
		for {
			block, err := blocks()
			if block == nil {
				if err != nil {
					test.Errorf("Failed to get block from splitFiles for file %s. Error: %v", fn, err)
				}
				break
			}
			fileNumBlocks++
		}
		if fileNumBlocks != numBlocks[fn] {
			test.Errorf("Unexpected number of blocks for file %s. Expected: %d, Received:%d", fn, numBlocks[fn],
				fileNumBlocks)
		}
	}
}

func TestHashAndCompressBlocks(test *testing.T) {
	hashes := map[string]string{
		"empty.test" : emptyHash,
		"oneblock.test" : oneBlockHash,
		"fiveblocksrandom.test" : fiveBlocksRandomHash,
		"partialblock.test": partialBlockHash,
	}

//	compressedSizes := map[string]int{
//		"empty.test" : emptyCompressed,
//		"oneblock.test" : oneBlockCompressed,
//		"partialblock.test": partialBlockCompressed,
//	}

	blocks := map[string]Block{}

	fns, err := getFilesInDir(TestDataDir, true)
	if err != nil {
		test.Fatalf(err.Error())
	}
	for _, fn := range fns {
		f, err := os.Open(fn)
		fn := filepath.Base(fn)
		var b []byte
		b, err = ioutil.ReadAll(f)
		if err != nil {
			test.Fatalf(err.Error())
		}
		block, err := hashAndCompressBlocks(b)
		if err != nil {
			test.Errorf(err.Error())
		}
		blocks[fn] = block

		h := block.hash
		if h != hashes[fn] {
			test.Errorf("Incorrect hash for file %s. Expected:%s Received:%s", fn,  hashes[fn], h)
		}
	}
}

func TestBloomFilterParams(test *testing.T) {
	m, k := bloomFilterParams(10, 0.05)
	if m != 63 || k != 4 {
		test.Errorf("Expected (m, k) = (63, 4). Received m, k = (%d, %d)", m, k)
	}

	m, k = bloomFilterParams(0, 0.5)
	if m != 0 || k != 0 {
		test.Errorf("Expected (m, k) = (0, 0). Received m, k = (%d, %d)", m, k)
	}

	m, k = bloomFilterParams(10, 1)
	if m != 0 || k != 0 {
		test.Errorf("Expected (m, k) = (0, 0). Received m, k = (%d, %d)", m, k)
	}
}

func TestLoadPrevHashes(test *testing.T) {
	fn := "./DoesNotExist"
	falsePosRate := 0.01
	_, err := loadPrevHashes(fn, falsePosRate)
	if err != nil {
		test.Errorf("Unexpected error from loading non-existant file %s. Error: %v", fn, err)
	}

	// empty file -- empty bloom filter
	fn = TestDataDir + "/empy.test"
	_, err = loadPrevHashes(fn, falsePosRate)
	if err != nil {
		test.Errorf("Unexpected error from loading empty file %s. Error: %v", fn, err)
	}

	// reading from directory should return an error
	fn = TestDataDir
	_, err = loadPrevHashes(fn, falsePosRate)
	if err == nil {
		test.Errorf("Expected error from loading directory %s", fn)
	}

	fn = TestDataDir + "/empy.test"
	_, err = loadPrevHashes(fn, falsePosRate)
	if err != nil {
		test.Errorf("Unexpected error from loading file %s. Error: %v", fn, err)
	}

}


