package components

import (
	"testing"
	"os"
	"path/filepath"
	"io"
	"io/ioutil"
	"runtime"
	"fmt"
)

const TestDataDir = "../../../../test_data"
const empty_dir = TestDataDir + "/emptydir"

var (
	emptyHash = []string{"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
	oneBlockHash = []string{"bf718b6f653bebc184e1479f1935b8da974d701b893afcf49e701f3e2f9f9c5a"}
	fiveBlocksRandomHash = []string{
		"9d7451e9b5c0de62376f69c6d5b64add2f4c4ff0aad3946a460aa180c2e1531f",
		"90ee39300cb64565ed12a95234c76aa8986fff7ffede914e99de02b6f3123f3c",
		"8856d16493bdb14cd208ce517183b14e9e66a81c47e0f07e2902e4d6c8a2ee6b",
		"23eedd2ae0bde4aa52d4b053c68c39e371e4c3500dbc8bd406c17bf9909e6768",
		"da5644fb9bc0a0dfac9995f22ed1b0fdfd24fcad273dab9d5f911a4504ef6341",
	}
	partialBlockHash = []string{
		"bf718b6f653bebc184e1479f1935b8da974d701b893afcf49e701f3e2f9f9c5a",
		"bf718b6f653bebc184e1479f1935b8da974d701b893afcf49e701f3e2f9f9c5a",
		"bf718b6f653bebc184e1479f1935b8da974d701b893afcf49e701f3e2f9f9c5a",
		"bf718b6f653bebc184e1479f1935b8da974d701b893afcf49e701f3e2f9f9c5a",
		"bf718b6f653bebc184e1479f1935b8da974d701b893afcf49e701f3e2f9f9c5a",
		"2edc986847e209b4016e141a6dc8716d3207350f416969382d431539bf292e4a",	}
)

const emptyCompressed = 31
const fiveBlocksCompressed = 369
const oneBlockCompressed = 111
const partialBlockCompressed = 372

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

func testBlockHashes(dbpath string) (err error) {

	opts := BackupSizingOpts{
		FalsePosRate: 0.01,
		HashDir: "hashes",
		NumCPUs: runtime.NumCPU(),
	}

	bs, err := GetBlockHashes(&opts, dbpath, blocksizes, 0)
	if err != nil {
		return
	}
	if bs == nil {
		return fmt.Errorf("Returned nil")
	}

	bs, err = GetBlockHashes(&opts, dbpath, blocksizes, 1)
	if err != nil {
		return fmt.Errorf("Second iteration: %v", err)
	}
	if bs == nil {
		return fmt.Errorf("Returned nil")
	}
	return nil
}

func TestDirectory(test *testing.T) {
	path := TestDataDir
	err := testBlockHashes(path)
	if err != nil {
		test.Errorf("Error testing path %s. Err: %v", path, err)
	}
}

func TestBasic(test *testing.T) {
	session := dial(wt_port_custPath)

	dbpath, err := GetDbPath(session)
	if err != nil {
		test.Fatalf("Could not get dbpath. err:%v", err)
	}

	err = testBlockHashes(dbpath)
	if err != nil {
		test.Errorf("Error testing path %s. Err: %v", dbpath, err)
	}
}

func TestDirPerDB(test *testing.T) {
	session := dial(replset_wt_dirPerDb)

	dbpath, err := GetDbPath(session)
	if err != nil {
		test.Fatalf("Could not get dbpath. err:%v", err)
	}

	err = testBlockHashes(dbpath)
	if err != nil {
		test.Errorf("Error testing path %s. Err: %v", dbpath, err)
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
		fnCh = readFileNamesToChannel("./DoesNotExist", mmap, errCh)
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
		fnCh = readFileNamesToChannel(empty_dir, mmap, errCh)
	}()
	fn, open = <-fnCh
	if open {
		test.Errorf("Failed to close filename channel. File:%s", fn)
	}
	if fn != "" {
		test.Errorf("Unexpected filename in empty directory:%s", fn)
	}

	fnCh = readFileNamesToChannel(TestDataDir, mmap, errCh)
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


func TestSplitFiles(test *testing.T) {
	numBlocks := map[string]int {
		"empty.test" : 0,
		"oneblock.test" : 1,
		"subdir.test" : 5,
		"partialblock.test": 6,
	}
	fns, err := getFilesInDir(TestDataDir, mmap, false)
	if err != nil {
		test.Fatalf(err.Error())
	}
	for _, fn := range fns {
		blocks, err := splitFiles(fn)
		if err != nil {
			test.Errorf("Failed to split file %s into blocks. Error: %v", fn, err)
		}
		fn = filepath.Base(fn)

		fileNumBlocks := 0
		for {
			b :=  make([]byte, blockSizeBytes)
			block, err := blocks(b)
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
	hashes := map[string][]string{
		"empty.test" : emptyHash,
		"oneblock.test" : oneBlockHash,
		"fiveblocksrandom.test" : fiveBlocksRandomHash,
		"partialblock.test": partialBlockHash,
	}

	fns, err := getFilesInDir(TestDataDir, mmap, true)
	if err != nil {
		test.Fatalf(err.Error())
	}
	for _, fn := range fns {
		f, err := os.Open(fn)
		fn := filepath.Base(fn)
		b, err := ioutil.ReadAll(f)
		if err != nil {
			if err != io.EOF {
				test.Fatalf(err.Error())
			}
		}
		blocks, err := hashAndCompressBlocks(b, blocksizes[0])
		if err != nil {
			test.Errorf(err.Error())
		}

		for i, block := range *blocks {
			h := block.hash
			exp := hashes[fn][i]
			if h != exp {
				test.Errorf("Incorrect hash for file %s. Expected:%s Received:%s", fn, exp, h)
			}
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
	if m != 1 || k != 0 {
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


