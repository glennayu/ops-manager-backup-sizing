package components

import (
	"testing"
	"os"
	"path/filepath"
	"fmt"
	"io"
	"io/ioutil"
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
		"d8e5bc7a11ccce349eed8295b268d2a7480285c01578f8f9f20a39ea001b7e63",	}
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

func TestWritingBlockHashes(test *testing.T) {
	session := dial(wt_port_custPath)

	dbpath, err := GetDbPath(session)
	dbpath = "/Users/gryu/test"
	if err != nil {
		test.Fatalf("Could not get dbpath. err:%v", err)
	}

	falsePosRate := 0.01

	bs, err := GetBlockHashes(dbpath, "hashes", falsePosRate, 0, blocksizes)
	if err != nil {
		test.Errorf("failed. Err: %v", err)
	}
	if bs == nil {
		test.Errorf("Returned nil")
	}
	for size, blockstat := range *bs {
		fmt.Printf("BlockSize: %d \t DedupRate: %f \t CompressionRate: %f\n", size,
		blockstat.DedupRate, blockstat.DataCompressionRatio)
	}

	bs, err = GetBlockHashes(dbpath, "hashes", falsePosRate,1,  blocksizes)
	if err != nil {
		test.Errorf("Failed on iteration2. Err:%v", err)
	}
	if bs == nil {
		test.Errorf("Returned nil")
	}
	for size, blockstat := range *bs {
		fmt.Printf("BlockSize: %d \t DedupRate: %f \t CompressionRate: %f\n", size,
			blockstat.DedupRate, blockstat.DataCompressionRatio)
	}}

func TestDirPerDB(test *testing.T) {
	session := dial(replset_wt_dirPerDb)

	dbpath, err := GetDbPath(session)
	if err != nil {
		test.Fatalf("Could not get dbpath. err:%v", err)
	}

	falsePosRate := 0.1
	bs, err := GetBlockHashes(dbpath, "hashes", falsePosRate, 0, blocksizes)
	if err != nil {
		test.Errorf("failed. Err: %v", err)
	}
	if bs == nil {
		test.Errorf("Returned nil")
	}

	bs, err = GetBlockHashes(dbpath, "hashes", falsePosRate, 1, blocksizes)
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


func TestSplitFiles(test *testing.T) {
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

func TestSplitBlocks(test *testing.T) {
	block := make([]byte, 10)
	for i := range block {
		block[i] = byte(i)
	}

	split := splitBlocks(block, 2)
	for _, s := range split {
		fmt.Println(s)
		if len(s) != 2 {
			test.Errorf("error splitting blocks")
		}
	}
	if len(split) != len(block) / 2 {
		test.Errorf("error splitting blocks - expected 5 splits, received %d", len(split))
	}

	// not an even multiple - should still work
	split = splitBlocks(block[:9], 2)
	for _, s := range split {
		fmt.Println(s)
		if len(s) != 2 {
			test.Errorf("error splitting blocks")
		}
	}
	if len(split) != len(block) / 2 {
		test.Errorf("error splitting blocks - expected 5 splits, received %d", len(split))
	}

	// empty
	split = splitBlocks([]byte{}, 2)
	if len(split) != 0 {
		test.Errorf("error splitting empty block - expected 0 splits, received %d", len(split))
	}

	split = splitBlocks(block, 15)
	if len(split) != 1 {
		test.Errorf("error splitting block - expected 1 splits, received %d", len(split))
	}
}

// todo all these things


func TestHashAndCompressBlocks(test *testing.T) {
	hashes := map[string][]string{
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

	fns, err := getFilesInDir(TestDataDir, true)
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

		fmt.Println(fn, blocks)

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


