package components

import (
	"testing"
	"math"
)

func TestWritingBlockHashes(test *testing.T) {
//	session := dial(replset_port)
//	session := dial(replset_wt_dirPerDb)
	session := dial(standalone_mmap)

	dbpath, err := GetDbPath(session)
	if err != nil {
		test.Fatalf("Could not get dbpath. err:%v", err)
	}

	bs, err := GetBlockHashes(dbpath, "hashes", 0)
	if err != nil {
		test.Errorf("failed. Err: %v", err)
	}

	bs, err = GetBlockHashes(dbpath, "hashes", 1)
	if err != nil {
		test.Errorf("Failed on iteration2. Err:%v", err)
	}
	if bs.DedupRate != 1 && bs.DedupRate != math.NaN() {
		test.Errorf("deduprate < 1 on exactly same data. Received: %f", bs.DedupRate)
	}
}
