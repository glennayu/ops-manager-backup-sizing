package components

import (
	"path/filepath"
	"testing"
)

func TestGetStorageEngine(test *testing.T) {
	sess := dial(wt_port_custPath)
	se, err := getStorageEngine(sess)
	if err != nil {
		test.Errorf("Failed getting storageEngine for port %v. Err: %v", wt_port_custPath, err)
	}
	if se != "wiredTiger" {
		test.Errorf("Expected storage engine wiredTiger, received %s", se)
	}

	sess = dial(replset_port)
	se, err = getStorageEngine(sess)
	if err != nil {
		test.Errorf("Failed getting storageEngine for port %v. Err: %v", replset_port, err)
	}
	if se != "mmapv1" {
		test.Errorf("Expected storage engine mmapv1, received %s", se)
	}

	sess = dial(standalone_mmap)
	se, err = getStorageEngine(sess)
	if err != nil {
		test.Errorf("Failed getting storageEngine for port %v. Err: %v", standalone_mmap, err)
	}
	if se != "mmapv1" {
		test.Errorf("Expected storage engine mmapv1, received %s", se)
	}

}

func TestGetFilesInDir(test *testing.T) {
	files, err := getFilesInDir(TestDataDir, mmap, true)
	if err != nil {
		test.Errorf("Failed getting files from test directory. %v", err)
	}
	if len(files) != 4 {
		test.Errorf("Expected 4 files. Received %d. Files returned: %v", len(files), files)
	}
	for _, f := range files {
		fn := filepath.Base(f)
		if fn == "local.0" || fn == "journal1" {
			test.Errorf("Unwanted file included: %s", fn)
		}
	}
}
