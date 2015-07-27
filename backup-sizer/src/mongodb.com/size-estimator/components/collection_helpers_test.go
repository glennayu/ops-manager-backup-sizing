package components
import (
	"testing"
	"path/filepath"
	"os"
)

func TestGetStorageEngine(test *testing.T) {
	sess := dial(wt_port_defPath)
	se, err := getStorageEngine(sess)
	if err != nil {
		test.Errorf("Failed getting storageEngine for port %v. Err: %v", wt_port_defPath, err)
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

func TestGetDbPath(test *testing.T) {
	session := dial(wt_port_custPath)
	defer session.Close()
	path, err := GetDbPath(session)
	if err != nil {
		test.Errorf("Failed to get dbpath on port %i. Err %v", wt_port_custPath, err)
	}

	session = dial(wt_port_defPath)
	defer session.Close()
	path, err = GetDbPath(session)
	if err != nil {
		test.Errorf("Failed to get dbpath on port %i. Err %v", wt_port_defPath, err)
	}
	if path != "/data/db" {
		test.Errorf("Expected default path '/data/db'. Received %s", path)
	}

	session = dial(replset_port)
	defer session.Close()
	path, err = GetDbPath(session)
	if err != nil {
		test.Errorf("Failed to get dbpath on port %i. Err %v", replset_port, err)
	}
}

func TestGetFilesInDir(test *testing.T) {
	excludedFiles := []string{"journal", "local*"}

	exists, err := CheckExists(empty_dir)
	if err != nil {
		test.Fatalf("Failure to check %s exists", empty_dir)
	}
	if !exists {
		test.Errorf("%s does not exist", empty_dir)
	}

	var files *[]string
	files, err = GetFilesInDir("./DoesNotExist", &excludedFiles, true)
	if err == nil {
		test.Errorf("Expected an error")
	}
	if files != nil {
		test.Errorf("Return value from non-existant directory: %v", files)
	}

	files, err = GetFilesInDir(empty_dir, &excludedFiles, true)
	if len(*files) != 0 {
		test.Errorf("Return value from empty directory:%v", files)
	}

	files, err = GetFilesInDir(TestDataDir, &excludedFiles, true)
	if err != nil {
		test.Errorf("Failed getting files from test directory. %v", err)
	}
	if len(*files) != 4 {
		test.Errorf("Expected 4 files. Received %d. Files returned: %v", len(*files), files)
	}
	for _, fn := range *files {
		fi, err := os.Stat(fn)
		if err != nil {
			test.Errorf("Error with file %s. Error: &v", fn, err)
		}
		if fi.IsDir() {
			test.Errorf("Unexpected directory in returned files: %s", fn)
		}
		base := filepath.Base(fn)
		if base == "local.0" || base == "journal1" {
			test.Errorf("Unwanted file included: %s", fn)
		}
	}
}


func TestGetExcludeFileRegexes(test *testing.T) {
	excludedFiles, err := GetExcludeFileRegexes(nil)
	if err == nil {
		test.Errorf("Expected error from nil session. Received: %v, Error:%v", excludedFiles, err)
	}

	session := dial(replset_port)
	excludedFiles, err = GetExcludeFileRegexes(session)
	if err != nil {
		test.Errorf("Error getting excluded files on port %d. Error: %v", replset_port, err)
	}
	if len(*excludedFiles) != 4 {
		test.Errorf("Expected 4 regexes for session running on mmapv1. Received: %v", excludedFiles)
	}

	session = dial(replset_wt_dirPerDb)
	excludedFiles, err = GetExcludeFileRegexes(session)
	if err != nil {
		test.Errorf("Error getting excluded files on port %d. Error: %v", replset_wt_dirPerDb, err)
	}
	if len(*excludedFiles) != 5 {
		test.Errorf("Expected 5 regexes for session running on wiredTiger. Received: %v", excludedFiles)
	}

	session = dial(wt_port_defPath)
	excludedFiles, err = GetExcludeFileRegexes(session)
	if err != nil {
		test.Errorf("Error getting excluded files on port %d. Error: %v", wt_port_defPath, err)
	}
	if len(*excludedFiles) != 5 {
		test.Errorf("Expected 5 regexes for session running on wiredTiger without oplog. Received: %v", excludedFiles)
	}
	if (*excludedFiles)[4] != "" {
		test.Errorf("Expected empty regex for oplog file. Received: %s", (*excludedFiles)[4])
	}
}