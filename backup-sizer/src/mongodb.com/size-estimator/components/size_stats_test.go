package components
import (
	"testing"
	"time"
	"gopkg.in/mgo.v2"
)


func TestGetDbPath(test *testing.T) {
	session := dial(wt_port_custPath)
	defer session.Close()
	path, err := getDbPath(session)
	if err != nil {
		test.Errorf("Failed to get dbpath on port %i. Err %v", wt_port_custPath, err)
	}

	sess_defPath := dial(wt_port_defPath)
	defer sess_defPath.Close()
	path, err = getDbPath(sess_defPath)
	if err != nil {
		test.Errorf("Failed to get dbpath on port %i. Err %v", wt_port_defPath, err)
	}
	if path != "/data/db" {
		test.Errorf("Expected default path '/data/db'. Received %s", path)
	}
}

func TestIncorrectPermissions(test *testing.T) {
	session_root := dial(wt_root)
	defer session_root.Close()

	res, err := getWTFileSize(session_root)
	if err == nil {
		test.Errorf("Expected permission error. Received res: %f, err: %v", res, err)
	}
}

func testCappedCollection(test *testing.T, port int) {
	session := dial(port)

	session.DB(dbName).DropDatabase()

	session.DB(dbName).C("capped").Create(&mgo.CollectionInfo{Capped:true, MaxBytes:4096})
	time.Sleep(5*time.Second)

	sizes1, err := GetSizeStats(session)
	if err != nil {
		test.Errorf("Failed to get sizes on db with a capped collection. Err: %v", err)
	}

	insertDocuments(session, dbName, "capped", 4096)
	time.Sleep(5*time.Second)

	sizes2, err := GetSizeStats(session)
	if err != nil {
		test.Errorf("Failed to get sizes on db with a capped collection. Err: %v", err)
	}

	if sizes2.FileSize != sizes1.FileSize {
		test.Errorf("FileSize increased after inserting into capped collection. Before: %v, After: %v",
			sizes1.FileSize, sizes2.FileSize)
	}
}

func testSizeStats(test *testing.T, port int) {
	session := dial(port)
	defer session.Close()

	session.DB(dbName).DropDatabase()
	time.Sleep(5*time.Second)

	sizes1, err := GetSizeStats(session)
	if err != nil {
		test.Errorf("Failed to get sizes on port %i. Err %v", port, err)
	}

	insertDocuments(session, dbName, collName, 200)
	time.Sleep(5*time.Second)

	sizes2, err := GetSizeStats(session)
	if err != nil {
		test.Errorf("Failed to get sizes on port %i. Err %v", port, err)
	}

	if sizes2.DataSize <= sizes1.DataSize {
		test.Errorf("DataSize did not increase after inserting documents. Before: %v, after: %v",
			sizes1.DataSize, sizes2.DataSize)
	}
	if sizes2.IndexSize <= sizes1.IndexSize {
		test.Errorf("IndexSize did not increase after inserting documents. Before: %v, after: %v",
			sizes1.IndexSize, sizes2.IndexSize)
	}
	if sizes2.FileSize <= sizes1.FileSize {
		test.Errorf("FileSize did not increase after inserting documents. Before: %v, after: %v",
			sizes1.FileSize, sizes2.FileSize)
	}

	removeDocuments(session, dbName, collName, 200)
	time.Sleep(5*time.Second)

	sizes3, err := GetSizeStats(session)
	if err != nil {
		test.Errorf("Failed to get sizes on port %i. Err %v", port, err)
	}

	if sizes3.DataSize >= sizes2.DataSize {
		test.Errorf("Data size did not decrease after removing documents. Before:%f, After:%f",
			sizes2.DataSize, sizes3.DataSize)
	}
	if sizes3.FileSize != sizes2.FileSize {
		test.Errorf("File size changed after removing documents. Before:%f, After:%f",
			sizes2.FileSize, sizes3.FileSize)
	}
}

func TestMmapSizeStats(test *testing.T) {
	testSizeStats(test, replset_port)
	testCappedCollection(test, replset_port)
}

func TestWTSizeStats(test *testing.T) {
	testSizeStats(test, wt_port_custPath)
	testCappedCollection(test, wt_port_custPath)
}