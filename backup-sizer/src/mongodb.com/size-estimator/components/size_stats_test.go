package components
import (
	"testing"
	"gopkg.in/mgo.v2"
)


func TestGetDbPath(test *testing.T) {
	session := dial(wt_port_custPath)
	defer session.Close()
	path, err := GetDbPath(session)
	if err != nil {
		test.Errorf("Failed to get dbpath on port %i. Err %v", wt_port_custPath, err)
	}

	sess_defPath := dial(wt_port_defPath)
	defer sess_defPath.Close()
	path, err = GetDbPath(sess_defPath)
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
	defer session.Close()

	session.SetSafe(&mgo.Safe{W:1})
	session.DB(dbName).DropDatabase()
	session.DB(dbName).C("capped").Create(&mgo.CollectionInfo{Capped:true, MaxBytes:4096})

	sizes1, err := GetSizeStats(session)
	if err != nil {
		test.Errorf("Failed to get sizes on db with a capped collection. Err: %v", err)
	}

	insertDocuments(session, dbName, "capped", 4096)

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
	session.SetSafe(&mgo.Safe{W:1})

	session.DB(dbName).DropDatabase()

	sizes1, err := GetSizeStats(session)
	if err != nil {
		test.Errorf("Failed to get sizes on port %i. Err %v", port, err)
	}

	insertDocuments(session, dbName, collName, 1000)

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

	removeDocuments(session, dbName, collName, 1000)

	sizes3, err := GetSizeStats(session)
	if err != nil {
		test.Errorf("Failed to get sizes on port %i. Err %v", port, err)
	}
	if sizes3.FileSize != sizes2.FileSize {
		test.Errorf("File size changed after removing documents. Before:%f, After:%f",
			sizes2.FileSize, sizes3.FileSize)
	}

	// test multiple databases
	generateBytes(session, "test2", collName, 5*1024*1024, bytesSame)
	sizes4, err := GetSizeStats(session)
	if err != nil {
		test.Errorf("Failed to get sizes on port %i with multiple databases. Err %v", port, err)
	}
	if sizes4.FileSize < sizes3.FileSize {
		test.Errorf("File size decreased after inserting into new db. Before:%f, After:%f",
			sizes3.FileSize, sizes4.FileSize)
	}

}

func TestMmapSizeStats(test *testing.T) {
	testSizeStats(test, replset_port)
	testCappedCollection(test, replset_port)
}

func TestWTSizeStats(test *testing.T) {
	testSizeStats(test, replset_wt_dirPerDb)
	testCappedCollection(test, wt_port_defPath)
}

