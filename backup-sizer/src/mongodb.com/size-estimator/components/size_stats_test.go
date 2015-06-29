package components
import (
	"testing"
	"time"
)

const wt_port_custPath = 28000
const wt_port_defPath = 28001

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

func testSizeStats(test *testing.T, port int) {
	session := dial(port)
	defer session.Close()

	session.DB(dbName).DropDatabase()
	time.Sleep(5*time.Second)

	sizes1, err := GetSizeStats(session)
	if err != nil {
		test.Errorf("Failed to get sizes on port %i. Err %v", port, err)
	}

	insertDocuments(session, dbName, collName, 25)
	time.Sleep(5*time.Second)

	sizes2, err := GetSizeStats(session)
	if err != nil {
		test.Errorf("Failed to get sizes on port %i. Err %v", port, err)
	}

	if sizes2.DataSize <= sizes1.DataSize || sizes2.IndexSize <= sizes1.IndexSize ||
	sizes2.FileSize <= sizes1.FileSize {
		test.Errorf("Sizes did not increase after inserting documents. Before: %v, after: %v",
			sizes1, sizes2)
	}

	removeDocuments(session, dbName, collName, 25)
	time.Sleep(5*time.Second)

	sizes3, err := GetSizeStats(session)
	if err != nil {
		test.Errorf("Failed to get sizes on port %i. Err %v", port, err)
	}

	if sizes3.DataSize >= sizes2.DataSize {
		test.Errorf("Data size did not decrease after removing documents. Before:%d, After:%d",
		sizes2.DataSize, sizes3.DataSize)
	}
	if sizes3.FileSize != sizes2.FileSize {
		test.Errorf("File size decreased after removing documents. Before:%d, After:%d",
		sizes2.FileSize, sizes3.FileSize)
	}
}

func TestMmapSizeStats(test *testing.T) {
	testSizeStats(test, replset_port)
}

func TestWTSizeStats(test *testing.T) {
	testSizeStats(test, wt_port_defPath)
}