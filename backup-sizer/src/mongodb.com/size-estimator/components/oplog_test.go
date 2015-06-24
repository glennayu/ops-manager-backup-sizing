package components
import (
	"fmt"
	"testing"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
	"bytes"
	"strconv"
)

const replset_port = 27017

func dial(port int) *mgo.Session {
	addr := "localhost:" + strconv.Itoa(port)
	session, err := mgo.Dial(addr)
	if err != nil {
		panic(fmt.Sprintf("Error dialing. Addr: %v Err: %v", addr, err))
	}

	return session
}

func insertDocuments(mongo *mgo.Session, database string, collection string, numInsert int) {
	session := mongo.Copy()
	defer session.Close()

	session.SetMode(mgo.Strong, false)
	for idx := 0; idx < numInsert; idx++ {
		session.DB(database).C(collection).Insert(bson.M{"number": idx})
	}
}


func insertBytes(mongo *mgo.Session, database string, collection string, numBytes uint64) {
	session := mongo.Copy()
	defer session.Close()

	var bytesGenerated uint64 = 0
	bytesVal := bytes.Repeat([]byte{0}, 5*1024)

	for bytesGenerated < numBytes {
		toAdd := &bson.D{
			{"_id", bytesGenerated},
			{"bytes", bytesVal},
		}

		session.DB(database).C(collection).Insert(toAdd)
		bytesGenerated += 5 * 1024
	}
}

func TestOplogSize(test *testing.T) {

}

func TestUpdatedStartEnd(test *testing.T) {
	session := dial(replset_port)
	defer session.Close()

	session.DB("test").DropDatabase()
	session.DB("test").C("test").Create(&mgo.CollectionInfo{})

	const numDocs = 10

	insertDocuments(session, "test", "test", numDocs)

	//	oplogColl, err := getOplogColl(session)
	oplogColl := session.DB("local").C("oplog.rs")
	//	if err != nil {
	//		test.Fatal("Error getting oplog. Error:%s", err.Error())
	//	}

	start1, end1, err := GetOplogStartEnd(oplogColl)
	if err != nil {
		test.Fatal("Error getting oplog start/end times")
	}


	time.Sleep(5*time.Second)
	insertDocuments(session, "test", "test", 1)

	start2, end2, err := GetOplogStartEnd(oplogColl)
	if err != nil {
		test.Fatal("Error getting oplog start/end times")
	}

	if end1 == end2 {
		test.Errorf(
			"End timestamp did not update after inserts. Original: %d - %d \t Update: %d - $d",
			start1, end1, start2, end2)
	}

	time.Sleep(5*time.Second)
	_, size, err := isCapped(oplogColl)
	if err != nil {
		test.Fatalf("Could not get configured size of oplog. Error: %s", err.Error())
	}

	insertBytes(session, "test", "test", uint64(size))
	start3, end3, err := GetOplogStartEnd(oplogColl)
	if err != nil {
		test.Fatal("Error getting oplog start/end times")
	}

	if start1 == start3 {
		test.Errorf(
			"Start timestamp changed update after inserts. Original: %d - %d \t Update: %d - $d",
			start1, end1, start3, end3)
	}
	if end2 == end3 {
		test.Errorf(
			"End timestamp did not update after inserts. Original: %d - %d \t Update: %d - $d",
			start2, end2, start3, end3)
	}
}


