package components
import (
	"fmt"
	"testing"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
	"strconv"
	"crypto/rand"
	"bytes"
	"math"
)

const replset_port = 27017
const standalone_port = 26000

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

const bytesSame = 0
const bytesRandom = 1

func genRandomBytes(cap int32) []byte {
	b := make([]byte, cap)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("error:", err)
		return nil
	}
	return b
}

func generateBytes(mongo *mgo.Session, database string, collection string,
numBytes uint64, mode int) {
	session := mongo.Copy()
	defer session.Close()

	var bytesGenerated uint64 = 0
	var bytesVal []byte

	for bytesGenerated < numBytes {
		switch mode {
		case bytesSame:
			bytesVal = bytes.Repeat([]byte{0}, 5*1024)
		case bytesRandom:
			bytesVal = genRandomBytes(5*1024)
		}
		toAdd := &bson.D{
			{"_id", bytesGenerated},
			{"bytes", bytesVal},
		}

		session.DB(database).C(collection).Insert(toAdd)
		bytesGenerated += 5 * 1024
	}
}

type oplogInfoTest struct {
	info     *OplogInfo
	expected float32
	desc     string
}

func TestMbPerDay(test *testing.T) {
	const MB = 1024*1024
	const SecPerDay = 86400

	testCases := []oplogInfoTest{
		{&OplogInfo{
			bson.MongoTimestamp(int64(0) << 32),
			bson.MongoTimestamp(int64(SecPerDay) << 32),
			MB}, 1, "basic"},
		{&OplogInfo{
			bson.MongoTimestamp(time.Now().Unix() << 32),
			bson.MongoTimestamp(time.Now().Add(72 * time.Hour).Unix() << 32),
			193 * MB}, float32(193) / float32(3), "normal case"},
		{&OplogInfo{
			bson.MongoTimestamp(0),
			bson.MongoTimestamp(0),
			0}, 0 * SecPerDay, "start, end, size = 0"},
		{&OplogInfo{
			bson.MongoTimestamp(int64(5) << 32 | 0),
			bson.MongoTimestamp(int64(5) << 32 | 5),
			1 * MB}, 1 * SecPerDay, "total time < 1 second, with multiple entries"},
		{&OplogInfo{
			bson.MongoTimestamp(int64(5) << 32 | 5),
			bson.MongoTimestamp(int64(5) << 32 | 5),
			1 * MB}, 1 * SecPerDay, "total time < 1 second, with one entry"},
	}

	for _, testCase := range testCases {
		info := testCase.info
		expected := testCase.expected
		desc := testCase.desc

		mb, err := MbPerDay(info)
		if err != nil {
			test.Errorf("Failure getting MB per day. Err %v", err)
		}
		if mb != expected {
			test.Errorf("Testing %s, OplogInfo %v, expected %f mb per day written, received %f",
				desc, info, expected, mb)
		}

		expectedGb := float64(testCase.expected / 1024)
		gb, err := GbPerDay(info)
		if err != nil {
			test.Errorf("Failure getting MB per day. Err %v", err)
		}
		if gb != expectedGb {
			test.Errorf("Testing %s, OplogInfo %v, expected %f mb per day written, received %f",
				desc, info, expectedGb, gb)
		}
	}

	info := OplogInfo{
		bson.MongoTimestamp(time.Now().Unix() << 32),
		bson.MongoTimestamp(time.Now().Add(-1 * time.Hour).Unix() << 32),
		1*MB}
	gb, err := GbPerDay(&info)
	if err == nil {
		test.Errorf("Expected error for start > end. Result: %f", gb)
	}
}

func TestGetIterator(test *testing.T) {
	session := dial(replset_port)
	defer session.Close()

	const dbName = "test"
	const collName = "test"

	session.DB(dbName).DropDatabase()

	coll := session.DB(dbName).C(collName)
	coll.Create(&mgo.CollectionInfo{})
	it, err := GetOplogIterator(time.Now(), time.Second, session)
	if err != nil {
		test.Errorf("Failed to get iterator. Err: %v", err)
	}

	var res bson.D
	if it.Next(res) {
		test.Error("Empty collection - expected empty iterator")
	}

	time.Sleep(3*time.Second) //
	timeA := time.Now()
	insertDocuments(session, dbName, collName, 10)
	time.Sleep(3*time.Second)
	timeB := time.Now()
	it, err = GetOplogIterator(timeB, timeB.Sub(timeA), session)
	if err != nil {
		test.Errorf("Failed to get iterator. Err: %v", err)
	}

	var results []bson.M
	err = it.All(&results)
	if err != nil {
		test.Errorf("Failed to get all results from iterator. Err: %v", err)
	}
	if len(results) != 10 {
		test.Errorf("Expected 10 documents, received %d", len(results))
	}

	time.Sleep(3*time.Second)
	insertDocuments(session, dbName, collName, 5)
	it, err = GetOplogIterator(time.Now(), 3*time.Second, session)
	if err != nil {
		test.Errorf("Failed to get iterator. Err: %v", err)
	}
	err = it.All(&results)
	if err != nil {
		test.Errorf("Failed to get all results from iterator. Err: %v", err)
	}
	if len(results) > 5 {
		test.Errorf("Expected 5 documents, received %d", len(results))
	}

	timeC := time.Now()
	it, err = GetOplogIterator(timeC, timeC.Sub(timeA), session)
	if err != nil {
		test.Errorf("Failed to get iterator. Err: %v", err)
	}
	err = it.All(&results)
	if err != nil {
		test.Errorf("Failed to get all results from iterator. Err: %v", err)
	}
	if len(results) != 15 {
		test.Errorf("Expected 15 documents, received %d", len(results))
	}
}

func TestCompressionRatio(test *testing.T) {
	session := dial(replset_port)
	defer session.Close()

	const dbName = "test"
	const collName = "test"

	session.DB("test").DropDatabase()

	coll := session.DB(dbName).C(collName)
	coll.Create(&mgo.CollectionInfo{})

	it := coll.Find(bson.M{}).Iter()
	cr, err := CompressionRatio(it)
	if err != nil {
		test.Fatal("Failed to get compression ratio for collection %v", coll)
	}
	if !math.IsNaN(float64(cr)) {
		test.Errorf("Empty collection - expected NaN, received %f", cr)
	}

	coll.Insert(bson.M{"a":1})
	it = coll.Find(bson.M{}).Iter()
	cr, err = CompressionRatio(it)
	if err != nil {
		test.Fatalf("Failed to get compression ratio for collection %v", coll)
	}
	coll.DropCollection()


	// write a bunch of repeated bytes to it - expected higher compression
	generateBytes(session, dbName, collName, 50*1024, bytesSame)
	it = coll.Find(bson.M{}).Iter()
	crSame, err := CompressionRatio(it)
	if err != nil {
		test.Fatal("Failed to get compression ratio for collection %v", coll)
	}
	if math.IsNaN(float64(crSame)) {
		test.Error("compressing repeated bytes - received %v", crSame)
	}
	coll.DropCollection()

	// write a bunch of random bytes to it - expected lower compression
	generateBytes(session, dbName, collName, 50*1024, bytesRandom)
	it = coll.Find(bson.M{}).Iter()
	crRand, err := CompressionRatio(it)
	if err != nil {
		test.Fatalf("Failed to get compression ratio for collection %v", coll)
	}
	if math.IsNaN(float64(crRand)) {
		test.Errorf("compressing last 3 seconds of repeated bytes - received %v", crSame)
	}

	if crRand > crSame {
		test.Errorf("Unexpected compression ratios. Same: %f Random: %f", crSame, crRand)
	}
	coll.DropCollection()

	// if collection doesn't exist - return NaN
	it = session.DB(dbName).C("doesNotExist").Find(bson.M{}).Iter()
	cr, err = CompressionRatio(it)
	if err != nil {
		test.Fatal("Failed to get compression ratio for collection %v", coll)
	}
	if !math.IsNaN(float64(cr)) {
		test.Errorf("Nonexistant collection - expected NaN, received %f", cr)
	}
}

func TestGetOplogInfo(test *testing.T) {
	// oplog doesn't exist
	sessionSingle := dial(standalone_port)
	info, err := GetOplogInfo(sessionSingle)
	if err == nil {
		test.Errorf("Expected error for nonexistent oplog. Result: %d",
			info)
	}
	sessionSingle.Close()

	session := dial(replset_port)
	defer session.Close()

	info1, err := GetOplogInfo(session)
	if err != nil {
		test.Fatal("Error getting oplogInfo")
	}

	session.DB("test").DropDatabase()
	session.DB("test").C("test").Create(&mgo.CollectionInfo{})

	const numDocs = 10
	insertDocuments(session, "test", "test", numDocs)


	info2, err := GetOplogInfo(session)
	if err != nil {
		test.Fatal("Error getting oplog start/end times")
	}
	// end timestamp should have changed
	if info2.endTS <= info1.endTS {
		test.Errorf("End should have changed after inserts. Original: %d, Updated: %d",
			info1.endTS, info2.endTS)
	}

	time.Sleep(5*time.Second)
	size := info2.size
	generateBytes(session, "test", "test", uint64(size), bytesSame)

	// should have rolled over - should have a new start
	info3, err := GetOplogInfo(session)
	if err != nil {
		test.Fatal("Error getting oplog info")
	}
	if info1.startTS >= info3.startTS {
		test.Errorf(
			"Start timestamp did not update after inserts. Original: %d, Updated %d",
			info1.startTS, info3.startTS)
	}
}



