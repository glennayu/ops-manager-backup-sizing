package components
import (
	"testing"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
	"math"
)

const replset_port = 27017
const standalone_port = 26000
const dbName = "test"
const collName = "test"

type oplogInfoTest struct {
	info     *OplogInfo
	expected float64
	desc     string
}

func TestGbPerDay(test *testing.T) {
	const MB = 1024*1024
	const GB = 1024*1024*1024
	const SecPerDay = 86400

	testCases := []oplogInfoTest{
		{&OplogInfo{
			bson.MongoTimestamp(int64(0) << 32),
			bson.MongoTimestamp(int64(SecPerDay) << 32),
			GB}, 1, "basic"},
		{&OplogInfo{
			bson.MongoTimestamp(time.Now().Unix() << 32),
			bson.MongoTimestamp(time.Now().Add(72 * time.Hour).Unix() << 32),
			193 * MB}, float64(193) / float64(1024 * 3), "normal case"},
		{&OplogInfo{
			bson.MongoTimestamp(0),
			bson.MongoTimestamp(0),
			0}, 0 * SecPerDay, "start, end, size = 0"},
		{&OplogInfo{
			bson.MongoTimestamp(int64(5) << 32 | 0),
			bson.MongoTimestamp(int64(5) << 32 | 5),
			1 * GB}, 1 * SecPerDay, "total time < 1 second, with multiple entries"},
		{&OplogInfo{
			bson.MongoTimestamp(int64(5) << 32 | 5),
			bson.MongoTimestamp(int64(5) << 32 | 5),
			1 * GB}, 1 * SecPerDay, "total time < 1 second, with one entry"},
	}

	for _, testCase := range testCases {
		info := testCase.info
		expected := testCase.expected
		desc := testCase.desc

		gb, err := info.GbPerDay()
		if err != nil {
			test.Errorf("Failure getting GB per day. Err %v", err)
		}
		if gb != expected {
			test.Errorf("Testing %s, OplogInfo %v, expected %f mb per day written, received %f",
				desc, info, expected, gb)
		}
	}

	info := OplogInfo{
		bson.MongoTimestamp(time.Now().Unix() << 32),
		bson.MongoTimestamp(time.Now().Add(-1 * time.Hour).Unix() << 32),
		1*MB}
	gb, err := info.GbPerDay()
	if err == nil {
		test.Errorf("Expected error for start > end. Result: %f", gb)
	}
}

func TestGetIterator(test *testing.T) {
	session := dial(replset_port)
	defer session.Close()

	session.DB(dbName).DropDatabase()

	coll := session.DB(dbName).C(collName)
	coll.Create(&mgo.CollectionInfo{})
	it, err := GetOplogIterator(time.Now(), time.Second, session)
	if err != nil {
		test.Errorf("Failed to get iterator. Err: %v", err)
	}

	var res bson.D
	if it.Next(res) {
		test.Errorf("Empty collection - expected empty iterator")
	}

	time.Sleep(3*time.Second)
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


	session.DB(dbName).DropDatabase()

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
		test.Errorf("compressing random bytes - received %v", crSame)
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
	defer sessionSingle.Close()

	info, err := GetOplogInfo(sessionSingle)
	if err == nil {
		test.Errorf("Expected error for nonexistent oplog. Result: %d",
		info)
	}

	session := dial(replset_port)
	defer session.Close()

	info1, err := GetOplogInfo(session)
	if err != nil {
		test.Fatal("Error getting oplogInfo")
	}

	session.DB(dbName).DropDatabase()
	session.DB(dbName).C(collName).Create(&mgo.CollectionInfo{})

	const numDocs = 10
	insertDocuments(session, dbName, collName, numDocs)


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
	generateBytes(session, dbName, collName, uint64(size), bytesSame)

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



