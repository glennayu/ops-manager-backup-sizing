package components

import (
	"time"
	"errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"github.com/mongodb/slogger/v1/slogger"
	"bytes"
	"github.com/golang/snappy/snappy"
	"fmt"
)

type OplogInfo struct {
	startTS bson.MongoTimestamp
	endTS   bson.MongoTimestamp
	size    int
}

type OplogStats struct {
	startTS              bson.MongoTimestamp
	endTS                bson.MongoTimestamp
	size                 int
	sizePerDay           float32
	compressedSizePerDay float32
}

func GbPerDay(info *OplogInfo) (float32) {
	first := int32(info.startTS >> 32)
	last := int32(info.endTS >> 32)
	size := info.size

	totalTime := last - first // in seconds
	fmt.Println(first, last, totalTime, size)

	const secPerDay = 60 * 60 * 24
	ratio := float32(secPerDay) / float32(totalTime)

	const MB = 1024 * 1024
	gbPerDay := float32(size) * ratio / MB

	return gbPerDay
}

func getIterator(timeInterval time.Duration, coll *mgo.Collection) *mgo.Iter {
	oplogStartTS := bson.MongoTimestamp(
		time.Now().Add(-1 * timeInterval).Unix() << 32,
	)

	qry := bson.M{
		"ts": bson.M{"$gte": oplogStartTS},
	}
	return coll.Find(qry).LogReplay().Iter()
}

func CompressionRatio(coll *mgo.Collection, timeInterval time.Duration) (float32, error) {
	const MB = 1024 * 1024

	iter := getIterator(timeInterval, coll)

	var doc *bson.D = new(bson.D)


	uncompressedSize := 0
	compressedSize := 0

	maxCapacity := 15 * MB
	var dataBuffer bytes.Buffer

	for iter.Next(doc) == true {
		docBytes, err := bson.Marshal(doc)

		// if current buffersize + doc > maxCapacity
		// TODO: corner case: if length of the first doc is already too large --- eh, should take care of it
		if len(dataBuffer.Bytes()) + len(docBytes) > maxCapacity {
			// compress
			compressedBytes, err := snappy.Encode(nil, dataBuffer.Bytes())
			if err != nil {
				// todo fill this in yo
			}
			// update counters
			compressedSize += len(compressedBytes)
			uncompressedSize += len(dataBuffer.Bytes())
			// clear buffer
			dataBuffer.Reset()
		}
		// add doc to the buffer
		if err != nil {
			// todo fill this in yo
		}
		dataBuffer.Write(docBytes)
		doc = new(bson.D)
	}

	return float32(uncompressedSize) / float32(compressedSize), nil
}

func getOplogColl(session *mgo.Session) (*mgo.Collection, error) {
	oplog := session.DB("local").C("oplog.rs")
	exists, err := collExists(oplog)
	if err != nil {
		return nil, err
	}

	if exists == false {
		return nil, slogger.NewStackError("`local.oplog.rs` does not seem to exist.")
	}

	return oplog, nil
}

func GetOplogInfo(session *mgo.Session) (*OplogInfo, error) {
	var result (bson.M)
	if err := session.DB("admin").Run(bson.D{{"serverStatus", 1}, {"oplog", 1}}, &result); err != nil {
		panic(err)
		return nil, err
	}

	times := result["oplog"].(bson.M)

	v, err := getMongodVersion(session)
	if err != nil {
		return nil, err
	}

	var firstMTS bson.MongoTimestamp
	var lastMTS bson.MongoTimestamp

	switch  {
	case "2.4" <= v && v < "2.6":
		firstMTS = times["start"].(bson.MongoTimestamp)
		lastMTS = times["end"].(bson.MongoTimestamp)
	case "2.6" <= v:
		firstMTS = times["earliestOptime"].(bson.MongoTimestamp)
		lastMTS = times["latestOptime"].(bson.MongoTimestamp)
	}

	size, err := getOplogSize(session)
	if err != nil {
		return nil, err
	}

	return &OplogInfo{
		startTS: firstMTS,
		endTS: lastMTS,
		size: size,
	}, nil
}

func getMongodVersion(session *mgo.Session) (string, error) {
	cmd := bson.D{
		{"buildInfo", 1},
	}
	var result (bson.M)

	if err := session.DB("admin").Run(cmd, &result); err != nil {
		return "", err
	}

	return result["version"].(string), nil
}

func getOplogSize(session *mgo.Session) (int, error) {
	var result (bson.M)
	err := session.DB("local").Run(bson.D{"collStats", "oplog.rs"}, &result)

	if err != nil {
		return -1, err
	}
	if result["capped"] == false {
		return -1, errors.New("Oplog is not capped")
	}

	if result["maxSize"] != nil {
		return result["maxSize"].(int), nil
	}
	return result["size"].(int), nil
}

func GetOplogStats(uri string, session *mgo.Session) (*OplogStats, error) {

	oplogInfo, err := GetOplogInfo(session)
	if err != nil {
		return nil, err
	}
	gb := GbPerDay(oplogInfo)

	timeInterval := 3 * time.Hour // TODO: make this a command line option

	oplogColl, err := getOplogColl(session)
	if err != nil {
		return nil, err
	}
	cr, err := CompressionRatio(oplogColl, timeInterval)
	if err != nil {
		return nil, err
	}

	return &OplogStats{
		oplogInfo.startTS,
		oplogInfo.endTS,
		oplogInfo.size,
		gb,
		cr,
	}, nil
}

