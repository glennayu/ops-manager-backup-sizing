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

func GbPerDay(info *OplogInfo) (float64, error) {
	mb, err := MbPerDay(info)
	if err != nil {
		return 0, err
	}
	return float64(mb) / 1024, nil
}

func MbPerDay(info *OplogInfo) (float32, error) {
	first := int32(info.startTS >> 32)
	last := int32(info.endTS >> 32)
	size := info.size

	if first > last {
		return 0,
		fmt.Errorf("Start timestamp (%f) cannot be later than end timestamp (%f)",
			info.startTS, info.endTS)
	}

	totalTime := last - first
	if totalTime == 0 {
		totalTime = 1
	}

	const secPerDay = 60 * 60 * 24
	ratio := float32(secPerDay) / float32(totalTime)

	const MB = 1024 * 1024
	sizeInMB := float32(size) / float32(MB)

	mbPerDay := sizeInMB * ratio

	return mbPerDay, nil
}

func GetOplogIterator(startTime time.Time, timeInterval time.Duration,
session *mgo.Session) (*mgo.Iter, error) {
	oplogColl, err := getOplogColl(session)
	if err != nil {
		return nil, err
	}

	startTS := bson.MongoTimestamp(
		startTime.Add(-1 * timeInterval).Unix() << 32,
	)

	qry := bson.M{
		"ts": bson.M{"$gte": startTS},
	}

	return oplogColl.Find(qry).LogReplay().Iter(), nil
}

func CompressionRatio(iter *mgo.Iter) (float32, error) {
	const MB = 1024 * 1024

	var doc *bson.D = new(bson.D)

	uncompressed := 0
	compressed := 0

	minSize := 10 * MB
	var dataBuffer bytes.Buffer

	for iter.Next(doc) == true {
		docBytes, err := bson.Marshal(doc)
		if err != nil {
			return 0, err
		}

		dataBuffer.Write(docBytes)
		doc = new(bson.D)
		if len(dataBuffer.Bytes()) > minSize {
			compressed, uncompressed, err =
			compressResults(&dataBuffer, compressed, uncompressed)
			if err != nil {
				return 0, nil
			}
			dataBuffer.Reset()
		}
	}

	if len(dataBuffer.Bytes()) != 0 {
		var err error
		compressed, uncompressed, err =
		compressResults(&dataBuffer, compressed, uncompressed)
		if err != nil {
			return 0, err
		}
	}

	return float32(uncompressed) / float32(compressed), nil
}

func compressResults(data *bytes.Buffer, compressed, uncompressed int) (int, int, error) {
	compressedBytes, err := snappy.Encode(nil, data.Bytes())
	if err != nil {
		return 0, 0, err
	}
	return compressed + len(compressedBytes), uncompressed + len(data.Bytes()), nil
}

func GetOplogInfo(session *mgo.Session) (*OplogInfo, error) {
	exists, err := collExists(session.DB("local").C("oplog.rs"))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("Oplog does not seem to exist")
	}

	var result (bson.M)
	if err := session.DB("admin").Run(bson.D{{"serverStatus", 1}, {"oplog", 1}}, &result); err != nil {
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
	err := session.DB("local").Run(bson.D{{"collStats", "oplog.rs"}}, &result)

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

func GetOplogStats(session *mgo.Session, timeInterval time.Duration) (*OplogStats, error) {
	oplogInfo, err := GetOplogInfo(session)
	if err != nil {
		return nil, err
	}

	iter, err := GetOplogIterator(time.Now(), timeInterval, session)
	if err != nil {
		return nil, err
	}

	mb, err := MbPerDay(oplogInfo)
	if err != nil {
		return nil, err
	}

	cr, err := CompressionRatio(iter)
	if err != nil {
		return nil, err
	}

	return &OplogStats{
		oplogInfo.startTS,
		oplogInfo.endTS,
		oplogInfo.size,
		mb,
		cr,
	}, nil
}

