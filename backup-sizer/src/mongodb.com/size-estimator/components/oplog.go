package components

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/golang/snappy"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

type OplogInfo struct {
	startTS bson.MongoTimestamp
	endTS   bson.MongoTimestamp
	size    int
}

type OplogStats struct {
	StartTS            bson.MongoTimestamp
	EndTS              bson.MongoTimestamp
	Size               int
	GbPerDay           float64
	CompressionRatio   float64
	CompressedGbPerDay float64
}

var OplogNotFoundError = errors.New("local.oplog.rs does not seem to exist.\n")

func (info *OplogInfo) GbPerDay() (float64, error) {
	first := int32(info.startTS >> 32)
	last := int32(info.endTS >> 32)
	size := info.size

	if first > last {
		return 0,
			fmt.Errorf("Start timestamp (%f) cannot be later than end timestamp (%f)\n",
				info.startTS, info.endTS)
	}

	totalTime := last - first
	if totalTime == 0 {
		totalTime = 1
	}

	const secPerDay = 60 * 60 * 24
	ratio := float64(secPerDay) / float64(totalTime)

	const GB = 1024 * 1024 * 1024
	sizeInGB := float64(size) / float64(GB)

	gbPerDay := sizeInGB * ratio
	return gbPerDay, nil
}

func GetOplogIterator(startTime time.Time, timeInterval time.Duration,
	session *mgo.Session) (*mgo.Iter, error) {
	oplogColl, err := getOplogColl(session)
	if err != nil {
		return nil, err
	}

	startTS := bson.MongoTimestamp(
		startTime.Add(-1*timeInterval).Unix() << 32,
	)

	qry := bson.M{
		"ts": bson.M{"$gte": startTS},
	}

	return oplogColl.Find(qry).LogReplay().Iter(), nil
}

func CompressionRatio(iter *mgo.Iter) (float64, error) {
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
			uncompressed = uncompressed + len(dataBuffer.Bytes())

			compressedBytes := snappy.Encode(nil, dataBuffer.Bytes())
			compressed = compressed + len(compressedBytes)

			dataBuffer.Reset()
		}
	}

	if len(dataBuffer.Bytes()) != 0 {
		uncompressed = uncompressed + len(dataBuffer.Bytes())

		compressedBytes := snappy.Encode(nil, dataBuffer.Bytes())
		compressed = compressed + len(compressedBytes)
	}

	return float64(uncompressed) / float64(compressed), nil
}

func GetOplogInfo(session *mgo.Session) (*OplogInfo, error) {
	exists, err := collExists(session.DB("local").C("oplog.rs"))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, OplogNotFoundError
	}

	var result bson.M
	err = serverStatus(session, &result)
	if err != nil {
		return nil, err
	}

	times := result["oplog"].(bson.M)

	var firstMTS bson.MongoTimestamp
	var lastMTS bson.MongoTimestamp

	firstMTS = times["earliestOptime"].(bson.MongoTimestamp)
	lastMTS = times["latestOptime"].(bson.MongoTimestamp)

	size, err := getOplogSize(session)
	if err != nil {
		return nil, err
	}

	return &OplogInfo{
		startTS: firstMTS,
		endTS:   lastMTS,
		size:    size,
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
		return nil, OplogNotFoundError
	}

	return oplog, nil
}

func GetOplogStats(session *mgo.Session, timeInterval time.Duration) (*OplogStats, error) {
	oplogInfo, err := GetOplogInfo(session)
	if err != nil {
		return nil, err
	}
	gb, err := oplogInfo.GbPerDay()
	if err != nil {
		return nil, err
	}

	iter, err := GetOplogIterator(time.Now(), timeInterval, session)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	cr, err := CompressionRatio(iter)
	if err != nil {
		return nil, err
	}

	return &OplogStats{
		oplogInfo.startTS,
		oplogInfo.endTS,
		oplogInfo.size,
		gb,
		cr,
		gb / cr,
	}, nil
}
