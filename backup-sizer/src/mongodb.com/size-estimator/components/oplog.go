package components

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"github.com/mongodb/slogger/v1/slogger"
	"gopkg.in/mgo.v2/bson"
	"time"
	"errors"
)

type OplogID struct {
	TS   bson.MongoTimestamp `bson:"ts"`
	Hash int64               `bson:"h"`
}

func gbPerDay(oplogColl *mgo.Collection) (float32, error) {
	first, last, err := oplogTime(oplogColl)
	if err != nil {
		return 0, err
	}

	totalTime := last - first // in seconds

	_, size, err := isCapped(oplogColl)
	if err != nil {
		return 0, err
	}

	totalSecPerDay := 60 * 60 * 24
	ratio := float32(totalSecPerDay) / float32(totalTime)

	const MB = 1024 * 1024

	gbPerDay := float32(size) * ratio / MB

	return gbPerDay, nil
}


func oplogTime(oplogCollection *mgo.Collection) (int, int, error) {
	var result (bson.M)
	session := oplogCollection.Database.Session
	if err := session.DB("admin").Run(bson.D{{"serverStatus", 1}, {"oplog", 1}}, &result); err != nil {
		panic(err)
		return 0, 0, err
	}

	times := result["oplog"].(bson.M)

	v, err := getMongodVersion(oplogCollection)
	if err != nil {
		return 0, 0, err
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

	first := int(firstMTS >> 32)
	last := int(lastMTS >> 32)

	return first, last, nil
}


func compressionRatio(oplogColl *mgo.Collection, timeInterval time.Duration) (float32, error) {
	const MB = 1024 * 1024

	slicer := NewSlicer(100, 10,
		10 * MB,
		15 * MB,
		new(SnappyCompressor),
		3*time.Second,
		nil,
	)

	resChan := make(chan float32)

	const MaxSlicesBeforeSend = 10
	go slicer.Stream(resChan)
	// hack just to make sure slices are being removed from slicer.Slices channel
	go slicer.Consume(
		func(slices []*Slice) error {
			return nil
		},
		MaxSlicesBeforeSend,
	)

	defer slicer.Kill()

	oplogStartTS := bson.MongoTimestamp(
		0,
//		time.Now().Add(-1 * timeInterval).Unix() << 32,
	)

	qry := bson.M{
		"ts": bson.M{"$gte": oplogStartTS},
}
	oplogIter := oplogColl.Find(qry).LogReplay().Iter()
	var newOplog *bson.D = new(bson.D)

	for oplogIter.Next(newOplog) == true {
		if docWritten := slicer.WriteDoc(newOplog); docWritten == false {
			return 0, errors.New("Failed to enqueue an oplog")
		}
		newOplog = new(bson.D)
	}
	slicer.Close()

	ret := <-resChan
	return ret, nil
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


func OplogSize(uri string, session *mgo.Session) (float32, float32, error) {

	oplogColl, err := getOplogColl(session)
	if err != nil {
		return 0, 0, err
	}


	gb, err := gbPerDay(oplogColl)
	if err != nil {
		return 0, 0, err
	}
	fmt.Println("GB per day: ", gb)

	timeInterval := 3 * time.Hour // TODO: make this a command line option
	cr, err := compressionRatio(oplogColl, timeInterval)
	if err != nil {
		return 0, 0, err
	}
	fmt.Println("compression ratio : ", cr)

	return gb, cr, nil
}

