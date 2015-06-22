package estimator

import (
"fmt"
"gopkg.in/mgo.v2"
	"github.com/mongodb/slogger/v1/slogger"
	"gopkg.in/mgo.v2/bson"
)

func gbPerDay(oplogColl *mgo.Collection) (float32, error) {
	first, last, err := oplogTime(oplogColl)
	if err != nil {
		return 0, err
	}

	fmt.Printf("First time: %d \t Last time: %d\n", last, first)

	totalTime := last - first // in seconds

	_, size, err := isCapped(oplogColl)
	if err != nil {
		return 0, err
	}

	totalSecPerDay := 60 * 60 * 24
	ratio := float32(totalSecPerDay) / float32(totalTime)

	const MB = 1024 * 1024

	gbPerDay := float32(size) * ratio / MB

	fmt.Printf("time: %d \t size: %d \t ratio: %f \t gbPerDay: %f \n", totalTime, size, ratio, gbPerDay)

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
		firstMTS = times["start"]
		lastMTS = times["end"]
	case "2.6" <= v:
		firstMTS = times["earliestOptime"].(bson.MongoTimestamp)
		lastMTS = times["latestOptime"].(bson.MongoTimestamp)
	}

	first := int(firstMTS >> 32)
	last := int(lastMTS >> 32)

	fmt.Println("First: ", first, "Last: ", last)
	return first, last, nil
}


func compressionRatio(oplogColl *mgo.Collection) (float64, error) {
	return 0, nil
	// TODO
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


func OplogSize(uri string, session *mgo.Session) (float32, error) {

	oplogColl, err := getOplogColl(session)
	if err != nil {
		fmt.Printf("welp", err)
		return 0, err
	}


	gb, err := gbPerDay(oplogColl)
	if err != nil {
		return gb, err
	}
	fmt.Println("GB per day: ", gb)
	compressionRatio(oplogColl)

	return 0, nil
}

