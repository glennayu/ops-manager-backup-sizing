package main

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"os"
	"./mongodb.com/size-estimator/components"
	"flag"
	"time"
	"reflect"
	"strconv"
	"gopkg.in/mgo.v2/bson"
)

const (
	DefaultPort = 27017
	DefaultHostName = "localhost"
	DefaultSleepTime = time.Duration(6*time.Hour)
	DefaultIter = 12
)

var (
	host = flag.String("host", DefaultHostName, "Hostname to ping")
	port = flag.Int("port", DefaultPort, "Port for the offline agent to ping")
	sleepTime time.Duration
	numIter int
	uri string
)

func init() {
	flag.DurationVar(&sleepTime, "interval", DefaultSleepTime, "How long to sleep between iterations")
	flag.IntVar(&numIter, "iter", DefaultIter, "Number of iterations")
}

func main() {
	flag.Parse()

	uri := fmt.Sprintf("%s:%d", *host, *port)

	fmt.Printf("Running on port %s every %v for %d iterations.", uri, sleepTime, numIter)

	session, err := mgo.Dial(uri)
	if err != nil {
		fmt.Printf("Failed to dial MongoDB on port %v. Err %v", uri, err)
		os.Exit(1)
	}
	defer session.Close()

	err = session.Ping();
	if err != nil {
		fmt.Printf("Failed to contact server on %s. Err %v", uri, err)
		os.Exit(1)
	}

	fmt.Printf("Successfully connected to %s\n", uri);

	Run()
}

func Run() {
	printFields()

	for iter := 0; iter < numIter; iter++ {
		start := time.Now()
		Iterate()
		continue
		sleep := RemainingSleepTime(start)
		time.Sleep(sleep)
	}
}

func RemainingSleepTime(start time.Time) time.Duration {
	return sleepTime - time.Now().Sub(start)
}

func Iterate() {
	session, err := mgo.Dial(uri)
	if err != nil {
		fmt.Printf("Failed to dial MongoDB on port %v. Err %v", uri, err)
		return
	}
	defer session.Close()

	oplogStats, err := components.GetOplogStats(session, sleepTime)
	if err != nil {
		fmt.Printf("Failed to get oplog stats on server %s. Err: %v", uri, err)
		os.Exit(1)
	}
	printVals(oplogStats)
}

func printFields() {
	oplog := &components.OplogStats{}
	s := reflect.ValueOf(oplog).Elem()

	var buffer []byte

	for i := 0; i < s.NumField(); i++ {
		buffer = append(buffer, s.Type().Field(i).Name ...)
		buffer = append(buffer, ',')
	}
	fmt.Println(string(buffer[0:len(buffer) - 1]))
	}

func printVals(oplog *components.OplogStats) {
	s := reflect.ValueOf(oplog).Elem()

	var buffer []byte

	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		val := f.Interface()
		switch val.(type) {
		case int32 :
			buffer = strconv.AppendInt(buffer, val.(int64), 10)
		case int64 :
			buffer = strconv.AppendInt(buffer, val.(int64), 10)
		case int :
			buffer = append(buffer, strconv.Itoa(val.(int))...)
		case float32 :
			buffer = strconv.AppendFloat(buffer, val.(float64), 'f', 3, 32 )
		case float64 :
			buffer = strconv.AppendFloat(buffer, val.(float64), 'f', 3, 64 )
		case string :
			buffer = append(buffer, val.(string)...)
		default :
			strname := reflect.TypeOf(val).Name()
			switch strname {
			case "MongoTimestamp":
				buffer = strconv.AppendInt(buffer, int64(val.(bson.MongoTimestamp)), 10)
			}
		}
		buffer = append(buffer, ',')
	}
	str := string(buffer[0:len(buffer)-1])
	fmt.Println(str)
}