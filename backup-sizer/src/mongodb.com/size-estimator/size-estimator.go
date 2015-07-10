package main

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"os"
	. "mongodb.com/size-estimator/components"
	"flag"
	"time"
	"reflect"
	"strconv"
	"gopkg.in/mgo.v2/bson"
)

const (
	kb = 1024
	mb = 1024 * kb
	DefaultPort = 27017
	DefaultHostName = "localhost"
	DefaultSleepTime = time.Duration(6*time.Hour)
	DefaultIter = 12
	DefaultHashDir = "hashes"
	DefaultFalsePosRate = 0.01
)

var (
	host string
	port int
	sleepTime time.Duration
	numIter int
	uri string
	hashDir string
	falsePosRate float64
	blocksizes = []int{64 * kb,
		128 * kb,
		256 * kb,
		512 * kb,
		1 * mb,
		2 * mb,
		4 * mb,
		8 * mb,
		16 * mb,
	}
)


func init() {
	flag.StringVar(&host, "host", DefaultHostName, "Hostname to ping")
	flag.IntVar(&port, "port", DefaultPort, "Port for the offline agent to ping")
	flag.DurationVar(&sleepTime, "interval", DefaultSleepTime, "How long to sleep between iterations")
	flag.IntVar(&numIter, "iterations", DefaultIter, "Number of iterations")
	flag.StringVar(&hashDir, "hashDir", DefaultHashDir, "Directory to store block hashes")
	flag.Float64Var(&falsePosRate, "falsePos", DefaultFalsePosRate, "False positive rate for duplicated hashes")
}

func main() {
	flag.Parse()
	uri = fmt.Sprintf("%s:%d", host, port)

	fmt.Printf("Running on port %s every %v for %d iterations.\n", uri, sleepTime, numIter)

	session, err := mgo.Dial(uri)
	if err != nil {
		fmt.Printf("Failed to dial MongoDB on port %v. Err %v\n", uri, err)
		os.Exit(1)
	}
	defer session.Close()

	err = session.Ping();
	if err != nil {
		fmt.Printf("Failed to contact server on %s. Err %v\n", uri, err)
		os.Exit(1)
	}

	fmt.Printf("Successfully connected to %s\n", uri);

	exists, err := CheckExists(hashDir)
	if err != nil {
		fmt.Printf("Failure checking directory %s exists. Err %v\n", hashDir, err)
		os.Exit(1)
	}
	if exists {
		err := os.RemoveAll(hashDir)
		if err != nil {
			fmt.Printf("Failure removing directory %s. Err %v\n", hashDir, err)
		}
	}
	Run()
}

func Run() {
		printFields()

	for iter := 0; iter < numIter; iter++ {
		start := time.Now()
		Iterate(iter)
		sleep := RemainingSleepTime(start)
		time.Sleep(sleep)
	}
}

func RemainingSleepTime(start time.Time) time.Duration {
	return sleepTime - time.Now().Sub(start)
}

func Iterate(iter int) {
	session, err := mgo.Dial(uri)
	if err != nil {
		fmt.Printf("Failed to dial MongoDB on port %v. Err %v\n", uri, err)
		return
	}
	defer session.Close()

	oplogStats, err := GetOplogStats(session, sleepTime)
	if err != nil {
		fmt.Printf("Failed to get oplog stats on server %s. Err: %v\n", uri, err)
		os.Exit(1)
	}

	sizeStats, err := GetSizeStats(session)
	if err != nil {
		fmt.Printf("Failed to get sizing stats on server %s. Err: %v\n", uri, err)
		os.Exit(1)
	}

	dbpath, err := GetDbPath(session)
	if err != nil {
		fmt.Printf("Failed to get directory path for session on server %s. Err:%v\n", uri, err)
		os.Exit(1)
	}


	blockStats, err := GetBlockHashes(dbpath, hashDir, falsePosRate, iter, blocksizes)
	if err != nil {
		fmt.Printf("Failed to get block hashes on server %s. Err %v\n", uri, err)
		os.Exit(1)
	}
	stats := []interface{}{
		oplogStats,
		sizeStats,
		blockStats,
	}

	printVals(&stats)
}

// todo ok this needs to be fixed but ibunno how welpedy halp
func printFields() {
	allStats := []interface{} {
		&OplogStats{},
		&SizeStats{},
	}

	var buffer []byte
	for _, stats := range allStats {

		s := reflect.ValueOf(stats).Elem()

		for i := 0; i < s.NumField(); i++ {
			buffer = append(buffer, s.Type().Field(i).Name ...)
			buffer = append(buffer, ',')
		}
	}

	// this is just going to have to be hardcoded for now.
	for _, bs := range blocksizes {
		s := fmt.Sprintf("DedupRate(%d),DataCompressionRate(%d),", bs, bs)
		buffer = append(buffer, s...)
	}

	fmt.Println(string(buffer[0:len(buffer) - 1]))
}

func toString(val interface{}) []byte {
	var s string
	switch val.(type) {
		case int32, int64 :
		s = strconv.FormatInt(val.(int64), 10)
		case int :
		s = strconv.Itoa(val.(int))
		case float32 :
		s = strconv.FormatFloat(val.(float64), 'f', 3, 32)
		case float64 :
		s = strconv.FormatFloat(val.(float64), 'f', 3, 64)
		case string :
		s = val.(string)
		default :
		strname := reflect.TypeOf(val).Name()
		switch strname {
		case "MongoTimestamp":
			s = strconv.FormatInt(int64(val.(bson.MongoTimestamp)), 10)
		}
	}
	return []byte(s)
}

func printVals(allStats *[]interface{}) {
	var buffer []byte

	for _, stats := range *allStats {
		v := reflect.ValueOf(stats)
		s := v.Elem()

		if s.Kind() == reflect.Map {
			blockStatsMapPtr := stats.(*AllBlockSizeStats)
			for _, size := range blocksizes {
				blockstat := (*blockStatsMapPtr)[size]
				buffer = append(buffer, toString(blockstat.DedupRate) ...)
				buffer = append(buffer, "," ...)
				buffer = append(buffer, toString(blockstat.DataCompressionRatio)...)
				buffer = append(buffer, "," ...)
			}
		} else {
			for i := 0; i < s.NumField(); i++ {
				f := s.Field(i)
				val := f.Interface()
				buffer = append(buffer, toString(val)...)
//				switch val.(type) {
//					case int32 :
//					buffer = strconv.AppendInt(buffer, val.(int64), 10)
//					case int64 :
//					buffer = strconv.AppendInt(buffer, val.(int64), 10)
//					case int :
//					buffer = append(buffer, strconv.Itoa(val.(int))...)
//					case float32 :
//					buffer = strconv.AppendFloat(buffer, val.(float64), 'f', 3, 32)
//					case float64 :
//					buffer = strconv.AppendFloat(buffer, val.(float64), 'f', 3, 64)
//					case string :
//					buffer = append(buffer, val.(string)...)
//					default :
//					strname := reflect.TypeOf(val).Name()
//					switch strname {
//					case "MongoTimestamp":
//						buffer = strconv.AppendInt(buffer, int64(val.(bson.MongoTimestamp)), 10)
//					}
//				}
				buffer = append(buffer, ',')
			}
		}
	}
	str := string(buffer[0:len(buffer)-1])
	fmt.Println(str)
	return
}
