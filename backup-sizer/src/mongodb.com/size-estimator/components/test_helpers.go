package components

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"strconv"
)

const standalone_mmap = 26000
const replset_port = 28000
const replset_wt_dirPerDb = 29000
const wt_port_custPath = 26100
const wt_root = 26200
const wt_port_defPath = 26300
const dbName = "test"
const collName = "test"

func dial(port int) *mgo.Session {
	addr := "localhost:" + strconv.Itoa(port)
	session, err := mgo.Dial(addr)
	if err != nil {
		panic(fmt.Sprintf("Error dialing. Addr: %v Err: %v", addr, err))
	}
	return session
}

func insertDocuments(mongo *mgo.Session, database string, collection string, numInsert int) {
	session := mongo.Clone()
	defer session.Close()

	session.SetMode(mgo.Strong, false)
	for idx := 0; idx < numInsert; idx++ {
		session.DB(database).C(collection).Insert(bson.M{"number": idx})
	}
}

func removeDocuments(mongo *mgo.Session, database string, collection string, numRemove int) {
	session := mongo.Clone()
	defer session.Close()

	session.SetMode(mgo.Strong, false)
	for idx := 0; idx < numRemove; idx++ {
		session.DB(database).C(collection).Remove(bson.M{"number": idx})
	}
}

const bytesSame = 0
const bytesRandom = 1

func randomBytes(cap int32) []byte {
	b := make([]byte, cap)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("error:", err)
		return nil
	}
	return b
}

func generateBytes(mongo *mgo.Session, database string, collection string, numBytes uint64, mode int) {
	session := mongo.Copy()
	defer session.Close()

	var bytesGenerated uint64 = 0
	var bytesVal []byte

	for bytesGenerated < numBytes {
		switch mode {
		case bytesSame:
			bytesVal = bytes.Repeat([]byte{0}, 5*1024)
		case bytesRandom:
			bytesVal = randomBytes(5 * 1024)
		}
		toAdd := &bson.D{
			{"_id", bytesGenerated},
			{"bytes", bytesVal},
		}

		session.DB(database).C(collection).Insert(toAdd)
		bytesGenerated += 5 * 1024
	}
}
