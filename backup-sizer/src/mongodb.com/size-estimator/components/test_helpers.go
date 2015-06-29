package components

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"strconv"
	"fmt"
	"crypto/rand"
	"bytes"
)


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