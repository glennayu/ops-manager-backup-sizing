package components

import (
	"bytes"
	"encoding/base64"
	"gopkg.in/mgo.v2/bson"
	"io/ioutil"
)

type SliceConsumer func([]*Slice) error

type Slice struct {
	bytes.Buffer
	NumDocs           int
	UnzippedSize      int
	FirstDoc          *bson.D
	LastDoc           *bson.D
	SliceNum          int
	CompressionFormat string
}

func (slice *Slice) addDoc(bytes []byte, doc *bson.D) {
	slice.NumDocs += 1
	slice.UnzippedSize += len(bytes)
	slice.LastDoc = doc
}

func (slice *Slice) b64EncodeData() (string, error) {
	data, err := ioutil.ReadAll(slice)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(data), nil
}
