package components

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
)

type SizeStats struct {
	DataSize  int64
	IndexSize int64
	FileSize  int64
}

func sumDirFiles(dir string, session *mgo.Session, crawlFurther bool) (int64, error) {
	files, err := GetDBFiles(session)
	if err != nil {
		return 0, err
	}
	fileSize := int64(0)
	for _, fname := range files {
		fi, err := os.Stat(fname)
		if err != nil {
			return 0, err
		}
		fileSize += fi.Size()
	}
	return fileSize, nil
}

func getWTFileSize(session *mgo.Session) (int64, error) {
	dbpath, err := GetDbPath(session)
	if err != nil {
		return 0, err
	}

	fileSize, err := sumDirFiles(dbpath, session, true)
	if err != nil {
		return 0, err
	}

	return int64(fileSize), nil
}

func GetSizeStats(session *mgo.Session) (*SizeStats, error) {
	dbs, err := session.DatabaseNames()
	if err != nil {
		return nil, err
	}

	dataSize := int64(0)
	fileSize := int64(0)
	indexSize := int64(0)

	fs := false

	results := struct {
		DataSize  int64 `bson:"dataSize"`
		FileSize  int64 `bson:"fileSize"`
		IndexSize int64 `bson:"indexSize"`
	}{}
	for _, db := range dbs {
		if err := session.DB(db).Run(bson.D{{"dbStats", 1}}, &results); err != nil {
			return nil, err
		}
		dataSize += results.DataSize
		indexSize += results.IndexSize
		v := results.FileSize
		if v != 0 {
			fileSize += v
			fs = true
		}
	}

	if !fs {
		fileSize, err = getWTFileSize(session)
		if err != nil {
			return nil, err
		}
	}

	return &SizeStats{
		dataSize, indexSize, fileSize,
	}, nil
}
