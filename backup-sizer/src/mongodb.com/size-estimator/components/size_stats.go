package components
import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
)

type SizeStats struct {
	DataSize 	float64
	IndexSize	float64
	FileSize	float64
}

func sumDirFiles(dir string, session *mgo.Session, crawlFurther bool) (int64, error) {
	files, err := GetDBFiles(session)
	if err != nil {
		return 0, err
	}
	fileSize := int64(0)
	for _, fname := range *files {
		fi, err := os.Stat(fname)
		if err != nil {
			return 0, err
		}
		fileSize += fi.Size()
	}
	return fileSize, nil
}

func getWTFileSize(session *mgo.Session) (float64, error) {
	dbpath, err := GetDbPath(session)
	if err != nil {
		return 0, err
	}

	fileSize, err := sumDirFiles(dbpath, session, true)
	if err != nil {
		return 0, err
	}

	return float64(fileSize), nil
}

func GetSizeStats(session *mgo.Session) (*SizeStats, error) {
	dbs, err := session.DatabaseNames()
	if err != nil {
		return nil, err
	}

	dataSize := float64(0)
	fileSize := float64(0)
	indexSize := float64(0)

	fs := false

	var results (bson.M)
	for _, db := range dbs {
		if err := session.DB(db).Run(bson.D{{"dbStats", 1}}, &results); err != nil {
			return nil, err
		}
		dataSize += results["dataSize"].(float64)
		indexSize += results["indexSize"].(float64)
		v := results["fileSize"]
		if v != nil {
			fileSize += v.(float64)
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
