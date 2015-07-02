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

func GetDbPath(session *mgo.Session) (string, error) {
	var results (bson.M)
	session.DB("admin").Run(bson.D{{"getCmdLineOpts",1}}, &results)

	parsed := results["parsed"].(bson.M)
	v, err := getMongodVersion(session)
	if err != nil {
		return "", err
	}

	var dbpath string = "/data/db" // mongodb default
	switch v[0:3]{
	case "2.6" :
		if parsed["dbpath"] != nil {
			dbpath = parsed["dbpath"].(string)
		}
	default :
		storage := parsed["storage"].(bson.M)
		if storage["dbPath"] != nil {
			dbpath = storage["dbPath"].(string)
		}
	}

	return dbpath, err
}

func sumDirFiles(dir string, crawlFurther bool) (int64, error) {
	files, err := getFilesInDir(dir, true)
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

func getWTFileSize(session *mgo.Session) (float64, error) {
	dbpath, err := GetDbPath(session)
	if err != nil {
		return 0, err
	}

	fileSize, err := sumDirFiles(dbpath, true)
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
