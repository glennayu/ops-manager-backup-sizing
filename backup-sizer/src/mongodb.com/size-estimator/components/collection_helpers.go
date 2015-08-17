package components

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type StorageEngine string

const (
	wiredTiger StorageEngine = "wiredTiger"
	mmap       StorageEngine = "mmapv1"
)

/******* BackupSizingOpts *********/

type BackupSizingOpts struct {
	Host         string
	Port         int
	SleepTime    time.Duration
	NumIter      int
	Uri          string
	HashDir      string
	FalsePosRate float64
	NumCPUs      int
}

func (opts BackupSizingOpts) GetSession() *mgo.Session {
	session, err := mgo.Dial(opts.Uri)
	if err != nil {
		fmt.Printf("Failed to dial MongoDB on port %s. Err %v\n", opts.Uri, err)
		os.Exit(1)
	}
	return session
}

/******* Methods on mongo session *********/

func GetDbPath(session *mgo.Session) (string, error) {
	var results (bson.M)
	session.DB("admin").Run(bson.D{{"getCmdLineOpts", 1}}, &results)

	parsed := results["parsed"].(bson.M)
	v, err := getMongodVersion(session)
	if err != nil {
		return "", err
	}
	var dbpath string = "/data/db" // mongodb default
	switch v[0:3] {
	case "2.4":
		if parsed["dbpath"] != nil {
			dbpath = parsed["dbpath"].(string)
		}
	default:
		storage := parsed["storage"].(bson.M)
		if storage["dbPath"] != nil {
			dbpath = storage["dbPath"].(string)
		}
	}
	return dbpath, err
}

func GetOplogCollStats(session *mgo.Session, result *bson.M) error {
	err := session.DB("local").Run(bson.D{{"collStats", "oplog.rs"}}, &result)
	if err != nil {
		return err
	}
	return nil
}

// Returns database files relevant for backup, excludingq files based on storage engine
func GetDBFiles(session *mgo.Session) (*[]string, error) {
	excludeRegexes, err := getExcludeFileRegexes(session)
	if err != nil {
		return nil, err
	}

	dbpath, err := GetDbPath(session)
	if err != nil {
		return nil, err
	}

	return GetFilesInDir(dbpath, excludeRegexes, true)
}

func getExcludeFileRegexes(session *mgo.Session) (*[]string, error) {
	if session == nil {
		return nil, fmt.Errorf("Failure to get regexes for files to exclude--session is nil")
	}
	storageEngine, err := getStorageEngine(session)
	if err != nil {
		return nil, err
	}

	var excludeRegexes []string
	switch storageEngine {
	case wiredTiger:
		oplogFile := getOplogFile(session)
		excludeRegexes = []string{"mongod.lock", "WiredTiger.basecfg", "mongodb.log", "journal", oplogFile}
	case mmap:
		excludeRegexes = []string{"mongod.lock", "local.*", "mongodb.log", "journal"}
	}

	return &excludeRegexes, nil
}

// For WT replica sets only
// Can find the file for the oplog in WT by running collStats on oplog.rs collection
//
//	  "wiredTiger" : {
//         "uri" : "statistics:table:collection-6--1120041349777087752"
//    }
//
func getOplogFile(session *mgo.Session) string {
	var result bson.M
	err := GetOplogCollStats(session, &result)
	if err != nil {
		return ""
	}

	wtDoc := result["wiredTiger"].(bson.M)
	if wtDoc == nil {
		return ""
	}

	filebase := strings.TrimPrefix(wtDoc["uri"].(string), "statistics:table:")
	return filebase
}

func getStorageEngine(session *mgo.Session) (StorageEngine, error) {
	var result bson.M
	err := serverStatus(session, &result)
	if err != nil {
		return "", err
	}

	// for versions prior to 3.0, serverStatus.storageEngine does not exist
	if result["storageEngine"] == nil {
		return mmap, nil
	}
	storageEngine := result["storageEngine"].(bson.M)
	se := StorageEngine(storageEngine["name"].(string))
	return se, nil
}

func serverStatus(session *mgo.Session, result *bson.M) error {
	if err := session.DB("admin").Run(bson.D{{"serverStatus", 1}, {"oplog", 1}}, result); err != nil {
		return err
	}
	return nil
}

/******* Methods on mongo collections *********/

// Taken from mms-backup components:
// This method returns whether the collection exists. Due to 2.8
// supporting multiple storage engines, this cannot rely on only
// `listCollections` nor `system.namespaces`. Take the easy road and let
// the mgo driver deal with that for us. Unfortunately it returns all
// collections. This method is only used to find the oplog collection
// (`local` database), so hopefully the client did not create a bunch of
// collections in their `local` database.
func collExists(coll *mgo.Collection) (bool, error) {
	collections, err := coll.Database.CollectionNames()
	if err != nil {
		return false, err
	}

	for _, shortName := range collections {
		if coll.Name == shortName {
			return true, nil
		}
	}
	return false, nil
}

/************ getting files in a directory *******/

func GetFilesInDir(dir string, excludeRegexes *[]string, crawlFurther bool) (*[]string, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(dir)
	if err != nil {
		if os.IsPermission(err) {
			fmt.Printf("Incorrect permissions for file %s\n", dir)
		}
		return nil, err
	}

	fileInfos, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		return nil, err
	}

	files := make([]string, 0)

	for _, fi := range fileInfos {
		absPath := dir + "/" + fi.Name()
		exclude, err := excludeFile(excludeRegexes, fi.Name())
		if err != nil {
			return nil, err
		}
		if exclude {
			continue
		}
		if fi.IsDir() && crawlFurther {
			subDirFiles, err := GetFilesInDir(absPath, excludeRegexes, false)
			if err != nil {
				return nil, err
			}
			files = append(files, *subDirFiles...)
		} else if !fi.IsDir() {
			files = append(files, absPath)
		}
	}
	return &files, nil
}

func excludeFile(exclude *[]string, fname string) (bool, error) {
	for _, excludeString := range *exclude {
		match, err := regexp.Match(excludeString, ([]byte)(fname))
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}
