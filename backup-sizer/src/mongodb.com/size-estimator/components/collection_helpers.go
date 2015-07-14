package components

import (
	"gopkg.in/mgo.v2"
	"os"
	"fmt"
	"path/filepath"
	"time"
	"gopkg.in/mgo.v2/bson"
)

type BackupSizingOpts struct {
	Host string
	Port int
	SleepTime time.Duration
	NumIter int
	Uri string
	HashDir string
	FalsePosRate float64
	NumCPUs int
}

func (opts BackupSizingOpts) GetSession() (*mgo.Session)  {
	session, err := mgo.Dial(opts.Uri)
	if err != nil {
		fmt.Printf("Failed to dial MongoDB on port %v. Err %v\n", opts.Uri, err)
		os.Exit(1)
	}
	return session
}

func (opts BackupSizingOpts) GetDBPath() (string, error) {
	session := opts.GetSession()
	defer session.Close()

	return GetDbPath(session)
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

// STOLEN FROM mms-backup components
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

func getFilesInDir(dir string, crawlFurther bool) ([]string, error) {
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
		if fi.IsDir() && crawlFurther {
			subDirFiles, err := getFilesInDir(absPath, false)
			if err != nil {
				return nil, err
			}
			files = append(files, subDirFiles...)
		} else if !fi.IsDir(){
			files = append(files, absPath)
		}
	}
	return files, nil
}
