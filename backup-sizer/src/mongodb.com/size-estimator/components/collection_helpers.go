package components

import (
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2"
	"github.com/mongodb/slogger/v1/slogger"
	"strings"
)


type NamespaceInfo struct {
	Name    string      `bson:"name"`
	Options CollOptions `bson:"options"`

}

type CollOptions struct {
	Create string `bson:"create"`
	Capped bool   `bson:"capped"`
	Size   int64  `bson:"size"`
	Max    int64  `bson:"max"`
	Other  bson.M `bson:",inline"`
}


func isCapped(coll *mgo.Collection) (bool, int64, error) {
	options, err := getCollOptions(coll)
	if err != nil {
		return false, -1, err
	}
	return options.Capped, options.Size, nil
}

func getCollOptions(coll *mgo.Collection) (*CollOptions, error) {
	// The `name` field on `listCollections` omits the database part
	// and is just `<collection>`.
	cmd := bson.D{
		{"listCollections", 1},
		{"filter", bson.M{"name": coll.Name}},
	}

	var cmdResult struct {
		// 2.8.0-rc4+
		Cursor struct {
				   FirstBatch []NamespaceInfo "firstBatch"
			   } "cursor"
		// 2.7 -> 2.8.0rc3
		Collections []NamespaceInfo "collections"
	}

	err := coll.Database.Run(cmd, &cmdResult)
	var info []NamespaceInfo
	switch {
	case len(cmdResult.Collections) > 0:
		info = cmdResult.Collections
	case len(cmdResult.Cursor.FirstBatch) > 0:
		info = cmdResult.Cursor.FirstBatch
	}

	switch {
	case err == nil && len(info) == 0:
		return nil, slogger.NewStackError("Collection not found. Collection: `%v`",
			coll.FullName)
	case err == nil && len(info) > 0:
		return &info[0].Options, nil
		case isNoCmd(err):
			return getCollOptionsPre28(coll)
	default:
		return nil, slogger.NewStackError("Error running `listCollections`. Collection: `%v` Err: %v",
			coll.FullName, err)
	}
}

func getCollOptionsPre28(coll *mgo.Collection) (*CollOptions, error) {
	// The `name` field in the `system.namespaces` query returns
	// `<database>.<collection>`
	var namespaceInfo NamespaceInfo
	err := coll.Database.C("system.namespaces").Find(bson.M{"name": coll.FullName}).One(&namespaceInfo)
	switch err {
	case nil:
		return &namespaceInfo.Options, nil
	case mgo.ErrNotFound:
		return nil, slogger.NewStackError("Could not query `system.namespaces`. Collection: `%v`",
			coll.FullName)
	default:
		return nil, slogger.NewStackError("Error querying `system.namespaces`. Collection: `%v` Err: %v",
			coll.FullName, err)
	}
}

func isNoCmd(err error) bool {
	e, ok := err.(*mgo.QueryError)
	return ok && strings.HasPrefix(e.Message, "no such cmd:")
}

func getMongodVersion(coll *mgo.Collection) (string, error) {
	cmd := bson.D{
		{"buildInfo", 1},
	}
	var result (bson.M)

	session := coll.Database.Session
	if err := session.DB("admin").Run(cmd, &result); err != nil {
		return "", err
	}

	return result["version"].(string), nil

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
