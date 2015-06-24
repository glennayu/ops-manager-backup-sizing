package main

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"os"
	"ops-manager-backup-sizing/backup-sizer/src/mongodb.com/size-estimator/components"
)


func main() {
	port := os.Args[1]
	uri := fmt.Sprintf("localhost:%s", port)

	session, err := mgo.Dial(uri)
	if err != nil {
		fmt.Printf("Failed to dial MongoDB on port %v. Err %v", port, err)
		os.Exit(1)
	}
	defer session.Close()

	err = session.Ping();
	if err != nil {

		fmt.Printf("Failed to contact server on %s. Err %v", uri, err)
		os.Exit(1)
	}

	fmt.Printf("Successfully pinged %s\n", uri);

	oplogStats, err := components.GetOplogStats(uri, session)
	if err != nil {
		fmt.Printf("Failed to get oplog stats on server %s. Err: %v", uri, err)
		os.Exit(1)
	}
	fmt.Printf("OplogStats: %v", oplogStats)

}
