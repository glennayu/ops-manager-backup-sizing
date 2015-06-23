package main

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"os"
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
		fmt.Printf("Failed to dial MongoDB on port %v. Err %v", port, err)
		os.Exit(1)
	}

	fmt.Printf("Successfully pinged %s\n", uri);
}
