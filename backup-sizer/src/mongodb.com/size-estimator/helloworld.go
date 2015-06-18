package main

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"os"
)


func main() {
	port := os.Args[1]
	fmt.Println(os.Args)
	uri := fmt.Sprintf("localhost:%s", port)
	session, err := mgo.Dial(uri)

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	defer session.Close()

	err = session.Ping();

	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}

	fmt.Printf("Successfully pinged %s\n", uri);
}
