package components

import (
	"fmt"
	"os"
	"bytes"
)

const TestDataDir = "./test_data"
const empty_dir = "./test_data/emptydir"

func generateTestData() error {
	testpath := TestDataDir

	fmt.Println(testpath)
	err := os.Mkdir(testpath, 0777)
	if err != nil {
		return err
	}
	err = os.Chdir(testpath)
	if err != nil {
		return err
	}

	// an empty directory
	err = os.Mkdir("emptydir", 0777)
	if err != nil {
		return err
	}



	// create an empty file
	_, err = os.Create("empty.test")
	if err != nil {
		return err
	}

	f, err := os.Create("oneblock.test")
	if err != nil {
		return err
	}
	writeBytes(f, blockSizeBytes)

	f, err = os.Create("fiveblocks.test")
	if err != nil {
		return err
	}
	writeBytes(f, 5*blockSizeBytes)

	f, err = os.Create("partialblock.test")
	if err != nil {
		return err
	}
	writeBytes(f, 5*blockSizeBytes + 1024)

	err = os.Mkdir("subdir", 0777)
	if err != nil {
		return err
	}
	os.Chdir("subdir")
	f, err = os.Create("subdir.test")
	if err != nil {
		return err
	}
	writeBytes(f, 5*blockSizeBytes)


	return nil
}


func writeBytes(f *os.File, numBytes uint64) {
	var bytesGenerated uint64 = 0
	var bytesVal []byte

	for bytesGenerated < numBytes {
		bytesVal = bytes.Repeat([]byte{'a'}, 1024)
		bytesGenerated += 1024
		f.Write(bytesVal)
	}
}
