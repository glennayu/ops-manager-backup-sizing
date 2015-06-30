package components

import (
	"testing"
)

func TestWritingBlockHashes(test *testing.T) {
	_, err := GetBlockHashes(0)
	if err != nil {
		test.Errorf("failed. Err: %v", err)
	}

	_, err = GetBlockHashes(1)
	if err != nil {
		test.Errorf("Failed on iteration2. Err:%v", err)
	}
}