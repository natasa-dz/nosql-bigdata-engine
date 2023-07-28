package main

import (
	. "NAiSP/Log"
	. "NAiSP/SSTable"
	"fmt"
)

func main() {
	// Test data for logs (assuming you have Log struct defined)
	log1 := &Log{
		CRC:       123,
		Timestamp: 1626723625,
		Tombstone: false,
		KeySize:   4,
		ValueSize: 6,
		Key:       []byte("key1"),
		Value:     []byte("value1"),
	}

	log2 := &Log{
		CRC:       456,
		Timestamp: 1626723626,
		Tombstone: false,
		KeySize:   4,
		ValueSize: 6,
		Key:       []byte("key2"),
		Value:     []byte("value2"),
	}

	log3 := &Log{
		CRC:       789,
		Timestamp: 1626723627,
		Tombstone: false,
		KeySize:   4,
		ValueSize: 6,
		Key:       []byte("key3"),
		Value:     []byte("value3"),
	}

	logs := []*Log{log1, log2, log3}

	// Call writeToMultipleFiles function
	err := WriteToMultipleFiles(logs, "test_file")
	if err != nil {
		fmt.Println("Error writing to multiple files:", err)
		return
	}

	fmt.Println("Data written to multiple files successfully!")
}
