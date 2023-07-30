package main

import (
	. "NAiSP/Log"
	. "NAiSP/SSTable"
	"fmt"
	"os"
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

	// Call writeToSingleFile function
	err := WriteToSingleFile(logs, "singleTest")
	if err != nil {
		fmt.Println("Error writing to a single file:", err)
		return
	}

	fmt.Println("Data written to a single file successfully!")

	/*	// Call writeToMultipleFiles function
		err := WriteToMultipleFiles(logs, 1, "test")
		if err != nil {
			fmt.Println("Error writing to multiple files:", err)
			return
		}

		fmt.Println("Data written to multiple files successfully!")*/
	file, err := os.Open("singleTest.db")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	// Ensure the file is closed when the function returns

	// Read binary data from the file into an integer slice
	var data []*Log

	data, _ = ReadLogs(file)
	fmt.Println(data[0].Timestamp)
	defer file.Close()
}
