package main

import (
	"NAiSP/BloomFilter"
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
		Key:       []byte("key5"),
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
	SortData(logs)
	for i := 0; i < len(logs); i++ {
		fmt.Println(string(logs[i].Key))
	}

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
	header, _ := ReadHeader(file)

	bloom := BloomFilter.ReadBloom(file, int64(header.BloomOffset))
	fmt.Println(bloom.BitSlices)
	fmt.Println(bloom.M)
	fmt.Println(bloom.K)
	fmt.Println(int64(header.IndexOffset))
	fmt.Println(int64(header.SummaryOffset))
	summary, _ := ReadSummary(file, int64(header.SummaryOffset))

	fmt.Println(summary.StartKey)
	fmt.Println(summary.EndKey)
	fmt.Println(summary.Entries[0].Key)
	//TODO:istestirati index!!
	defer file.Close()
	file, err = os.OpenFile("singleTest.db", os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	fmt.Println("File content deleted.")
}
