package main

import (
	"NAiSP/BloomFilter"
	. "NAiSP/Log"
	//. "NAiSP/Menu"
	. "NAiSP/SSTable"
	"fmt"
	"os"
)

func main() {
	//=======MENU TESTS======================
	//l1 := Log{Key: []byte("key1"), Value: []byte("val")}
	//l2 := Log{Key: []byte("key2"), Value: []byte("val")}
	//l3 := Log{Key: []byte("key3"), Value: []byte("val")}
	//l4 := Log{Key: []byte("key4"), Value: []byte("val")}
	//l5 := Log{Key: []byte("key5"), Value: []byte("val")}
	//l6 := Log{Key: []byte("key6"), Value: []byte("val")}
	//l7 := Log{Key: []byte("key7"), Value: []byte("val")}
	//l8 := Log{Key: []byte("key8"), Value: []byte("val")}
	//l9 := Log{Key: []byte("key9"), Value: []byte("val")}
	//l10 := Log{Key: []byte("key10"), Value: []byte("val")}
	//l11 := Log{Key: []byte("key11"), Value: []byte("val")}
	//
	//slice := []*Log{&l1, &l2, &l3, &l4, &l5, &l6, &l7, &l8, &l9, &l10, &l11}
	//LIST_RANGESCAN_PaginationResponse(slice)

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
	log4 := &Log{
		CRC:       789,
		Timestamp: 1626723627,
		Tombstone: false,
		KeySize:   4,
		ValueSize: 6,
		Key:       []byte("key4"),
		Value:     []byte("value4"),
	}

	logs := []*Log{log1, log2, log3, log4}
	SortData(logs)
	// Call writeToMultipleFiles function
	/*BuildSSTableMultiple(logs, 1, 1)

	fmt.Println("Data written to multiple files successfully!")
	file, err := os.Open("./Data/SSTables/Multiple/Bloom-1-1.bin")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	file2, err := os.Open("./Data/SSTables/Multiple/Data-1-1.bin")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	file3, err := os.Open("./Data/SSTables/Multiple/Index-1-1.bin")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	file4, err := os.Open("./Data/SSTables/Multiple/Summary-1-1.bin")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}

	//Logs test
	var data []*Log
	offsetEnd, err := file2.Seek(0, os.SEEK_END)
	data, _ = ReadLogs(file2, 0, uint64(offsetEnd))
	for i := 0; i < len(data); i++ {
		fmt.Println(string(data[i].Key))
	}
	//Bloom test
	bloom := BloomFilter.ReadBloom(file, 0)
	fmt.Println(bloom.BitSlices)
	fmt.Println(bloom.M)
	fmt.Println(bloom.K)

	//Index test
	offsetEnd, err = file3.Seek(0, os.SEEK_END)
	fmt.Println(offsetEnd)
	indexEntries, _ := ReadIndex(file3, 0, offsetEnd)

	for i := 0; i < len(indexEntries); i++ {
		fmt.Println(string(indexEntries[i].Key))
		fmt.Println(indexEntries[i].Offset)
	}

	//Summary test
	summary, _ := ReadSummary(file4, 0)

	fmt.Println(summary.StartKey)
	fmt.Println(summary.EndKey)
	for i := 0; i < len(summary.Entries); i++ {
		fmt.Println(string(summary.Entries[i].Key))
		fmt.Println(summary.Entries[i].Offset)
	}

	defer file.Close()
	defer file2.Close()
	defer file3.Close()
	defer file4.Close()*/
	// Call writeToSingleFile function
	err := WriteToSingleFile(logs, 1, 1)
	if err != nil {
		fmt.Println("Error writing to a single file:", err)
		return
	}
	file, err := os.Open("./Data/SSTables/Single/Data-1-1.bin")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	//Logs test
	var data []*Log
	header, _ := ReadHeader(file)
	data, _ = ReadLogs(file, int64(header.LogsOffset), header.BloomOffset)
	fmt.Println(data[0].Timestamp)
	for i := 0; i < len(data); i++ {
		fmt.Println(string(data[i].Key))
	}
	//header, _ := ReadHeader(file)
	//Bloom test
	bloom := BloomFilter.ReadBloom(file, int64(header.BloomOffset))
	fmt.Println(bloom.BitSlices)
	fmt.Println(bloom.M)
	fmt.Println(bloom.K)
	fmt.Println(int64(header.IndexOffset))
	fmt.Println(int64(header.SummaryOffset))

	//Summary test
	summary, _ := ReadSummary(file, int64(header.SummaryOffset))

	fmt.Println(summary.StartKey)
	fmt.Println(summary.EndKey)
	for i := 0; i < len(summary.Entries); i++ {
		fmt.Println(string(summary.Entries[i].Key))
		fmt.Println(summary.Entries[i].Offset)
	}
	//Index test
	indexEntries, _ := ReadIndex(file, int64(header.IndexOffset), int64(header.SummaryOffset))

	for i := 0; i < len(indexEntries); i++ {
		fmt.Println(string(indexEntries[i].Key))
		fmt.Println(indexEntries[i].Offset)
	}

	defer file.Close()
	//Delete file
	/*file, err = os.OpenFile("singleTest.db", os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()*/

}
