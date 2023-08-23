package main

import (
	application "NAiSP/Application"
	. "NAiSP/Log"
	menu "NAiSP/Menu"
	"sort"
)

func SortData(entries []*Log) []*Log {
	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].Key) < string(entries[j].Key)
	})
	return entries
}
func main() {
	choiceOfConfig := menu.WriteAppInitializationMenu()
	app := application.InitializeApp(choiceOfConfig)
	app.StartApp()
	//============================MENU TESTS======================
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
	//l12 := Log{Key: []byte("key12"), Value: []byte("val")}
	//l13 := Log{Key: []byte("key13"), Value: []byte("val")}
	//
	//slice := []*Log{&l1, &l2, &l3, &l4, &l5, &l6, &l7, &l8, &l9, &l10, &l11, &l12, &l13}
	//LIST_RANGESCAN_PaginationResponse(slice, 4)
	//------------------------------------------------------------------------------
	//=======================BTREE TESTS==========================================
	//NOTE: these tests might not be valid anymore, cause search for duplicate is moved to memtable.go it is not
	//	in btree.go anymore so it might give errors when inserting duplicates
	//l1 := Log{Key: []byte("10"), Value: []byte("val10")}
	//l2 := Log{Key: []byte("20"), Value: []byte("val20")}
	//l3 := Log{Key: []byte("5"), Value: []byte("val5")}
	//l4 := Log{Key: []byte("6"), Value: []byte("val6")}
	//l5 := Log{Key: []byte("7"), Value: []byte("val7")}
	//l6 := Log{Key: []byte("12"), Value: []byte("val12")}
	//l7 := Log{Key: []byte("8"), Value: []byte("val8")}
	//l8 := Log{Key: []byte("30"), Value: []byte("val30")}
	//l9 := Log{Key: []byte("7"), Value: []byte("val1117")}
	//l10 := Log{Key: []byte("17"), Value: []byte("val17")}
	//var t Tree
	//t.Insert(l1)
	//t.Insert(l2)
	//t.Insert(l3)
	//t.Insert(l4)
	//t.Insert(l5)
	//t.Insert(l6)
	//t.Insert(l7)
	//t.Insert(l8)
	//t.Insert(l9)
	//t.Insert(l10)
	//----------------------------------------------------------------------------
	//========================SSTABLE TESTS=======================================
	// Test data for logs (assuming you have Log struct defined)
	/*log1 := &Log{
		CRC:       123,
		Timestamp: 1159721698,
		Tombstone: false,
		KeySize:   4,
		ValueSize: 6,
		Key:       []byte("key3"),
		Value:     []byte("value5"),
	}
	log2 := &Log{
		CRC:       456,
		Timestamp: 1495721699,
		Tombstone: false,
		KeySize:   4,
		ValueSize: 6,
		Key:       []byte("key8"),
		Value:     []byte("value9"),
	}

	log3 := &Log{
		CRC:       789,
		Timestamp: 1299721699,
		Tombstone: false,
		KeySize:   4,
		ValueSize: 6,
		Key:       []byte("key5"),
		Value:     []byte("value1"),
	}
	log4 := &Log{
		CRC:       789,
		Timestamp: 1229721699,
		Tombstone: false,
		KeySize:   4,
		ValueSize: 6,
		Key:       []byte("key4"),
		Value:     []byte("value1"),
	}

	logs := []*Log{log1, log2, log3, log4}
	SortData(logs)*/

	/*var level int
	var summaryBlockSIze int
	var levelT int
	var maxL int
	level = 2
	summaryBlockSIze = 3
	levelT = 2
	maxL = 4*/
	//LSM.SizeTieredCompactionSingle(&level, &sstableType, &summaryBlockSIze)
	//LSM.SizeTieredCompactionMultiple(&level, &summaryBlockSIze, &levelT, &maxL)
	// Call writeToMultipleFiles function
	//SSTable.BuildSSTableMultiple(logs, 2, 2, 3)

	/*file, err := os.Open("./Data/SSTables/Multiple/Bloom-2-1.bin")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	file2, err := os.Open("./Data/SSTables/Multiple/Data-2-1.bin")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	file3, err := os.Open("./Data/SSTables/Multiple/Index-2-1.bin")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	file4, err := os.Open("./Data/SSTables/Multiple/Summary-2-1.bin")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}

	//Logs test
	fmt.Println("LOGS TEST")
	var data []*Log
	offsetEnd, err := file2.Seek(0, os.SEEK_END)
	data, _ = ReadLogs(file2, 0, uint64(offsetEnd))
	for i := 0; i < len(data); i++ {
		fmt.Println(string(data[i].Key))
		fmt.Println(string(data[i].Value))
	}
	//Bloom test
	fmt.Println("BLOOM TEST")
	bloom := BloomFilter.ReadBloom(file, 0)
	fmt.Println(bloom.BitSlices)
	fmt.Println(bloom.M)
	fmt.Println(bloom.K)

	//Index test
	fmt.Println("INDEX TEST")
	offsetEnd, err = file3.Seek(0, os.SEEK_END)
	fmt.Println(offsetEnd)
	indexEntries, _ := ReadIndex(file3, 0, offsetEnd)

	for i := 0; i < len(indexEntries); i++ {
		fmt.Println(string(indexEntries[i].Key))
		fmt.Println(indexEntries[i].Offset)
	}

	//Summary test
	fmt.Println("SUMMARY TEST")
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
	/*err := SSTable.BuildSSTableSingle(logs, 1, 2, 3)
	if err != nil {
		fmt.Println("Error writing to a single file:", err)
		return
	}*/

	/*file, err := os.Open("./Data/SSTables/Single/Data-2-4.bin")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	//Logs test
	fmt.Println("LOGS TEST")
	var data []*Log
	header, _ := ReadHeader(file)
	fmt.Println(header.LogsOffset)
	fmt.Println(header.BloomOffset)
	fmt.Println(header.IndexOffset)
	fmt.Println(header.SummaryOffset)
	//offsetEnd, err := file.Seek(0, os.SEEK_END)
	data, _ = ReadLogs(file, int64(32), uint64(header.BloomOffset))

	for i := 0; i < len(data); i++ {
		fmt.Println(string(data[i].Key))
		fmt.Println(string(data[i].Value))
	}

	//Bloom test
	fmt.Println("BLOOM TEST")
	bloom := BloomFilter.ReadBloom(file, int64(header.BloomOffset))
	fmt.Println(bloom.BitSlices)
	fmt.Println(bloom.M)
	fmt.Println(bloom.K)

	//Summary test
	fmt.Println("SUMMARY TEST")
	summary, _ := ReadSummary(file, int64(header.SummaryOffset))

	fmt.Println(summary.StartKey)
	fmt.Println(summary.EndKey)
	for i := 0; i < len(summary.Entries); i++ {
		fmt.Println(string(summary.Entries[i].Key))
		fmt.Println(summary.Entries[i].Offset)
	}
	//Index test
	fmt.Println("INDEX TEST")
	//offsetEnd, err := file.Seek(0, os.SEEK_END)
	indexEntries, _ := ReadIndex(file, int64(header.IndexOffset), int64(header.SummaryOffset))

	for i := 0; i < len(indexEntries); i++ {
		fmt.Println(indexEntries[i].Key)
		fmt.Println(indexEntries[i].Offset)
	}

	defer file.Close()*/
}
