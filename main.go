package main

import (
	application "NAiSP/Application"
	. "NAiSP/Log"
	menu "NAiSP/Menu"
	. "NAiSP/SSTable"
	test "NAiSP/Testing"
	"sort"
)

func SortData(entries []*Log) []*Log {
	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].Key) < string(entries[j].Key)
	})
	return entries
}
func main() {
	PrintLogs("single", "1", "2")
	choiceOfConfig := menu.WriteAppInitializationMenu()
	app := application.InitializeApp(choiceOfConfig)
	test.InitializeData(app)
	app.StartApp()
	//----------------------------------------------------------------------------
	//========================SSTABLE TESTS=======================================
	// Test data for logs (assuming you have Log struct defined)
	log1 := &Log{
		CRC:       123,
		Timestamp: 1159721699,
		Tombstone: true,
		KeySize:   4,
		ValueSize: 6,
		Key:       []byte("key4"),
		Value:     []byte("value5"),
	}
	log2 := &Log{
		CRC:       456,
		Timestamp: 1159721698,
		Tombstone: false,
		KeySize:   4,
		ValueSize: 6,
		Key:       []byte("key5"),
		Value:     []byte("value9"),
	}

	log3 := &Log{
		CRC:       789,
		Timestamp: 1159721698,
		Tombstone: false,
		KeySize:   4,
		ValueSize: 6,
		Key:       []byte("key6"),
		Value:     []byte("value6"),
	}
	log4 := &Log{
		CRC:       789,
		Timestamp: 1159721698,
		Tombstone: false,
		KeySize:   4,
		ValueSize: 6,
		Key:       []byte("key7"),
		Value:     []byte("value1"),
	}

	logs := []*Log{log1, log2, log3, log4}
	SortData(logs)
	//BuildSSTableMultiple(logs, 2, 1, 3)
	/*err := SSTable.BuildSSTableSingle(logs, 1, 2, 3)
	if err != nil {
		fmt.Println("Error writing to a single file:", err)
		return
	}*/
}
