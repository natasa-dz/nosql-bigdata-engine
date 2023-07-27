package main

import (
	"NAiSP/BloomFilter"
	"NAiSP/SSTable"
	"fmt"
)

func main() {
	// Create a new Bloom Filter with expected elements and false positive rate
	bloomFilter := &BloomFilter.Bloom2{}    // Use &Bloom2{} to create a new instance
	bloomFilter.InitializeBloom2(100, 0.01) // Use Initialize() instead of InitializeBloom2()

	dataMap := map[string]SSTable.Data{
		"key3": SSTable.Data{Key: "key3", Value: "value3"}, // Use Key and Value instead of key and value
		"key1": SSTable.Data{Key: "key1", Value: "value1"},
		"key2": SSTable.Data{Key: "key2", Value: "value2"},
	}

	// Generate SSTable metadata and sorted data
	sstableFileName := "usertable-1.sst"
	generation := 1
	metadata, err := SSTable.BuildMetaData(dataMap, bloomFilter, sstableFileName, generation)
	if err != nil {
		fmt.Println("Error creating metadata:", err)
		return
	}

	// Print the metadata
	fmt.Println("SSTable Metadata:")
	fmt.Println("Version:", metadata.Version)
	fmt.Println("DataSummary:")
	for _, entry := range metadata.DataSummary.Entries {
		fmt.Printf("Key: %s, Offset: %d, Size: %d\n", entry.Key, entry.Offset, entry.Size) // Use Key and Offset instead of key and offset
	}
	fmt.Println("BloomFilter:", metadata.BloomFilter)
	fmt.Println("SSTableIndex:")
	for _, entry := range metadata.SSTableIndex.Entries {
		fmt.Printf("Key: %s, Index: %d\n", entry.Key, entry.Index) // Use Key instead of key
	}
	fmt.Println("TOC:")
	fmt.Printf("FileName: %s, StartOffset: %d, EndOffset: %d, MinKey: %x, MaxKey: %x\n", metadata.TOC[0].FileName, metadata.TOC[0].StartOffset, metadata.TOC[0].EndOffset, metadata.TOC[0].MinKey, metadata.TOC[0].MaxKey)
	fmt.Println("MerkleRoot:", metadata.MerkleRoot)
}
