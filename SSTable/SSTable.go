package SSTable

/////////////////////////// OKVIRNA IMPLEMENTACIJA SSTABLE-A

import (
	. "NAiSP/BloomFilter"
	. "NAiSP/merkleTree"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)

type SSTable struct {
	Generation int
	Data       []Data
	Index      map[string]int64
	Summary    map[string]int
	Filter     Bloom2
	TOC        TOCEntry
	Metadata   MerkleRoot
}

///////////////////////// SUMMARY

type SummaryEntry struct {
	Key    string
	Offset int64
	Size   int64
}

type Summary struct {
	Entries []SummaryEntry
}

func buildSummary(entries []Data, generation int) *Summary {
	// Sort the entries by key
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	// Create the filename for the Summary file
	filename := fmt.Sprintf("usertable-%d-Summary.txt", generation)

	// Create the Summary entries
	summaryEntries := make([]SummaryEntry, len(entries))
	var offset int64
	for i, entry := range entries {
		encodedKey := hex.EncodeToString([]byte(entry.key))
		summaryEntries[i] = SummaryEntry{
			Key:    encodedKey,
			Offset: offset,
			Size:   int64(len(entry.value)),
		}
		offset += int64(len(entry.value))
	}

	// Create the contents of the Summary file
	summaryContents := "Key\tOffset\tSize\n"
	for _, entry := range summaryEntries {
		summaryContents += fmt.Sprintf("%s\t%d\t%d\n", entry.Key, entry.Offset, entry.Size)
	}

	// Write the contents to the Summary file
	err := ioutil.WriteFile(filename, []byte(summaryContents), 0644)
	if err != nil {
		fmt.Println("Error writing Summary file:", err)
		return nil // Return nil in case of an error
	}

	fmt.Println("Summary file created:", filename)

	// Create and return the Summary object
	return &Summary{
		Entries: summaryEntries,
	}
}

///////////////////////// INDEX

type IndexEntry struct {
	Key   string
	Index int64
}

type Index struct {
	Entries []IndexEntry
}

func buildIndex(entries []Data) *Index {
	// Sort the entries by key
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	// Create the Index entries
	indexEntries := make([]IndexEntry, len(entries))
	for i, entry := range entries {
		encodedKey := hex.EncodeToString([]byte(entry.key))
		indexEntries[i] = IndexEntry{
			Key:   encodedKey,
			Index: entry.index,
		}
	}

	// Create the Index object
	index := &Index{
		Entries: indexEntries,
	}

	return index
}

/* func buildFilter():
funkcija uzima ocekivane elemente, br. ocek. el. i rate, dodaje el. u bloom i kreira bloom filter*/

func buildFilter(sortedData []Data, expectedElements int, falsePositiveRate float64) *Bloom2 {
	bloom := &Bloom2{}
	bloom.InitializeBloom2(expectedElements, falsePositiveRate)

	// Add each key to the Bloom Filter
	for _, data := range sortedData {
		bloom.Add([]byte(data.key))
	}

	return bloom
}

func buildMerkleTreeRoot(sortedData []Data) *Node {
	// Create leaf nodes for each data entry and hash them individually.
	var leafNodes []*Node
	for _, data := range sortedData {
		node := &Node{
			Data: []byte(data.key + data.value), // Concatenate key and value for simplicity
		}
		leafNodes = append(leafNodes, node)
	}

	// Build the Merkle tree by combining and hashing pairs of nodes.
	for len(leafNodes) > 1 {
		var newLevel []*Node

		for i := 0; i < len(leafNodes); i += 2 {
			if i+1 < len(leafNodes) {
				newNode := &Node{
					Data:  Hash(append(leafNodes[i].Data, leafNodes[i+1].Data...)),
					Left:  leafNodes[i],
					Right: leafNodes[i+1],
				}
				newLevel = append(newLevel, newNode)
			} else {
				// If there's an odd number of nodes, simply add the last node to the new level.
				newLevel = append(newLevel, leafNodes[i])
			}
		}

		leafNodes = newLevel
	}

	// The last remaining node is the root of the Merkle tree.
	return leafNodes[0]
}

///////////////////// META DATA

/*func buildMetaData():
otprilike ideja- sortiraj-sortData(), kreiraj TOC, kreiraj MerkleTree-buildMerkleTreeRoot-vraca hash root-a,
*/

type TOCEntry struct {
	FileName    string
	StartOffset int64
	EndOffset   int64
	MinKey      []byte
	MaxKey      []byte
}

type TOC []TOCEntry

type Metadata struct {
	Version      string
	DataSummary  *Summary
	BloomFilter  *Bloom2
	SSTableIndex *Index
	TOC          TOC
	MerkleRoot   *Node
}

func buildMetaData(dataMap map[string]Data, bloomFilter *Bloom2, sstableFileName string, generation int) *Metadata {
	// Convert the map to a slice of Data for sorting and other operations
	var dataSlice []Data
	for _, data := range dataMap {
		dataSlice = append(dataSlice, data)
	}

	// Sort the data
	sortedData := sortData1(dataSlice)

	// Build the Bloom Filter for the sorted data
	for _, data := range sortedData {
		bloomFilter.Add([]byte(data.key))
	}

	// Build the SSTable Index
	sstableIndex := buildIndex(sortedData)

	// Create a Table of Contents entry for this SSTable
	tocEntry := TOCEntry{
		FileName:    sstableFileName,
		StartOffset: 0,                                         // calculate this based on the file size once you write the SSTable to disk.
		EndOffset:   0,                                         // same, nakon upisa racunas based on the file size
		MinKey:      []byte(sortedData[0].key),                 // Convert string to []byte
		MaxKey:      []byte(sortedData[len(sortedData)-1].key), // Convert string to []byte
	}

	// Create the Table of Contents
	toc := TOC{tocEntry}

	// Build the Merkle Tree from the sorted data
	merkleRoot := buildMerkleTreeRoot(sortedData)
	const SSTableVersion = "1.0"

	// Create the Metadata object
	metadata := &Metadata{
		Version:      SSTableVersion,
		DataSummary:  buildSummary(sortedData, generation), // Pass the generation here
		BloomFilter:  bloomFilter,
		SSTableIndex: sstableIndex,
		TOC:          toc,
		MerkleRoot:   merkleRoot,
	}

	// Write the SSTable to disk and get the actual file size
	// This function should write the SSTable data to disk and return the file size
	fileSize := writeSSTableToDisk(sstableFileName, sortedData)

	// Update the TOCEntry with the actual file size
	tocEntry.StartOffset = 0 // Set the correct start offset based on the SSTable file's position.
	tocEntry.EndOffset = fileSize

	return metadata
}

func writeSSTableToDisk(fileName string, sortedData []Data) int64 {
	// Open the file for writing
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return 0
	}
	defer file.Close()

	// Write the sorted data to the file
	for _, data := range sortedData {
		// Assuming each data entry is a simple key-value pair
		// You may need to adjust this based on your actual data structure
		line := fmt.Sprintf("%s,%s\n", data.key, data.value)
		_, err := file.WriteString(line)
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return 0
		}
	}

	// Get the file size after writing the data
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file info:", err)
		return 0
	}

	return fileInfo.Size()
}

/////////////////// SORT DATA

/*
	func sortData():

otprilike ideja-memtable ti je input,
Data struct sadrzi podatke memTable-a,
vraca key-value vrednosti sortirane po kljucu spremne za upis
*/
type Data struct {
	key   string
	value string
	index int64
}

func sortData1(entries []Data) []Data {
	// Sort the entries by key
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	return entries
}

func sortData(memtable map[string]Data) []Data {
	// Create a slice to hold the data from the memtable
	dataSlice := make([]Data, 0, len(memtable))

	// Convert the map to a slice of Data entries
	for _, data := range memtable {
		dataSlice = append(dataSlice, data)
	}

	// Sort the Data slice by key in ascending order
	sort.Slice(dataSlice, func(i, j int) bool {
		return dataSlice[i].key < dataSlice[j].key
	})

	// Assign incremental index values to the sorted entries
	for i, entry := range dataSlice {
		entry.index = int64(i)
		dataSlice[i] = entry
	}

	return dataSlice
}

///////////////////////////////// DEBUG
