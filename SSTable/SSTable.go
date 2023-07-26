package SSTable

/////////////////////////// OKVIRNA IMPLEMENTACIJA SSTABLE-A

import (
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

type SummaryEntry struct {
	Key    string
	Offset int64
	Size   int64
}

type IndexEntry struct {
	Key   string
	Index int64
}

func buildIndex(entries []Data, generation int) {
	// Sort the entries by key
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	// Create the filename for the Index file
	filename := fmt.Sprintf("usertable-%d-Index.txt", generation)

	// Create the Index entries
	indexEntries := make([]IndexEntry, len(entries))
	for i, entry := range entries {
		encodedKey := hex.EncodeToString([]byte(entry.key))
		indexEntries[i] = IndexEntry{
			Key:   encodedKey,
			Index: entry.index,
		}
	}

	// Create the contents of the Index file
	indexContents := "Key\tIndex\n"
	for _, entry := range indexEntries {
		indexContents += fmt.Sprintf("%s\t%d\n", entry.Key, entry.Index)
	}

	// Write the contents to the Index file
	err := ioutil.WriteFile(filename, []byte(indexContents), 0644)
	if err != nil {
		fmt.Println("Error writing Index file:", err)
		return
	}

	fmt.Println("Index file created:", filename)
}

func buildSummary(entries []Data, generation int) {
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
		return
	}

	fmt.Println("Summary file created:", filename)
}

/* func buildFilter():
funkcija uzima ocekivane elemente, br. ocek. el. i rate, dodaje el. u bloom i kreira bloom filter*/

func buildFilter(sortedData []Data, expectedElements int, falsePositiveRate float64) *Bloom2 {
	bloom := &Bloom2{}
	bloom.InitializeBloom2(expectedElements, falsePositiveRate)

	// Add each key to the Bloom Filter
	for _, data := range sortedData {
		bloom.add([]byte(data.key))
	}

	return bloom
}

func buildMerkleTreeRoot(sortedData []Data) *Node {
	// Create leaf nodes for each data entry and hash them individually.
	var leafNodes []*Node
	for _, data := range sortedData {
		node := &Node{
			data: []byte(data.key + data.value), // Concatenate key and value for simplicity
		}
		leafNodes = append(leafNodes, node)
	}

	// Build the Merkle tree by combining and hashing pairs of nodes.
	for len(leafNodes) > 1 {
		var newLevel []*Node

		for i := 0; i < len(leafNodes); i += 2 {
			if i+1 < len(leafNodes) {
				newNode := &Node{
					data:  Hash(append(leafNodes[i].data, leafNodes[i+1].data...)),
					left:  leafNodes[i],
					right: leafNodes[i+1],
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

func buildMetaData(dataSlice []Data, bloomFilter *Bloom2, indexInfo *Index, sstableFileName string) *Metadata {

	// Sort the data
	sortedData := sortData(dataSlice)

	// Build the Bloom Filter for the sorted data
	for _, data := range sortedData {
		bloomFilter.add(data.key)
	}

	// Build the SSTable Index
	sstableIndex := buildIndex(sortedData)

	// Create a Table of Contents entry for this SSTable
	tocEntry := TOCEntry{
		FileName:    sstableFileName,
		StartOffset: 0, // calculate this based on the file size once you write the SSTable to disk.
		EndOffset:   0, // same, nakon upisa racunas based on the file size
		MinKey:      sortedData[0].key,
		MaxKey:      sortedData[len(sortedData)-1].key,
	}

	// Create the Table of Contents
	toc := TOC{tocEntry}

	// Build the Merkle Tree from the sorted data
	merkleRoot := buildMerkleTreeRoot(sortedData)

	// Create the Metadata object
	metadata := &Metadata{
		Version:      SSTableVersion,
		DataSummary:  buildSummary(sortedData),
		BloomFilter:  bloomFilter.GetInfo(),
		SSTableIndex: indexInfo.GetInfo(),
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
