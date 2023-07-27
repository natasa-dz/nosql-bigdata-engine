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
	"strings"
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

//IZMENI SUMMARY-dodaj da uzima 0 i n-ti element, ofset indexa i u summary ofset

func BuildSummary(entries []Data, generation int) *Summary {
	// Sort the entries by Key
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	// Create the filename for the Summary file
	filename := fmt.Sprintf("usertable-%d-Summary.txt", generation)

	// Create the Summary entries
	summaryEntries := make([]SummaryEntry, len(entries))
	var offset int64

	for i, entry := range entries {

		encodedKey := hex.EncodeToString([]byte(entry.Key))
		summaryEntries[i] = SummaryEntry{
			Key:    encodedKey,
			Offset: offset,
			Size:   int64(len(entry.Value)),
		}
		offset += int64(len(entry.Value))
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

func BuildIndex(entries []Data) *Index {
	// Sort the entries by Key
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	// Create the Index entries
	indexEntries := make([]IndexEntry, len(entries))
	for i, entry := range entries {
		encodedKey := hex.EncodeToString([]byte(entry.Key))
		indexEntries[i] = IndexEntry{
			Key:   encodedKey,
			Index: entry.Index,
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

func BuildFilter(sortedData []Data, expectedElements int, falsePositiveRate float64) *Bloom2 {
	bloom := &Bloom2{}
	bloom.InitializeBloom2(expectedElements, falsePositiveRate)

	// Add each Key to the Bloom Filter
	for _, data := range sortedData {
		bloom.Add([]byte(data.Key))
	}

	return bloom
}

// bottom-up izgradnja, pretpostavka da imamo key:value parove!!!!!!!!

func BuildMerkleTreeRoot(sortedData []Data) *Node {
	// Create leaf nodes for each data entry and hash them individually.
	var leafNodes []*Node
	for _, data := range sortedData {
		node := &Node{
			Data: []byte(data.Key + data.Value), // Concatenate Key and Value for simplicity
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

func BuildMetaData(dataMap map[string]Data, bloomFilter *Bloom2, sstableFileName string, generation int) (*Metadata, error) {

	// Convert the map to a slice of Data for sorting and other operations
	var dataSlice []Data
	for _, data := range dataMap {
		dataSlice = append(dataSlice, data)
	}

	// Sort the data
	sortedData := SortData1(dataSlice)

	// Build the Bloom Filter for the sorted data?
	for _, data := range sortedData {
		bloomFilter.Add([]byte(data.Key))
	}

	// Build the SSTable Index
	sstableIndex := BuildIndex(sortedData)

	// Build the Merkle Tree from the sorted data
	merkleRoot := BuildMerkleTreeRoot(sortedData)

	const SSTableVersion = "1.0"

	// Write the SSTable to disk and get the actual file size
	fileSize, err := WriteSSTableToDisk(sstableFileName, sortedData)
	if err != nil {
		return nil, err
	}

	// Create a Table of Contents entry for this SSTable
	tocEntry := TOCEntry{
		FileName:    sstableFileName,
		StartOffset: 0, // In this example, we assume that the SSTable starts at offset 0.
		EndOffset:   fileSize,
		MinKey:      []byte(sortedData[0].Key),
		MaxKey:      []byte(sortedData[len(sortedData)-1].Key),
	}

	// Create the Table of Contents
	toc := TOC{tocEntry}

	// Update the TOCEntry with the actual file size
	tocEntry.StartOffset = 0 // In this example, we assume that the SSTable starts at offset 0.
	tocEntry.EndOffset = fileSize

	// Create the Metadata object
	metadata := &Metadata{
		Version:      SSTableVersion,
		DataSummary:  BuildSummary(sortedData, generation), // Pass the generation here
		BloomFilter:  bloomFilter,
		SSTableIndex: sstableIndex,
		TOC:          toc,
		MerkleRoot:   merkleRoot,
	}

	return metadata, nil
}

func WriteSSTableToDisk(fileName string, sortedData []Data) (int64, error) {
	// Open the file for writing
	file, err := os.Create(fileName)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// Write the sorted data to the file in CSV format
	for _, data := range sortedData {
		// Use CSV format: "key,value\n"
		line := fmt.Sprintf("%s,%s\n", data.Key, data.Value)
		_, err := file.WriteString(line)
		if err != nil {
			return 0, err
		}
	}

	// Get the file size after writing the data
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, err
	}

	return fileInfo.Size(), nil
}

/////////////////// SORT DATA

/*
	func sortData():

otprilike ideja-memtable ti je input,
Data struct sadrzi podatke memTable-a,
vraca Key-Value vrednosti sortirane po kljucu spremne za upis
*/
type Data struct {
	Key   string
	Value string
	Index int64
}

func SortData1(entries []Data) []Data {
	// Sort the entries by Key
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	return entries
}

func SortData(memtable map[string]Data) []Data {
	// Create a slice to hold the data from the memtable
	dataSlice := make([]Data, 0, len(memtable))

	// Convert the map to a slice of Data entries
	for _, data := range memtable {
		dataSlice = append(dataSlice, data)
	}

	// Sort the Data slice by Key in ascending order
	sort.Slice(dataSlice, func(i, j int) bool {
		return dataSlice[i].Key < dataSlice[j].Key
	})

	// Assign incremental Index values to the sorted entries
	for i, entry := range dataSlice {
		entry.Index = int64(i)
		dataSlice[i] = entry
	}

	return dataSlice
}

///////////////////////////////// 3. stavka vezbi

// Function to save Data to disk with the specified naming format
func SaveDataToDisk(sortedData []Data, generation int) error {
	// Create the filename for the Data file
	filename := fmt.Sprintf("usertable-%d-Data.db", generation)

	// Open the file for writing
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the sorted data to the file in CSV format
	for _, data := range sortedData {
		// Use CSV format: "key,value\n"
		line := fmt.Sprintf("%s,%s\n", data.Key, data.Value)
		_, err := file.WriteString(line)
		if err != nil {
			return err
		}
	}

	return nil
}

// Function to save Index to disk with the specified naming format
func SaveIndexToDisk(index *Index, generation int) error {
	// Create the filename for the Index file
	filename := fmt.Sprintf("usertable-%d-Index.db", generation)

	// Open the file for writing
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the Index entries to the file
	for _, entry := range index.Entries {
		line := fmt.Sprintf("%s,%d\n", entry.Key, entry.Index)
		_, err := file.WriteString(line)
		if err != nil {
			return err
		}
	}

	return nil
}

// Function to save Summary to disk with the specified naming format
func SaveSummaryToDisk(summary *Summary, generation int) error {
	// Create the filename for the Summary file
	filename := fmt.Sprintf("usertable-%d-Summary.txt", generation)

	// Open the file for writing
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the Summary entries to the file
	file.WriteString("Key\tOffset\tSize\n")
	for _, entry := range summary.Entries {
		line := fmt.Sprintf("%s\t%d\t%d\n", entry.Key, entry.Offset, entry.Size)
		_, err := file.WriteString(line)
		if err != nil {
			return err
		}
	}

	return nil
}

// Function to save BloomFilter to disk with the specified naming format
func SaveBloomFilterToDisk(bloomFilter *Bloom2, generation int) error {
	// Create the filename for the BloomFilter file
	filename := fmt.Sprintf("usertable-%d-Filter.txt", generation)

	// Open the file for writing
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Serialize the BloomFilter contents to a string
	serializedBloom := serializeBloomFilter(bloomFilter)

	// Write the BloomFilter contents to the file
	_, err = file.WriteString(serializedBloom)
	if err != nil {
		return err
	}

	return nil
}

// Helper function to serialize the BloomFilter contents to a string
func serializeBloomFilter(bloomFilter *Bloom2) string {
	var serializedStrings []string

	// Get the bit slices from the BloomFilter
	bitSlices := bloomFilter.BitSlices

	// Convert each byte of the bit slices to a hexadecimal string
	for _, bitSlice := range bitSlices {
		hexString := hex.EncodeToString([]byte{bitSlice})
		serializedStrings = append(serializedStrings, hexString)
	}

	// Join the hexadecimal strings with commas
	serializedBloom := strings.Join(serializedStrings, ",")

	return serializedBloom
}

// Function to save TOC to disk with the specified naming format
func SaveTOCToDisk(toc TOC, generation int) error {
	// Create the filename for the TOC file
	filename := fmt.Sprintf("usertable-%d-TOC.txt", generation)

	// Open the file for writing
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the TOC entries to the file
	for _, entry := range toc {
		line := fmt.Sprintf("%s %d %d %s %s\n", entry.FileName, entry.StartOffset, entry.EndOffset,
			hex.EncodeToString(entry.MinKey), hex.EncodeToString(entry.MaxKey))
		_, err := file.WriteString(line)
		if err != nil {
			return err
		}
	}

	return nil
}

// Function to save Metadata to disk with the specified naming format
func SaveMetadataToDisk(metadata *Metadata, generation int) error {
	// Create the filename for the Metadata file
	filename := fmt.Sprintf("usertable-%d-Metadata.txt", generation)

	// Open the file for writing
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the Version to the file
	_, err = file.WriteString(metadata.Version + "\n")
	if err != nil {
		return err
	}

	// Write the DataSummary entries to the file
	for _, entry := range metadata.DataSummary.Entries {
		line := fmt.Sprintf("%s %d %d\n", entry.Key, entry.Offset, entry.Size)
		_, err := file.WriteString(line)
		if err != nil {
			return err
		}
	}

	// Write the BloomFilter contents to the file
	for _, bit := range metadata.BloomFilter.BitSlices {
		_, err := file.WriteString(fmt.Sprintf("%d ", bit))
		if err != nil {
			return err
		}
	}
	_, err = file.WriteString("\n")
	if err != nil {
		return err
	}

	// Write the SSTableIndex entries to the file
	for _, entry := range metadata.SSTableIndex.Entries {
		line := fmt.Sprintf("%s %d\n", entry.Key, entry.Index)
		_, err := file.WriteString(line)
		if err != nil {
			return err
		}
	}

	// Write the TOC entries to the file
	for _, entry := range metadata.TOC {
		line := fmt.Sprintf("%s %d %d %s %s\n", entry.FileName, entry.StartOffset, entry.EndOffset,
			hex.EncodeToString(entry.MinKey), hex.EncodeToString(entry.MaxKey))
		_, err := file.WriteString(line)
		if err != nil {
			return err
		}
	}

	// Serialize and write the MerkleRoot hash to the file as a hexadecimal string
	merkleRootHash := serializeMerkleTree(metadata.MerkleRoot)
	_, err = file.WriteString(hex.EncodeToString(merkleRootHash) + "\n")
	if err != nil {
		return err
	}

	return nil
}

// Function to serialize the Merkle tree and return its hash
func serializeMerkleTree(root *Node) []byte {
	if root == nil {
		return nil
	}

	if root.Left == nil && root.Right == nil {
		return root.Data // Leaf node, return its hash (data)
	}

	// Recursively hash the left and right subtrees
	leftHash := serializeMerkleTree(root.Left)
	rightHash := serializeMerkleTree(root.Right)

	// Combine the hashes and return the hash of the combined data
	combinedData := append(leftHash, rightHash...)
	return Hash(combinedData) // Assuming you have a function Hash(data []byte) []byte to compute the hash
}
