package SSTable

/////////////////////////// OKVIRNA IMPLEMENTACIJA SSTABLE-A

import (
	. "NAiSP/BloomFilter"
	. "NAiSP/Log"
	. "NAiSP/merkleTree"
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)

type SSTable struct {
	Generation int
	Data       []Log
	Index      Index
	Summary    Summary
	Filter     Bloom2
	//	TOC        TOCEntry
	Metadata MerkleRoot
}

func BuildSSTable(sortedData []Log, generation int) {}

func BuildDataFile(generation int, sortedData []Log) {
	var DataContent []byte
	for _, data := range sortedData {
		DataContent = append(DataContent, data.Serialize()...)
	}
	err := ioutil.WriteFile("Data-Gen-"+strconv.Itoa(generation), DataContent, 0644)
	if err != nil {
		fmt.Println("Greska pri kreiranju Data fajla")
	}
}

func BuildIndexFile(generation int, sortedData []Log) int {
	var IndexContent = new(bytes.Buffer)
	for i, data := range sortedData {
		binary.Write(IndexContent, binary.LittleEndian, data.Key)   //ispisi binarno kljuc
		binary.Write(IndexContent, binary.LittleEndian, LOG_SIZE*i) //ispisi binarno velicinu bloka puta i(prvi put 0, pa onda ide dalje..)
	}
	err := ioutil.WriteFile("Index-Gen-"+strconv.Itoa(generation), IndexContent.Bytes(), 0644)
	if err != nil {
		fmt.Println("Greska pri kreiranju Index fajla")
	}
	return IndexContent.Len() / len(sortedData) //vratice int koji je velicina bloka (key-adr in data) u indexu
	//TODO: PITANJE JE DA LI CE SVAKI BLOK U INDEXU BITI ISTE VELICINE?
}

// STEPS- kreiraj bloom, ucitaj logs u bloom, merkle, onda za svaki tip-sacuvaj write
func WriteToMultipleFiles(logs []*Log, FILENAME string) error {

	filter := BuildFilter(logs, 100, 0.1)

	indexes := BuildIndex(logs, 0)

	indexData := indexes.Entries

	summary := BuildSummary(indexData)

	// Serialize the logs to bytes
	var serializedLogs []byte
	for _, log := range logs {
		serializedLogs = append(serializedLogs, log.Serialize()...)
	}

	// Write data to SSTable file
	sstableFile, err := os.OpenFile(FILENAME+".sst", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer sstableFile.Close()

	_, err = sstableFile.Write(serializedLogs)
	if err != nil {
		return err
	}

	// Write indexes to Index file
	indexFile, err := os.OpenFile(FILENAME+".index", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	_, err = indexFile.Write(indexes.SerializeIndexes())
	if err != nil {
		return err
	}

	// Write summary to Summary file
	summaryFile, err := os.OpenFile(FILENAME+".summary", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer summaryFile.Close()

	_, err = summaryFile.Write(summary.Serialize())
	if err != nil {
		return err
	}

	// Write filter to Bloom Filter file
	filterFile, err := os.OpenFile(FILENAME+".filter", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer filterFile.Close()

	_, err = filterFile.Write(filter.Serialize())
	if err != nil {
		return err
	}

	return nil
}

func writeToSingleFile() {}

func readFromMultipleFiles() {}

func readFromSingleFile() {}

// delete, deleteMultiple, readRecords---> implementiraj?

/* func buildFilter():
funkcija uzima ocekivane elemente, br. ocek. el. i rate, dodaje el. u bloom i kreira bloom filter*/

func BuildFilter(logs []*Log, expectedElements int, falsePositiveRate float64) *Bloom2 {
	bloom := &Bloom2{}
	bloom.InitializeBloom2(expectedElements, falsePositiveRate)

	// Add each Key to the Bloom Filter
	for _, log := range logs {
		bloom.Add(log.Key)
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

/*type TOCEntry struct {
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
*/
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
