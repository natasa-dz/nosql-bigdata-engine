package SSTable

import (
	. "NAiSP/BloomFilter"
	. "NAiSP/Log"
	. "NAiSP/merkleTree"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)

const (
	SUMMARY_BLOCK_SIZE = 10
)

type SSTable struct {
	Header     Header
	Generation int
	Data       []Log
	Index      Index
	Summary    Summary
	Filter     Bloom2
	//	TOC        TOCEntry
	Metadata MerkleRoot
}

// MULTIPLE:
func BuildSSTableMultiple(sortedData []*Log, generation int, level int) {
	//cetri bafera za cetri razlicita fajla
	var FilterContent = new(bytes.Buffer)
	var DataContent = new(bytes.Buffer)
	var IndexContent = new(bytes.Buffer)
	var SummaryContent = new(bytes.Buffer)

	filter := BuildFilter(sortedData, len(sortedData), 0.01)
	binary.Write(FilterContent, binary.LittleEndian, filter.Serialize().Bytes())

	var offsetLog uint64
	offsetLog = 0
	WriteSummaryHeader(sortedData, SummaryContent) //u summary ce ispisati prvi i poslednji kljuc iz indexa
	for i, data := range sortedData {              //za svaki podatak
		binary.Write(DataContent, binary.LittleEndian, data.Serialize()) //ubaci ga u baffer
		if ((i+1)%10) == 0 || i == 0 {                                   //svaki 10. kljuc - summary napravljen u fazonu da ima jos indexa ne samo prvi i poslednji
			WriteSummaryLog(SummaryContent, uint64(sortedData[i].KeySize), sortedData[i].Key, uint64(IndexContent.Len()))
			//kako indexEntry i dalje nije zapisan pocetak njega je trenutna duzina indexcontent buffera, dakle ubacujemo ga u summary
		}
		offsetLog += uint64(len(data.Serialize()))
		WriteIndexLog(IndexContent, uint64(data.KeySize), data.Key, offsetLog) //tek sad pisemo indexEntry u index bafer
	}

	merkle := BuildMerkleTreeRoot(sortedData)
	//fje koje ce kreirati fajlove i ispisati sadrzaj navedenih bafera
	WriteToFile(generation, level, "Data", "Multiple", DataContent)
	WriteToFile(generation, level, "Index", "Multiple", IndexContent)
	WriteToFile(generation, level, "Summary", "Multiple", SummaryContent)
	WriteToFile(generation, level, "Bloom", "Multiple", FilterContent)
	WriteToTxtFile(generation, level, "Metadata", "Multiple", hex.EncodeToString(SerializeMerkleTree(merkle)))
}

func WriteSummaryLog(SummaryContent *bytes.Buffer, KeySize, Key, OffsetInIndexFile any) {
	binary.Write(SummaryContent, binary.LittleEndian, KeySize)           //upisi velicinu kljuca
	binary.Write(SummaryContent, binary.LittleEndian, Key)               //kljuc
	binary.Write(SummaryContent, binary.LittleEndian, OffsetInIndexFile) //trenutna duzina index bufera(kako 10. kljuc jos nije upisan ovo ce biti pocetak 10. kljuca)

}
func WriteSummaryHeader(sortedData []*Log, SummaryContent *bytes.Buffer) {
	binary.Write(SummaryContent, binary.LittleEndian, sortedData[0].KeySize) //min key
	binary.Write(SummaryContent, binary.LittleEndian, sortedData[0].Key)
	binary.Write(SummaryContent, binary.LittleEndian, sortedData[len(sortedData)-1].KeySize) //max key
	binary.Write(SummaryContent, binary.LittleEndian, sortedData[len(sortedData)-1].Key)
}

func WriteIndexLog(IndexContent *bytes.Buffer, KeySize, Key, OffSetInDataFile any) {
	binary.Write(IndexContent, binary.LittleEndian, KeySize)          //ispisi duzinu kljuca(ovo je uvek readable jer je uint64)
	binary.Write(IndexContent, binary.LittleEndian, Key)              //ispisi kljuc
	binary.Write(IndexContent, binary.LittleEndian, OffSetInDataFile) //ispisi offset bloka u Data fajlu
}

func WriteToFile(generation int, level int, fileType string, fileOrganisation string, bufferToWrite *bytes.Buffer) {
	err := ioutil.WriteFile("./Data/SSTables/"+fileOrganisation+"/"+fileType+"-"+strconv.Itoa(generation)+"-"+strconv.Itoa(level)+".bin", bufferToWrite.Bytes(), 0644)
	if err != nil {
		fmt.Println("Err u pisanju fajla "+fileType, err)
		return
	}
}
func WriteToTxtFile(generation int, level int, fileType string, fileOrganisation string, data string) {
	file, err := os.Create("./Data/SSTables/" + fileOrganisation + "/" + fileType + "-" + strconv.Itoa(generation) + "-" + strconv.Itoa(level) + ".txt")
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()
	_, err = file.WriteString(data)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}
}

// SINGLE FILE
func SortData(logs []*Log) {
	sort.Slice(logs, func(i, j int) bool {
		return string(logs[i].Key) < string(logs[j].Key)
	})
}

func WriteToSingleFile(logs []*Log, generation int, level int) error {
	SortData(logs)
	header := Header{
		LogsOffset:    32,
		BloomOffset:   0,
		IndexOffset:   0,
		SummaryOffset: 0}

	// Serialize the logs to bytes
	var serializedLogs []byte
	for _, log := range logs {
		serializedLogs = append(serializedLogs, log.Serialize()...)
	}
	header.BloomOffset += header.LogsOffset + uint64(len(serializedLogs))

	// Build Bloom Filter
	filter := BuildFilter(logs, len(logs), 0.1)
	filterSerialized := filter.Serialize()
	header.IndexOffset += header.BloomOffset + uint64(filterSerialized.Len())

	// Build Index
	indexData := BuildIndex(logs, header.LogsOffset)
	serializedIndex := SerializeIndexes(indexData)

	// Build Summary
	summary := BuildSummary(indexData, header.IndexOffset)
	summarySerialized := summary.Bytes()
	header.SummaryOffset += header.IndexOffset + uint64(len(serializedIndex))

	var FileContent = new(bytes.Buffer)
	merkle := BuildMerkleTreeRoot(logs)
	binary.Write(FileContent, binary.LittleEndian, header.HeaderSerialize())
	binary.Write(FileContent, binary.LittleEndian, serializedLogs)
	binary.Write(FileContent, binary.LittleEndian, filterSerialized.Bytes())
	binary.Write(FileContent, binary.LittleEndian, serializedIndex)
	binary.Write(FileContent, binary.LittleEndian, summarySerialized)
	WriteToFile(generation, level, "Data", "Single", FileContent)
	WriteToTxtFile(generation, level, "Metadata", "Single", hex.EncodeToString(SerializeMerkleTree(merkle)))

	return nil
}

// ReadFromMultipleFiles - TODO:Algoritam otprilike-proveri da li se trazeni kljuc nalazi u BloomFilter-u,
// ako se ne nalazi- predji na sledeci SSTable, ako si nasao-otvori Summary za dati SSTable nadji asocirani Log,
//iscitaj entrije kako bi nasli odgovarajuci, kada se sve odradi-iscitaj SSTable

// delete, deleteMultiple, readRecords---> implementiraj?

///////////////////// META DATA	===> NE BRISI OVO DOLE!!!!!

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

//func SortData(memtable map[string]Data) []Data {
//	// Create a slice to hold the data from the memtable
//	dataSlice := make([]Data, 0, len(memtable))
//
//	// Convert the map to a slice of Data entries
//	for _, data := range memtable {
//		dataSlice = append(dataSlice, data)
//	}
//
//	// Sort the Data slice by Key in ascending order
//	sort.Slice(dataSlice, func(i, j int) bool {
//		return dataSlice[i].Key < dataSlice[j].Key
//	})
//
//	// Assign incremental Index values to the sorted entries
//	for i, entry := range dataSlice {
//		entry.Index = int64(i)
//		dataSlice[i] = entry
//	}
//
//	return dataSlice
//}
