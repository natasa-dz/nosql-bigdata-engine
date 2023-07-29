package MemTable

import (
	"NAiSP/Log"
)

//trashold - granica/prag zapisa (< 100%)
//kapacitet strukture memtabla nije isto sto i ovaj trashold!!!
//poenta: trashold je procenat recimo da kazemo da je kapacitet b stabla 10elem i onda kazemo kad se popuni vise od 75% stabla ti flushuj na disk
//kada se trashold popuni Flushujemo na disk --> SSTable

type Memtable struct {
	size        uint
	trashold    float64
	tableStruct IMemtableStruct
}

func GenerateMemtable(kapacitetStrukture uint, pragZaFlush float64, imeStrukture string) *Memtable {
	table := Memtable{size: kapacitetStrukture, trashold: pragZaFlush}
	if imeStrukture == "btree" {
		table.tableStruct = CreateTree()
	} else {
		//TODO treba da se inicijalizuje skip lista
	}
	return &table
}

/*func (table *Memtable) Flush() {
	//size ce ici na 0 (ako planiramo da ih rotiramo, lakse ih je obrisati i uzeti sledeci iz liste)
	//treba da iz strukture uzmes i sortiras kljuceve
	//tako sortirane treba da uzmes i ubacis ih u SSTable
	//kad provalim kako SSTable radi mozda se izmeni ovaj algoritam, ali svakako otprilike je ovako
	//mozda nije lose imati neku strukturu koja ce biti prelazna izmedju svih memtablestruktura (btree i skiplist) i SSTable struktova
}*/

//  TREBA IMPLEMENTIRATI

/*
	func (table *Memtable) GetElements() []Data {
		var elements []Data

		// Traverse the BTree and collect non-tombstone elements
		table.tableStruct.Traverse(func(node *BTreeNode) {
			for _, data := range node.Data {
				if !data.Tombstone {
					elements = append(elements, data)
				}
			}
		})

		return elements
	}
*/
/*func (table *Memtable) Flush() error {

elements := table.GetElements()
if len(elements) == 0 {
	// Nothing to flush
	return nil
}

generation := 0 // You might need a way to manage generation numbers

// Create a BloomFilter for the SSTable
bloomFilter := BuildFilter(elements, len(elements), 0.01) // Example: false positive rate of 1%

/*	// Create the SSTable Index
	sstableIndex := BuildIndex(elements)
	// Build the Merkle Tree from the sorted data
	merkleRoot := BuildMerkleTreeRoot(elements)
*/
/*	dataMap := make(map[string]Data)
	for _, data := range elements {
		dataMap[data.Key] = data
	}

	// Create the Metadata for the SSTable
	metadata, err := BuildMetaData(dataMap, bloomFilter, fmt.Sprintf("usertable-%d-SSTable.txt", generation), generation)
	if err != nil {
		return err
	}*/

/*	// Save the SSTable components to disk
	err = SaveDataToDisk(elements, generation)
	if err != nil {
		return err
	}

	err = SaveIndexToDisk(sstableIndex, generation)
	if err != nil {
		return err
	}

	err = SaveSummaryToDisk(metadata.DataSummary, generation)
	if err != nil {
		return err
	}

	err = SaveBloomFilterToDisk(metadata.BloomFilter, generation)
	if err != nil {
		return err
	}

	err = SaveTOCToDisk(metadata.TOC, generation)
	if err != nil {
		return err
	}

	err = SaveMetadataToDisk(metadata, generation)
	if err != nil {
		return err
	}
*/
// Clear the Memtable after successful flush
/*	table.tableStruct = CreateTree()

	return nil
}*/

func (table *Memtable) Insert(data Log.Log) {
	//if:
	//proveri da li postoji, ako postoji samo updatuj podatak i returnuj
	//else:
	//insertuj
	//proveri da li je popunjenost > trashold
	//jeste onda flushuj
	indexInNode, AddressOfNode := table.tableStruct.Search(string(data.Key))
	if AddressOfNode != nil {
		AddressOfNode.keys[indexInNode] = data
		return
	} else {
		table.tableStruct.Insert(data)
		if (float64(table.tableStruct.GetNumOfElements()) / float64(table.size)) > table.trashold {
			//unsortedData := table.tableStruct.GetAllLogs()
			//table.Flush() TODO Flushuj i zameni tabele ili isprazni kako god ti volja
		}
	}

}

// ///////////////////////////////////// PRIMER KAKO BI MOGAO OTRPILIKE IZGLEDATI FLUSH

/*
	func (table *Memtable) FlushTry() {
		elements := table.GetElements()
		generation := 0 // You might need a way to manage generation numbers

		// Create an SSTable and serialize it (for demonstration purposes, adjust as needed)
		sstable := &SSTable{
			Generation: generation,
			Elements:   elements,
		}
		sstable.Serialize("usertable", generation, "db")

		// Clear the Memtable
		table.tableStruct = CreateTree()
	}
*/

// Now, when you create the SSTable, you can use these functions to save each element to disk with the correct naming format.
/*func CreateSSTable(memtable map[string]Data, generation int) error {
	// Sort the data from the memtable
	sortedData := SortData(memtable)

	// Build the Bloom Filter
	bloomFilter := BuildFilter(sortedData, expectedElements, falsePositiveRate)

	// Build the Index
	index := BuildIndex(sortedData)

	// Build the Summary
	summary := BuildSummary(sortedData, generation)

	// Build the Merkle Tree root
	merkleRoot := BuildMerkleTreeRoot(sortedData)

	// Create the Metadata
	metadata := &Metadata{
		Version:      "1.0",
		DataSummary:  summary,
		BloomFilter:  bloomFilter,
		SSTableIndex: index,
		TOC:          nil, // Implement TOC generation if needed
		MerkleRoot:   merkleRoot,
	}

	// Save each element to disk with the appropriate naming format
	if err := SaveDataToDisk(sortedData, generation); err != nil {
		return err
	}

	if err := SaveIndexToDisk(index, generation); err != nil {
		return err
	}

	if err := SaveSummaryToDisk(summary, generation); err != nil {
		return err
	}

	if err := SaveBloomFilterToDisk(bloomFilter, generation); err != nil {
		return err
	}

	// Save other elements (TOC, Metadata) if needed

	return nil
}*/
