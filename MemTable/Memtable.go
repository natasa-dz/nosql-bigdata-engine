package MemTable

import (
	. "NAiSP/Log"
	sstable "NAiSP/SSTable"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

//trashold - granica/prag zapisa (< 100%)
//kapacitet strukture memtabla nije isto sto i ovaj trashold!!!
//poenta: trashold je procenat recimo da kazemo da je kapacitet b stabla 10elem i onda kazemo kad se popuni vise od 75% stabla ti flushuj na disk
//kada se trashold popuni Flushujemo na disk --> SSTable

type Memtable struct {
	size        uint32
	trashold    float64
	tableStruct IMemtableStruct
}

func GenerateMemtable(kapacitetStrukture uint32, pragZaFlush float64, imeStrukture string, stepenBStabla int) *Memtable {
	table := Memtable{size: kapacitetStrukture, trashold: pragZaFlush}
	if imeStrukture == "btree" {
		table.tableStruct = CreateTree(stepenBStabla)
	} else {
		//TODO treba da se inicijalizuje skip lista
	}
	return &table
}

/*func (table *Memtable) Flush() {
	//size ce ici na 0 (ako planiramo da ih rotiramo, lakse ih je obrisati i uzeti sledeci iz liste)
	//treba da iz strukture uzmes i sortiras kljuceve
	//tako sortirane treba da uzmes i ubacis ih u SSTable
	//zatim treba da ga obrises
	//NAPOMENA: U VECOJ APSTRAKCIJI (WRITE PATH) TREBA DA IMAMO LISTU PRAZNIH MEMTABLOVA I KAKO SE JEDAN POPUNI TAKO SE TAJ FLUSHUJE I BRISE,
		//SLEDECI KRECE DA SE PUNI I TAKODJE SE PRAVI I JEDAN PRAZAN DA NE BI LISTA OSTALA BEZ MEMTABLOVA U JEDNOM MOMENTU
}*/

func (table *Memtable) Flush(numOfFiles string) {
	unsortedLogs := table.tableStruct.GetAllLogs()
	SortedLogs := sortData(unsortedLogs)
	sstable.BuildSSTable(SortedLogs, getLastGen(numOfFiles)+1, 1, numOfFiles)
}

func sortData(entries []*Log) []*Log {
	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].Key) < string(entries[j].Key)
	})
	return entries
}

func (table *Memtable) TableFull() bool {
	if (float64(table.tableStruct.GetNumOfElements()) / float64(table.size)) > table.trashold {
		return true
	}
	return false
}

func (table *Memtable) Insert(data *Log, numOfFiles string) {
	indexInNode, AddressOfNode := table.tableStruct.Search(string(data.Key))
	if AddressOfNode != nil {
		AddressOfNode.keys[indexInNode] = *data
	} else {
		table.tableStruct.Insert(*data)
		if table.TableFull() {
			table.Flush(numOfFiles)
			table.tableStruct.Empty()
		}
	}
}

func (table *Memtable) Delete(key string) bool {
	ans := table.tableStruct.Delete(key)
	return ans
}

func (table *Memtable) Search(key string) *Log {
	indexInNode, nodeAdrress := table.tableStruct.Search(key)
	if indexInNode != -1 {
		return &nodeAdrress.keys[indexInNode]
	}
	return &Log{} //NOTE: OVO JE JAKO BITNO DA SE PROVERAVA NAKON SEARCHA UZ POMOC MEMTABLA!!!!
}

// dobavi poslednju generaciju i najveci level za pravljenje SSTabla
func getLastGen(numOfFiles string) int {
	nameOfDir := strings.ToUpper(string(numOfFiles[0])) + numOfFiles[1:]
	files, err := os.ReadDir("Data/SStables/" + nameOfDir) //read all files from Single/Multiple
	if err != nil {
		fmt.Println("Err when reading last generation")
	}

	onlyTOCFiles := []string{} //will have names(Strings) of all TOC files
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "TOC-") {
			onlyTOCFiles = append(onlyTOCFiles, file.Name())
		}
	}

	maxgen := 1
	for _, fileName := range onlyTOCFiles {
		parts := strings.Split(fileName, "-")
		gen, _ := strconv.Atoi(parts[1])
		if gen > maxgen {
			maxgen = gen
		}
	}
	return maxgen
}
