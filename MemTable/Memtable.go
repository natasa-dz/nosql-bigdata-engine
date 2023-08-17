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

func GenerateMemtable(kapacitetStrukture uint32, pragZaFlush float64, imeStrukture string, stepenBStabla int, skipListHeight int) *Memtable {
	table := Memtable{size: kapacitetStrukture, trashold: pragZaFlush}
	if imeStrukture == "btree" {
		table.tableStruct = CreateTree(stepenBStabla)
	} else {
		table.tableStruct = InitSkipList(skipListHeight)
	}
	return &table
}

func (table *Memtable) Flush(numOfFiles string, summaryBlockSize int) {
	unsortedLogs := table.tableStruct.GetAllLogs()
	SortedLogs := sortData(unsortedLogs)
	sstable.BuildSSTable(SortedLogs, getLastGen(numOfFiles)+1, 1, numOfFiles, summaryBlockSize)
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

func (table *Memtable) Insert(data *Log, numOfFiles string, summaryBlockSize int) {
	foundLog := table.tableStruct.Search(string(data.Key))
	if foundLog != nil {
		foundLog.Value = (*data).Value
	} else {
		table.tableStruct.Insert(data)
		if table.TableFull() {
			table.Flush(numOfFiles, summaryBlockSize)
			table.tableStruct.Empty()
		}
	}
}

func (table *Memtable) Delete(key string) bool {
	ans := table.tableStruct.Delete(key)
	return ans
}

func (table *Memtable) Search(key string) *Log {
	foundLog := table.tableStruct.Search(key)
	if foundLog != nil {
		return foundLog
	}
	return nil
}

// dobavi poslednju generaciju i najveci level za pravljenje SSTabla
func getLastGen(numOfFiles string) int {
	nameOfDir := strings.ToUpper(string(numOfFiles[0])) + numOfFiles[1:]
	files, err := os.ReadDir("Data/SSTables/" + nameOfDir) //read all files from Single/Multiple
	if err != nil {
		fmt.Println("Err when reading last generation")
	}

	onlyTOCFiles := []string{} //will have names(Strings) of all TOC files
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "TOC-") {
			onlyTOCFiles = append(onlyTOCFiles, file.Name())
		}
	}

	maxgen := 0
	for _, fileName := range onlyTOCFiles {
		parts := strings.Split(fileName, "-")
		gen, _ := strconv.Atoi(parts[1])
		if gen > maxgen {
			maxgen = gen
		}
	}
	return maxgen
}
