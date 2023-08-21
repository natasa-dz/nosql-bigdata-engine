package MemTable

import (
	"NAiSP/LSM"
	. "NAiSP/Log"
	sstable "NAiSP/SSTable"
	"sort"
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

func (table *Memtable) Flush(numOfFiles string, summaryBlockSize int, fileType string) {
	unsortedLogs := table.tableStruct.GetAllLogs()
	SortedLogs := sortData(unsortedLogs)
	maxGeneration, _ := LSM.GetMaxGenerationFromLevel(fileType, 1)
	sstable.BuildSSTable(SortedLogs, maxGeneration+1, 1, numOfFiles, summaryBlockSize)
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

func (table *Memtable) Insert(data *Log, numOfFiles string, summaryBlockSize int, fileType string) {
	foundLog := table.tableStruct.Search(string(data.Key))
	if foundLog != nil {
		foundLog.Value = (*data).Value
	} else {
		table.tableStruct.Insert(data)
		if table.TableFull() {
			table.Flush(numOfFiles, summaryBlockSize, fileType)
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
