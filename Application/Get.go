package Application

import (
	bloomFilter "NAiSP/BloomFilter"
	fileManager "NAiSP/FileManager"
	. "NAiSP/Log"
	sstable "NAiSP/SSTable"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func (app *Application) Get(key string) *Log {

	dataPath := "./Data/SSTables/" + strings.Title(app.ConfigurationData.NumOfFiles) + "/"

	var foundLog *Log

	foundLog = app.CheckMemtable(key)
	if foundLog == nil {
		foundLog = app.CheckCache(key)
		if foundLog == nil {
			if app.ConfigurationData.NumOfFiles == "multiple" {
				foundLog = app.CheckSSTableMultiple(dataPath, key)
			} else {
				foundLog = app.CheckSSTableSingle(dataPath, key)
			}
		}
	}

	return foundLog
}

func (app *Application) CheckMemtable(key string) *Log { //vraca da li je Log pronadjen i ako jeste da li je obrisan
	valueMemtable := app.Memtable.Search(key)
	if valueMemtable == nil {
		fmt.Println("Key not found in memtable")
		return nil
	} else if valueMemtable.Tombstone == true {
		fmt.Println("Key found in memtable is deleted")
		return valueMemtable
	}

	fmt.Println("Read from memtable: ")
	return valueMemtable
}

func (app *Application) CheckCache(key string) *Log {

	valueCache := app.Cache.Search(key)
	if valueCache == nil {
		fmt.Println("Key not found in cache")
		return nil
	}

	fmt.Println("Key read from cache")
	return valueCache
}

func (app *Application) CheckSSTableSingle(dataPath string, key string) *Log {
	sstableFiles := fileManager.GetFilesWithWord(dataPath, "Data")
	sstableFiles = fileManager.SortFileNames(sstableFiles, true)

	for _, ssTableFileName := range sstableFiles {
		sstableFile := fileManager.Open(dataPath + ssTableFileName)
		if sstableFile == nil {
			fmt.Println("Error reading sstable file: ", ssTableFileName)
		}
		header, err := sstable.ReadHeader(sstableFile)
		if err != nil {
			fmt.Println("Error reading sstable header")
			return nil
		}

		bloom := bloomFilter.ReadBloom(sstableFile, int64(header.BloomOffset))

		if bloom.BloomSearch([]byte(key)) {
			fmt.Println("Bloom filter indicates that the key might exist in file ", ssTableFileName)
			foundOffset := app.GetOffset(dataPath+ssTableFileName, "", key, header)
			if foundOffset != -1 {
				foundLog := GetValueFromDataFile(foundOffset, dataPath+ssTableFileName)
				return foundLog
			}

		}
	}

	fmt.Println("Bloom filter indicates that the key does not exist")
	return nil
}

func (app *Application) CheckSSTableMultiple(dataPath string, key string) *Log {
	bloomFiles := fileManager.GetFilesWithWord(dataPath, "Bloom")
	bloomFiles = fileManager.SortFileNames(bloomFiles, true)

	for _, bloomFileName := range bloomFiles {
		deserializedFN := fileManager.DeserializeFileName(bloomFileName)
		fileNumber := strconv.Itoa(deserializedFN.Generation) + "-" + strconv.Itoa(deserializedFN.Level)
		bloomFile := fileManager.Open(dataPath + bloomFileName)

		if bloomFile != nil {
			bloom := bloomFilter.ReadBloom(bloomFile, 0)

			if bloom.BloomSearch([]byte(key)) {
				fmt.Println("Bloom filter indicates that the key might exist in file ", bloomFileName)
				foundOffset := app.GetOffset(dataPath, fileNumber, key, nil)
				if foundOffset != -1 {
					var pathDataFile string
					if app.ConfigurationData.NumOfFiles == "multiple" {
						pathDataFile = dataPath + "Data-" + fileNumber
					} else {
						pathDataFile = dataPath
					}

					foundLog := GetValueFromDataFile(foundOffset, pathDataFile)
					return foundLog
				}
			}
		}

	}

	fmt.Println("Bloom filter indicates that the key does not exist")
	return nil
}

func (app *Application) GetOffset(path string, fileNumber string, key string, header *sstable.Header) int64 {
	var summaryFile *os.File

	if app.ConfigurationData.NumOfFiles == "multiple" {
		summaryPath := path + "Summary-"
		summaryPath += fileNumber

		summaryFile = fileManager.Open(summaryPath)
	} else {
		summaryFile = fileManager.Open(path)
	}

	startKey, endKey := sstable.ReadSummaryHeader(summaryFile, int64(header.SummaryOffset))

	if key >= startKey && key <= endKey {
		var summary *sstable.Summary
		if app.ConfigurationData.NumOfFiles == "multiple" {
			summary, _ = sstable.ReadSummary(summaryFile, 0)
		} else {
			summary, _ = sstable.ReadSummary(summaryFile, int64(header.SummaryOffset))
		}

		var indexFile *os.File
		if app.ConfigurationData.NumOfFiles == "multiple" {
			indexFile = fileManager.Open(path + "Index-" + fileNumber)
		} else {
			indexFile = summaryFile
		}
		indexStart, indexEnd := sstable.SearchIndexEntry(summary.Entries, []byte(key))
		foundOffset := sstable.FindKeyOffset(indexFile, key, int64(indexStart.Offset), int64(indexEnd.Offset))

		if foundOffset != -1 {
			fmt.Println("Key found in SStable-", fileNumber)
			return foundOffset
		} else {
			fmt.Println("Key not found in SStable-", fileNumber)
		}
	} else {
		fmt.Println("Key not found in SSTable-", fileNumber)
	}

	return -1
}

func GetValueFromDataFile(offset int64, path string) *Log {
	dataFile := fileManager.Open(path)
	dataFile.Seek(offset, io.SeekStart)
	//logs, _ := sstable.GetAllLogs(dataFile, "Multiple")
	//fmt.Println(logs)
	if dataFile != nil {
		readLog, err := ReadLog(dataFile)
		if err == nil {
			return readLog
		}
	}

	fmt.Println("Error reading data file ", path)
	return nil

}
