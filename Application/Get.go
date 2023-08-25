package Application

import (
	bloomFilter "NAiSP/BloomFilter"
	fileManager "NAiSP/FileManager"
	. "NAiSP/Log"
	sstable "NAiSP/SSTable"
	"fmt"
	"io"
	"strings"
)

func (app *Application) Get(key string) *Log {

	dataPath := "./Data/SSTables/" + strings.Title(app.ConfigurationData.NumOfFiles) + "/"

	valueMemtable := app.Memtable.Search(key)
	if valueMemtable == nil {
		fmt.Println("Key not found in memtable")

		valueCache := app.Cache.Search(key)
		if valueCache == nil {
			fmt.Println("Key not found in cache")

			foundLog := CheckSSTable(dataPath, key)
			if foundLog == nil {
				return nil
			} else {
				return foundLog
			}
		} else {
			fmt.Println("Read from cache: ", string(valueCache.Value))
			return valueCache
		}
	} else {
		fmt.Println("Read from memtable: ", string(valueMemtable.Value))
		return valueMemtable
	}

	return nil

}

func CheckSSTable(dataPath string, key string) *Log {
	bloomFiles := fileManager.GetFilesWithWord(dataPath, "Bloom")

	for _, bloomFileName := range bloomFiles {
		numbers := strings.Split(bloomFileName, "-")
		generation := numbers[1]
		level := numbers[2]
		fileNumber := generation + "-" + level
		bloomFile := fileManager.Open(dataPath + bloomFileName)

		if bloomFile != nil {
			bloom := bloomFilter.ReadBloom(bloomFile, 0)

			if bloom.BloomSearch([]byte(key)) {
				fmt.Println("Bloom filter indicates that the key might exist in file ", bloomFileName)
				foundOffset := GetOffset(dataPath, fileNumber, key)
				if foundOffset != -1 {
					pathDataFile := dataPath + "Data-" + fileNumber
					foundLog := GetValueFromDataFile(foundOffset, pathDataFile)
					return foundLog
				}
			}
		}

	}

	fmt.Println("Bloom filter indicates that the key does not exist")
	return nil
}

func GetOffset(path string, fileNumber string, key string) int64 {
	summaryPath := path + "Summary-"
	summaryPath += fileNumber

	summaryFile := fileManager.Open(summaryPath)

	startKey, endKey := sstable.ReadSummaryHeader(summaryFile, 0)

	if key >= startKey && key <= endKey {
		summary, err := sstable.ReadSummary(summaryFile, 0)
		if err == nil {
			indexFile := fileManager.Open(path + "Index-" + fileNumber)
			indexStart, indexEnd := sstable.SearchIndexEntry(summary.Entries, []byte(key))
			foundOffset := sstable.FindKeyOffset(indexFile, key, int64(indexStart.Offset), int64(indexEnd.Offset))

			if foundOffset != -1 {
				fmt.Println("Key found in SStable-", fileNumber)
				return foundOffset
			} else {
				fmt.Println("Key not found in SStable-", fileNumber)
			}
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
