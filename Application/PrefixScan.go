package Application

import (
	fileManager "NAiSP/FileManager"
	. "NAiSP/Log"
	"NAiSP/MemTable"
	sstable "NAiSP/SSTable"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func (app *Application) PrefixScan(prefix string) []*Log {
	aggregated := MemTable.InitSkipList(10)

	if app.ConfigurationData.NumOfFiles == "multiple" {
		app.PrefixScanSSTableMultiple(prefix, aggregated)
	} else {
		app.PrefixScanSSTableSingle(prefix, aggregated)
	}

	foundMemtable := app.Memtable.SearchForPrefix(prefix)
	for _, log := range foundMemtable {
		l := aggregated.Search(string(log.Key))
		if l != nil {
			l = log
		} else {
			aggregated.Insert(log)
		}
	}

	return aggregated.GetAllLogs()
}

func (app *Application) PrefixScanSSTableSingle(prefix string, aggregated *MemTable.SkipList) {
	path := "./Data/SSTables/" + strings.Title(app.ConfigurationData.NumOfFiles) + "/"

	sstableFiles := fileManager.GetFilesWithWord(path, "Data")
	sstableFiles = fileManager.SortFileNames(sstableFiles, false)

	for _, fileName := range sstableFiles {
		var foundOffsets []int64
		sstableFile := fileManager.Open(path + fileName)

		if sstableFile == nil {
			fmt.Println("Error reading sstable file: ", fileName)
			break
		}
		header, err := sstable.ReadHeader(sstableFile)
		if err != nil {
			fmt.Println("Error reading header of sstable: ", fileName)
			break
		}

		startKey, endKey := sstable.ReadSummaryHeader(sstableFile, int64(header.SummaryOffset))
		if (prefix >= startKey && prefix <= endKey) || (strings.HasPrefix(startKey, prefix)) {
			summary, _ := sstable.ReadSummary(sstableFile, int64(header.SummaryOffset))
			startOffset := sstable.SearchIndexEntryPrefix(summary.Entries, prefix)
			foundOffsets = append(foundOffsets, sstable.FindKeyOffsetsWithPrefix(sstableFile, prefix, int64(startOffset.Offset))...)
		}

		for _, offset := range foundOffsets {
			sstableFile.Seek(int64(offset), io.SeekStart)
			readLog, err := ReadLog(sstableFile)
			if err == nil {
				l := aggregated.Search(string(readLog.Key))
				if l != nil {
					l = readLog
				} else {
					aggregated.Insert(readLog)
				}
			}
		}
	}
}

func (app *Application) PrefixScanSSTableMultiple(prefix string, aggregated *MemTable.SkipList) {
	path := "./Data/SSTables/" + strings.Title(app.ConfigurationData.NumOfFiles) + "/"

	sstableFiles := fileManager.GetFilesWithWord(path, "Summary")
	sstableFiles = fileManager.SortFileNames(sstableFiles, false)

	for _, fileName := range sstableFiles {
		var foundOffsets []int64

		fileNameObj := fileManager.DeserializeFileName(fileName)
		fileNumber := strconv.Itoa(fileNameObj.Generation) + "-" + strconv.Itoa(fileNameObj.Level)

		ssummaryFile := fileManager.Open(path + fileName)

		if ssummaryFile == nil {
			fmt.Println("Error reading summary  file: ", path+fileName)
			break
		}

		startKey, endKey := sstable.ReadSummaryHeader(ssummaryFile, 0)
		indexFile := fileManager.Open(path + "Index-" + fileNumber + ".bin")
		if indexFile == nil {
			fmt.Println("Error reading index file: ", path+"Index-"+fileNumber+".bin")
			break
		}
		if (prefix >= startKey && prefix <= endKey) || (strings.HasPrefix(startKey, prefix)) {
			summary, _ := sstable.ReadSummary(ssummaryFile, 0)
			startOffset := sstable.SearchIndexEntryPrefix(summary.Entries, prefix)
			foundOffsets = append(foundOffsets, sstable.FindKeyOffsetsWithPrefix(indexFile, prefix, int64(startOffset.Offset))...)
		}

		dataFile := fileManager.Open(path + "Data-" + fileNumber + ".bin")
		if dataFile == nil {
			fmt.Println("Error reading data file: ", path+"Data-"+fileNumber+".bin")
			break
		}

		for _, offset := range foundOffsets {
			dataFile.Seek(int64(offset), io.SeekStart)
			readLog, err := ReadLog(dataFile)
			if err == nil {
				l := aggregated.Search(string(readLog.Key))
				if l != nil {
					l = readLog
				} else {
					aggregated.Insert(readLog)
				}
			}
		}
	}
}
