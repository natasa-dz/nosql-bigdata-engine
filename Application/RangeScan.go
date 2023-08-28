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

func (app *Application) RangeScan(minKey, maxKey string) []*Log {
	aggregated := MemTable.CreateTree(2)

	if app.ConfigurationData.NumOfFiles == "multiple" {
		app.ScanSSTableMultiple(minKey, maxKey, aggregated)
	} else {
		app.ScanSSTableSingle(minKey, maxKey, aggregated)
	}

	foundMemtable := app.Memtable.SearchInterval(minKey, maxKey)
	for _, log := range foundMemtable {
		l := aggregated.Search(string(log.Key))
		if l != nil {
			l = log
		} else {
			aggregated.Insert(log)
		}
	}

	return RemoveDeleted(aggregated.GetAllLogs())
}

func (app *Application) ScanSSTableSingle(minKey, maxKey string, aggregated *MemTable.Tree) {

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
		if minKey >= startKey && minKey <= endKey {
			summary, _ := sstable.ReadSummary(sstableFile, int64(header.SummaryOffset))
			indexStartMinKey := sstable.SearchIndexEntry(summary.Entries, []byte(minKey))
			foundOffsets = append(foundOffsets, sstable.FindKeyOffsetsInInterval(sstableFile, minKey, maxKey, int64(indexStartMinKey.Offset))...)
		}
		if maxKey >= startKey && maxKey <= endKey {
			summary, _ := sstable.ReadSummary(sstableFile, int64(header.SummaryOffset))
			indexStartMaxKey := sstable.SearchIndexEntry(summary.Entries, []byte(maxKey))
			foundOffsets = append(foundOffsets, sstable.FindKeyOffsetsInInterval(sstableFile, minKey, maxKey, int64(indexStartMaxKey.Offset))...)
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

func (app *Application) ScanSSTableMultiple(minKey, maxKey string, aggregated *MemTable.Tree) {

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

		if minKey >= startKey && minKey <= endKey {
			summary, _ := sstable.ReadSummary(ssummaryFile, 0)
			indexStartMinKey := sstable.SearchIndexEntry(summary.Entries, []byte(minKey))
			foundOffsets = append(foundOffsets, sstable.FindKeyOffsetsInInterval(indexFile, minKey, maxKey, int64(indexStartMinKey.Offset))...)
		}
		if maxKey >= startKey && maxKey <= endKey {
			summary, _ := sstable.ReadSummary(ssummaryFile, 0)
			indexStartMaxKey := sstable.SearchIndexEntry(summary.Entries, []byte(maxKey))
			foundOffsets = append(foundOffsets, sstable.FindKeyOffsetsInInterval(indexFile, minKey, maxKey, int64(indexStartMaxKey.Offset))...)
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
