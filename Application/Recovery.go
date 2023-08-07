package Application

import (
	. "NAiSP/Log"
	ss "NAiSP/SSTable"
	wal "NAiSP/WriteAheadLog"
	"fmt"
	"os"
	"sort"
	"strings"
)

func (app *Application) Recover(numOfFiles string) bool {
	walFiles := getAllWalFiles(numOfFiles)
	SSData := extractDataFromSSFile(numOfFiles)
	logsToInsertInMemtable, numOfLogsInLastWal := getAllLogsForMemtable(walFiles, SSData, numOfFiles)
	for _, log := range logsToInsertInMemtable {
		if log.Tombstone == false {
			app.Memtable.Insert(log, numOfFiles, app.ConfigurationData.NumOfSummarySegmentLogs)
			app.Cache.Insert(log)
		} /*else {
			app.Memtable.Delete(string(log.Key))
		}*/
	}
	app.NumOfWalInserts = numOfLogsInLastWal
	if app.NumOfWalInserts == app.ConfigurationData.NumOfWalSegmentLogs {
		needNewWal := true
		return needNewWal
	}
	needNewWal := false
	return needNewWal
}

func getAllLogsForMemtable(walFiles []os.DirEntry, SSData []*Log, numOfFiles string) ([]*Log, int) {
	var retVal []*Log
	numOfLogsInLastWalFile := 0 //brojac koliko ima logova u poslednjem wal fajlu
	found := false
	for i, file := range walFiles {
		openedFile, err := os.Open("Data/Wal/" + strings.Title(numOfFiles) + "/" + file.Name())
		if err != nil {
			fmt.Println("Error opening Wal file:", err)
			return nil, -1
		}
		defer openedFile.Close()

		for {
			log, err := wal.ReadNextRecordFromWal(openedFile) //citas 1 po 1 iz wal.log
			if err != nil {
				break // kraj Wal fajla
			}
			if i == 0 {
				numOfLogsInLastWalFile++
			}

			if Contains(SSData, log) {
				found = true
				break
			}
			retVal = append(retVal, log)

		}
		if found {
			break
		}
	}

	return retVal, numOfLogsInLastWalFile
}

func Contains(SS []*Log, toCheck *Log) bool {
	for _, temp := range SS {
		if toCheck.Equals(temp) {
			return true
		}
	}
	return false
}

func extractDataFromSSFile(numOfFiles string) []*Log {
	SSFile := getLatestSSTableFile(numOfFiles)
	openedFile, err := os.Open("Data/SStables/" + strings.Title(numOfFiles) + "/" + SSFile.Name())
	if err != nil {
		fmt.Println("Error opening SS file:", err)
		return nil
	}
	defer openedFile.Close()
	openedFileInfo, _ := os.Stat("Data/SStables/" + strings.Title(numOfFiles) + "/" + SSFile.Name())

	if numOfFiles == "single" {
		header, _ := ss.ReadHeader(openedFile)
		retVal, _ := ReadLogs(openedFile, int64(header.LogsOffset), header.BloomOffset)
		return retVal
	}

	retVal, _ := ReadLogs(openedFile, 0, uint64(openedFileInfo.Size()))
	return retVal
}

func getLatestSSTableFile(numOfFiles string) os.DirEntry {
	var retVal os.DirEntry
	numOfFiles = strings.Title(numOfFiles)
	files, err := os.ReadDir("Data/SStables/" + numOfFiles + "/")
	if err != nil {
		fmt.Println("ERR...Cannot gather all SStableFiles")
		return nil
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "Data-") {
			if retVal == nil {
				retVal = file
				break
			}
			if file.Name() > retVal.Name() {
				retVal = file //trebalo bi da osigura da se dobije poslednja generacija
			}
		}

	}
	return retVal
}

func getAllWalFiles(numOfFiles string) []os.DirEntry {
	numOfFiles = strings.Title(numOfFiles)
	files, err := os.ReadDir("Data/Wal/" + numOfFiles + "/")
	if err != nil {
		fmt.Println("ERR...Cannot gather all Wal files")
		return nil
	}
	if len(files) == 0 {
		os.Create("Data/Wal/" + numOfFiles + "/wal_0001.log")
		files, err := os.ReadDir("Data/Wal/" + numOfFiles + "/")
		if err != nil {
			fmt.Println("ERR...Cannot gather all Wal files")
			return nil
		}
		return files
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() > files[j].Name()
	})

	return files
}
