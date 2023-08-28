package Application

import (
	. "NAiSP/Log"
	ss "NAiSP/SSTable"
	wal "NAiSP/WriteAheadLog"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func (app *Application) Recover(numOfFiles string) {
	walFiles := getAllWalFiles(numOfFiles) //ako je prazan direktorijum on ce napraviti novi wal.log fajl
	SSData := extractDataFromSSFile(numOfFiles)
	logsToInsertInMemtable, numOfLogsInLastWal := getAllLogsForMemtable(walFiles, SSData, numOfFiles)

	for i := len(logsToInsertInMemtable) - 1; i >= 0; i-- {
		/*if logsToInsertInMemtable[i].Tombstone == false {
			app.Memtable.Insert(logsToInsertInMemtable[i], numOfFiles, app.ConfigurationData.NumOfSummarySegmentLogs, app.ConfigurationData.NumOfFiles)
			app.Cache.Insert(logsToInsertInMemtable[i])
		} else {
			app.Memtable.Delete(string(log.Key))
			app.Cache.delete(string...)
		}*/

		app.Memtable.Insert(logsToInsertInMemtable[i], numOfFiles, app.ConfigurationData.NumOfSummarySegmentLogs, app.ConfigurationData.NumOfFiles)
		app.Cache.Insert(logsToInsertInMemtable[i])
	}
	app.NumOfWalInserts = numOfLogsInLastWal
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

		logs, _ := wal.ReadWal(openedFile) //iscitas ceo wal fajl
		for j := len(logs) - 1; j >= 0; j-- {

			if i == 0 {
				numOfLogsInLastWalFile++
			}
			if Contains(SSData, logs[j]) {
				found = true
				break
			}
			retVal = append(retVal, logs[j])
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
	SSFile, _ := FindMostRecentFile(numOfFiles)
	if SSFile == "" { //prazan direktorijum
		return nil
	}
	openedFile, err := os.Open(SSFile)
	if err != nil {
		fmt.Println("Error opening SS file:", err)
		return nil
	}
	defer openedFile.Close()
	openedFileInfo, _ := os.Stat(SSFile)

	if numOfFiles == "single" {
		header, _ := ss.ReadHeader(openedFile)
		retVal, _ := ReadLogs(openedFile, int64(header.LogsOffset), header.BloomOffset)
		return retVal
	}

	retVal, _ := ReadLogs(openedFile, 0, uint64(openedFileInfo.Size()))
	return retVal
}
func FindMostRecentFile(numOfFiles string) (string, error) {
	var mostRecentFile string
	var mostRecentTime time.Time

	err := filepath.Walk("Data/SSTables/"+numOfFiles+"/", func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			modTime := info.ModTime()
			numbers := strings.Split(info.Name(), "-")
			if modTime.After(mostRecentTime) && numbers[0] == "Data" {
				mostRecentTime = modTime
				mostRecentFile = filePath
			}
		}
		return nil
	})

	if err != nil {
		return "", err
	}
	fmt.Println(mostRecentFile)
	return mostRecentFile, nil
}

func getAllWalFiles(numOfFiles string) []os.DirEntry {
	numOfFiles = strings.Title(numOfFiles)
	files, err := os.ReadDir("Data/Wal/" + numOfFiles + "/")
	if err != nil {
		fmt.Println("ERR...Cannot gather all Wal files")
		return nil
	}
	if len(files) == 0 { //ako je prazan direktorijum otvori novi prvi wal.log fajl
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
