package Application

import (
	. "NAiSP/Log"
	wal "NAiSP/WriteAheadLog"
	"fmt"
	"strings"
)

func (app *Application) Delete(key string) bool {
	var foundLog *Log

	foundLog = app.CheckMemtable(key)
	if foundLog != nil { //kljuc postoji u Memtable-u: izmeniti tombstone log-a i ukloniti ga iz cache-a ako postoji

		foundLog.Tombstone = true
		app.Memtable.Insert(foundLog, app.ConfigurationData.NumOfFiles, app.ConfigurationData.NumOfSummarySegmentLogs, app.ConfigurationData.NumOfFiles)
		wal.AppendToWal(app.WalFile, foundLog) //ubaci u Wal
		app.DeleteFromCache(key)
		return true

	} else { //klju NE postoji u Memtable-u: traziti ga dalje

		isDeleted, foundLogCache := app.DeleteFromCache(key)
		if isDeleted { //ako je kljuc pronadjen u cache-u ukloniti ga odatle i dodati novi zapis u Memtable da je log obrisan
			foundLogCache.Tombstone = true
			app.Memtable.Insert(foundLogCache, app.ConfigurationData.NumOfFiles, app.ConfigurationData.NumOfSummarySegmentLogs, app.ConfigurationData.NumOfFiles)
			wal.AppendToWal(app.WalFile, foundLogCache) //ubaci u Wal
			return true
		}

		path := "./Data/SSTables/" + strings.Title(app.ConfigurationData.NumOfFiles) + "/"

		foundLog = app.CheckSSTable(path, key)
		if foundLog != nil {
			foundLog.Tombstone = true
			app.Memtable.Insert(foundLog, app.ConfigurationData.NumOfFiles, app.ConfigurationData.NumOfSummarySegmentLogs, app.ConfigurationData.NumOfFiles)
			wal.AppendToWal(app.WalFile, foundLog) //ubaci u Wal
			return true
		}

		fmt.Println("Key not found")
		return false
	}

	return true
}

func (app *Application) DeleteFromCache(key string) (bool, *Log) {
	foundLog := app.CheckCache(key)
	if foundLog != nil {
		app.Cache.Delete(foundLog)
		return true, foundLog
	}

	return false, nil
}
