package Application

import (
	cache "NAiSP/Cache"
	config "NAiSP/ConfigurationHandler"
	. "NAiSP/Log"
	memtable "NAiSP/MemTable"
	menu "NAiSP/Menu"
	bucket "NAiSP/TokenBucket"
	wal "NAiSP/WriteAheadLog"
	"os"
	"time"
)

type Application struct {
	ConfigurationData *config.ConfigHandler
	Memtable          *memtable.Memtable
	WalFile           *os.File
	TokenBucket       *bucket.TockenBucket
	Cache             *cache.LRUCache
	NumOfWalInserts   int //brojcanik za koliko smo logova bacili u 1 wal
}

func InitializeApp(choice string) *Application {
	var app Application
	if choice == "CUSTOM" {
		app = Application{ConfigurationData: config.UseCustomConfiguration()}
	} else {
		app = Application{ConfigurationData: config.UseDefaultConfiguration()}
	}
	app.NumOfWalInserts = 0
	app.Memtable = memtable.GenerateMemtable(app.ConfigurationData.SizeOfMemtable, app.ConfigurationData.Trashold, app.ConfigurationData.MemtableStruct, int(app.ConfigurationData.BTreeDegree))
	app.WalFile, _ = wal.CreateNewWAL()
	app.TokenBucket = bucket.CreateBucket(app.ConfigurationData.TokenBucketSize, time.Duration(app.ConfigurationData.TokenBucketRefreshTime))
	app.Cache = cache.CreateCache(app.ConfigurationData.CacheSize)
	return &app
}
func (app *Application) StartApp() {
	var userInput string
	for userInput != "X" {
		if app.NumOfWalInserts == app.ConfigurationData.NumOfWalSegmentLogs {
			app.changeWalFile()
		}
		userInput = menu.WriteMainMenu()
		if userInput == "1" {
			if app.TokenBucket.MakeRequest() { //proveri ima li slobodnih zahteva
				key, value := menu.PUT_Menu()                                                                                //iz menija uzmi vrednosti
				newLog := CreateLog(key, value)                                                                              //pravi log
				wal.AppendToWal(app.WalFile, newLog)                                                                         //ubaci u wal
				app.Memtable.Insert(newLog, app.ConfigurationData.NumOfFiles, app.ConfigurationData.NumOfSummarySegmentLogs) //ubaci u memtable
				app.Cache.Insert(newLog)                                                                                     //ubaci ga u cache
				app.NumOfWalInserts++
			} else {
				menu.OutOfTokensNotification()
			}
		}
	}
}

func (app *Application) changeWalFile() { //fja za promenu wal file kad stigne do konfigurabilnog broja segmenata u sebi
	app.WalFile, _ = wal.CreateNewWAL()
	app.NumOfWalInserts = 0
}
