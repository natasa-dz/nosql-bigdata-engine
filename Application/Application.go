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
}

func InitializeApp(choice string) *Application {
	var app Application
	if choice == "CUSTOM" {
		app = Application{ConfigurationData: config.UseCustomConfiguration()}
	} else {
		app = Application{ConfigurationData: config.UseDefaultConfiguration()}
	}
	app.Memtable = memtable.GenerateMemtable(app.ConfigurationData.SizeOfMemtable, app.ConfigurationData.Trashold, app.ConfigurationData.MemtableStruct, int(app.ConfigurationData.BTreeDegree))
	app.WalFile, _ = wal.CreateNewWAL() //todo: resiti stvari koje treba kroz konfiguraciju da se gledaju sto se tice wal-a
	app.TokenBucket = bucket.CreateBucket(app.ConfigurationData.TokenBucketSize, time.Duration(app.ConfigurationData.TokenBucketRefreshTime))
	app.Cache = cache.CreateCache(app.ConfigurationData.CacheSize)
	return &app
}
func (app *Application) StartApp() {
	var userInput string
	for userInput != "X" {
		userInput = menu.WriteMainMenu()
		if userInput == "1" {
			if app.TokenBucket.MakeRequest() { //proveri ima li slobodnih zahteva
				key, value := menu.PUT_Menu()                                  //iz menija uzmi vrednosti
				newLog := CreateLog(key, value)                                //pravi log
				wal.AppendToWal(app.WalFile, newLog)                           //ubaci u wal
				app.Memtable.Insert(*newLog, app.ConfigurationData.NumOfFiles) //ubaci u memtable
				app.Cache.Insert(newLog)                                       //ubaci ga u cache
			} else {
				menu.OutOfTokensNotification()
			}
		}
	}
}
