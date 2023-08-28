package Application

import (
	cache "NAiSP/Cache"
	config "NAiSP/ConfigurationHandler"
	"NAiSP/LSM"
	. "NAiSP/Log"
	memtable "NAiSP/MemTable"
	menu "NAiSP/Menu"
	bucket "NAiSP/TokenBucket"
	wal "NAiSP/WriteAheadLog"
	"fmt"
	"os"
	"time"
)

type Application struct {
	ConfigurationData *config.ConfigHandler
	Memtable          *memtable.Memtable
	WalFile           *os.File
	TokenBucket       *bucket.TockenBucket
	Cache             *cache.LRUCache
	NumOfWalInserts   int //brojcanik za koliko smo logova bacili u 1 Wal
}

func InitializeApp(choice string) *Application {
	var app Application
	if choice == "CUSTOM" {
		app = Application{ConfigurationData: config.UseCustomConfiguration()}
	} else {
		app = Application{ConfigurationData: config.UseDefaultConfiguration()}
	}
	app.NumOfWalInserts = 0
	app.Memtable = memtable.GenerateMemtable(app.ConfigurationData.SizeOfMemtable, app.ConfigurationData.Trashold, app.ConfigurationData.MemtableStruct,
		int(app.ConfigurationData.BTreeDegree), int(app.ConfigurationData.SkipListMaxHeight))
	app.Cache = cache.CreateCache(app.ConfigurationData.CacheSize)
	app.Recover(app.ConfigurationData.NumOfFiles)
	app.WalFile, _ = wal.LoadLatestWAL(app.ConfigurationData.NumOfFiles)
	app.TokenBucket = bucket.CreateBucket(app.ConfigurationData.TokenBucketSize, time.Duration(app.ConfigurationData.TokenBucketRefreshTime))

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
				key, value := menu.PUT_Menu()                                                                                                                  //iz menija uzmi vrednosti
				newLog := CreateLog(key, value)                                                                                                                //pravi log
				wal.AppendToWal(app.WalFile, newLog)                                                                                                           //ubaci u Wal
				app.Memtable.Insert(newLog, app.ConfigurationData.NumOfFiles, app.ConfigurationData.NumOfSummarySegmentLogs, app.ConfigurationData.NumOfFiles) //ubaci u memtable
				app.Cache.Insert(newLog)                                                                                                                       //ubaci ga u cache
				app.NumOfWalInserts++
			} else {
				menu.OutOfTokensNotification()
			}
		} else if userInput == "2" {
			if app.TokenBucket.MakeRequest() {
				key := menu.GET_Menu()
				foundLog := app.Get(key)
				if foundLog != nil {
					fmt.Println("Found value: ", string(foundLog.Value))
					app.Cache.Insert(foundLog)
				}
			} else {
				menu.OutOfTokensNotification()
			}
		} else if userInput == "3" {
			key := menu.DELETE_Menu()
			isDeleted := app.Delete(key)
			if isDeleted {
				fmt.Println("Log is succesfuly deleted")
			} else {
				fmt.Println("Key is not deleted")
			}
		} else if userInput == "4" {
			prefix := menu.LIST_Menu()
			foundLogs := app.PrefixScan(prefix)

			menu.LIST_RANGESCAN_PaginationResponse(foundLogs, app.ConfigurationData.MenuPaginationSize)
			
		} else if userInput == "5" {
			minKey, maxKey := menu.RANGESCAN_Menu()
			foundLogs := app.RangeScan(minKey, maxKey)

			menu.LIST_RANGESCAN_PaginationResponse(foundLogs, app.ConfigurationData.MenuPaginationSize)

		} else if userInput == "6" {
			levelNum := menu.CompactionMenu(app.ConfigurationData.MaxNumOfLSMLevels-1, app.ConfigurationData.NumOfFiles)
			if levelNum == 0 {
				continue
			}
			if app.ConfigurationData.NumOfFiles == "single" {
				LSM.SizeTieredCompactionSingle(&levelNum, &app.ConfigurationData.NumOfSummarySegmentLogs, &app.ConfigurationData.MaxNumOfSSTablesPerLevel, &app.ConfigurationData.MaxNumOfLSMLevels)
			} else {
				LSM.SizeTieredCompactionMultiple(&levelNum, &app.ConfigurationData.NumOfSummarySegmentLogs, &app.ConfigurationData.MaxNumOfSSTablesPerLevel, &app.ConfigurationData.MaxNumOfLSMLevels)
			}
		}
	}

}

func (app *Application) changeWalFile() { //fja za promenu Wal file kad stigne do konfigurabilnog broja segmenata u sebi
	app.WalFile, _ = wal.CreateNewWAL(app.ConfigurationData.NumOfFiles)
	app.NumOfWalInserts = 0
}
