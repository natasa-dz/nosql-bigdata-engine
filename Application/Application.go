package Application

import (
	config "NAiSP/ConfigurationHandler"
	. "NAiSP/Log"
	memtable "NAiSP/MemTable"
	menu "NAiSP/Menu"
	wal "NAiSP/WriteAheadLog"
	"os"
)

type Application struct {
	ConfigurationData *config.ConfigHandler
	Memtable          *memtable.Memtable
	WalFile           *os.File
	//falice ovde cache i token baket, ali to cemo kasnije
}

func InitializeApp(choice string) *Application {
	var app Application
	if choice == "CUSTOM" {
		app = Application{ConfigurationData: config.UseCustomConfiguration()}
		app.Memtable = memtable.GenerateMemtable(app.ConfigurationData.SizeOfMemtable, app.ConfigurationData.Trashold, app.ConfigurationData.MemtableStruct)
		app.WalFile, _ = wal.CreateNewWAL()
	} else {
		app = Application{ConfigurationData: config.UseDefaultConfiguration()}
		app.Memtable = memtable.GenerateMemtable(app.ConfigurationData.SizeOfMemtable, app.ConfigurationData.Trashold, app.ConfigurationData.MemtableStruct)
		app.WalFile, _ = wal.CreateNewWAL()
	}
	return &app
}
func (app *Application) StartApp() {
	var userInput string
	for userInput != "X" {
		userInput = menu.WriteMainMenu()
		if userInput == "1" {
			key, value := menu.PUT_Menu()        //iz menija uzmi vrednosti
			newLog := CreateLog(key, value)      //pravi log
			wal.AppendToWal(app.WalFile, newLog) //ubaci u wal
			app.Memtable.Insert(*newLog)         //ubaci u memtable
		}
	}
}
