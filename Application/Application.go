package Application

import (
	config "NAiSP/ConfigurationHandler"
	. "NAiSP/Log"
	memtable "NAiSP/MemTable"
	menu "NAiSP/Menu"
)

type Application struct {
	ConfigurationData *config.ConfigHandler
	Memtable          *memtable.Memtable
	//falice ovde cache i token baket, ali to cemo kasnije
}

func InitializeApp(choice string) *Application {
	var app Application
	if choice == "CUSTOM" {
		app = Application{ConfigurationData: config.UseCustomConfiguration()}
		app.Memtable = memtable.GenerateMemtable(app.ConfigurationData.SizeOfMemtable, app.ConfigurationData.Trashold, app.ConfigurationData.MemtableStruct)

	} else {
		app = Application{ConfigurationData: config.UseDefaultConfiguration()}
		app.Memtable = memtable.GenerateMemtable(app.ConfigurationData.SizeOfMemtable, app.ConfigurationData.Trashold, app.ConfigurationData.MemtableStruct)

	}
	return &app
}
func (app *Application) StartApp() {
	var userInput string
	for userInput != "X" {
		userInput = menu.WriteMainMenu()
		if userInput == "1" {
			key, value := menu.PUT_Menu()
			newLog := CreateLog(key, value)
			app.Memtable.Insert(*newLog)
		}
	}
}
