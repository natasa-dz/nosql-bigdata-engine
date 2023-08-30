package main

import (
	application "NAiSP/Application"
	menu "NAiSP/Menu"
	test "NAiSP/Testing"
)

func main() {
	choiceOfConfig := menu.WriteAppInitializationMenu()
	app := application.InitializeApp(choiceOfConfig)
	//test.InitializeData(app)
	test.InitializeDataForCompaction(app)
	app.StartApp()
}
