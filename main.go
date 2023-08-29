package main

import (
	application "NAiSP/Application"
	menu "NAiSP/Menu"
)

func main() {
	choiceOfConfig := menu.WriteAppInitializationMenu()
	app := application.InitializeApp(choiceOfConfig)
	//test.InitializeData(app)
	app.StartApp()
}
