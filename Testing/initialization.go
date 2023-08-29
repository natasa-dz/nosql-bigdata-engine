package Testing

import (
	"NAiSP/Application"
	fileManager "NAiSP/FileManager"
	"NAiSP/LSM"
	. "NAiSP/Log"
	wal "NAiSP/WriteAheadLog"
	"time"
)

func InitializeDataForCompaction(app *Application.Application) {
	ResetDataFiles()
	duration := 2 * time.Second

	InsertData("Mexico", "Spanish", app)
	InsertData("Canada", "English, French", app)
	InsertData("Australia", "English", app)
	InsertData("Austria", "Germany", app)
	InsertData("United Kingdom", "English", app)
	InsertData("Nigeria", "English", app)
	InsertData("Pakistan", "Urdu", app)
	InsertData("South Africa", "Zulu, Afrikaans", app)
	InsertData("Indonesia", "Indonesian", app)
	InsertData("Netherlands", "Dutch", app)
	CompactData(1, app)
	time.Sleep(duration)
	InsertData("Nigeria", "xxx", app)
	InsertData("Mexico", "xxx", app)
	InsertData("South Africa", "xxx", app)
	InsertData("Indonesia", "xxx", app)
	InsertData("Netherlands", "xxx", app)
	InsertData("Colombia", "Spanish", app)
	InsertData("Malaysia", "Malay", app)
	InsertData("Philippines", "Filipino", app)
	InsertData("Vietnam", "Vietnamese", app)
	InsertData("Serbia", "Serbian", app)
	CompactData(1, app)
	//1-3
	time.Sleep(duration)
	InsertData("United States of America", "English", app)
	InsertData("Germany", "German", app)
	InsertData("France", "French", app)
	InsertData("Spain", "Spanish", app)
	InsertData("Italy", "Italian", app)
	InsertData("China", "Mandarin Chinese", app)
	InsertData("Japan", "Japanese", app)
	InsertData("Russia", "Russian", app)
	InsertData("Brazil", "Portuguese", app)
	InsertData("India", "Hindi", app)
	CompactData(1, app)
	//1-2
	time.Sleep(duration)
	InsertData("South Korea", "Korean", app)
	InsertData("Saudi Arabia", "Arabic", app)
	InsertData("Turkey", "Turkish", app)
	InsertData("Egypt", "Arabic", app)
	InsertData("Argentina", "Spanish", app)
	InsertData("Bosnia", "Bosnian", app)
	InsertData("Macedonia", "Macedonian", app)
	time.Sleep(duration)

}

func InitializeData(app *Application.Application) {
	ResetDataFiles()

	InsertData("United States of America", "English", app)
	InsertData("Germany", "German", app)
	InsertData("France", "French", app)
	InsertData("Spain", "Spanish", app)
	InsertData("Italy", "Italian", app)
	InsertData("China", "Mandarin Chinese", app)
	InsertData("Japan", "Japanese", app)
	InsertData("Russia", "Russian", app)
	InsertData("Brazil", "Portuguese", app)
	InsertData("India", "Hindi", app)
	InsertData("Mexico", "Spanish", app)
	InsertData("Canada", "English, French", app)
	InsertData("Australia", "English", app)
	InsertData("Austria", "Germany", app)
	InsertData("United Kingdom", "English", app)
	InsertData("South Korea", "Korean", app)
	InsertData("Saudi Arabia", "Arabic", app)
	InsertData("Turkey", "Turkish", app)
	InsertData("Egypt", "Arabic", app)
	InsertData("Argentina", "Spanish", app)
	InsertData("Nigeria", "English", app)
	InsertData("Pakistan", "Urdu", app)
	InsertData("South Africa", "Zulu, Afrikaans", app)
	InsertData("Indonesia", "Indonesian", app)
	InsertData("Netherlands", "Dutch", app)
	InsertData("Colombia", "Spanish", app)
	InsertData("Malaysia", "Malay", app)
	InsertData("Philippines", "Filipino", app)
	InsertData("Vietnam", "Vietnamese", app)

}

func ResetDataFiles() {

	ssTablePathSingle := "./Data/SSTables/Single/"
	ssTablePathMultiple := "./Data/SSTables/Multiple/"
	walPathSingle := "./Data/Wal/Single/"
	walPathMultiple := "./Data/Wal/Multiple/"

	fileManager.RemoveFilesFromDir(ssTablePathSingle)
	fileManager.RemoveFilesFromDir(ssTablePathMultiple)
	fileManager.RemoveFilesFromDir(walPathSingle)
	fileManager.RemoveFilesFromDir(walPathMultiple)

}

func InsertData(key, value string, app *Application.Application) {
	newLog := CreateLog([]byte(key), []byte(value))                                                                                                //pravi log
	wal.AppendToWal(app.WalFile, newLog)                                                                                                           //ubaci u Wal
	app.Memtable.Insert(newLog, app.ConfigurationData.NumOfFiles, app.ConfigurationData.NumOfSummarySegmentLogs, app.ConfigurationData.NumOfFiles) //ubaci u memtable
	app.Cache.Insert(newLog)                                                                                                                       //ubaci ga u cache
	app.NumOfWalInserts++
}

func CompactData(level int, app *Application.Application) {
	if app.ConfigurationData.NumOfFiles == "single" {
		LSM.SizeTieredCompactionSingle(&level, &app.ConfigurationData.NumOfSummarySegmentLogs, &app.ConfigurationData.MaxNumOfSSTablesPerLevel, &app.ConfigurationData.MaxNumOfLSMLevels)
	} else {
		LSM.SizeTieredCompactionMultiple(&level, &app.ConfigurationData.NumOfSummarySegmentLogs, &app.ConfigurationData.MaxNumOfSSTablesPerLevel, &app.ConfigurationData.MaxNumOfLSMLevels)
	}
}
