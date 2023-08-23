package ConfigurationHandler

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

const configFilePath = "ConfigurationHandler/config.json"

type ConfigHandler struct {
	NumOfWalSegmentLogs     int     `json:"NumOfWalSegmentLogs"`
	NumOfSummarySegmentLogs int     `json:"NumOfSummarySegmentLogs"`
	MemtableStruct          string  `json:"MemtableStruct"`
	SizeOfMemtable          uint32  `json:"SizeOfMemtable"`
	Trashold                float64 `json:"Trashold"`
	NumOfFiles              string  `json:"NumOfFiles"`
	//if memtable struct is btree
	BTreeDegree uint32 `json:"BTreeDegree"`
	//else struct == skipList(onda mi trebaju elementi za skiplist kao sto za btree imam njegov degree
	SkipListMaxHeight        int `json:"SkipListMaxHeight"`
	CacheSize                int `json:"CacheSize"`
	TokenBucketSize          int `json:"TokenBucketSize"`
	TokenBucketRefreshTime   int `json:"TokenBucketRefreshTime"`
	MenuPaginationSize       int `json:"MenuPaginationSize"`
	MaxNumOfLSMLevels        int `json:"MaxNumOfLSMLevels"`
	MaxNumOfSSTablesPerLevel int `json:"MaxNumOfSSTablesPerLevel"`
}

func UseCustomConfiguration() *ConfigHandler {
	file, err := os.Open(configFilePath)
	if err != nil {
		fmt.Println("Err opening json file")
		return nil
	}
	defer file.Close()

	jsonData, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("Err reading json file")
		return nil
	}

	var config ConfigHandler
	err = json.Unmarshal(jsonData, &config)
	if err != nil {
		fmt.Println("Error unmarshaling json file")
		return nil
	}
	return &config
}

func UseDefaultConfiguration() *ConfigHandler {
	config := ConfigHandler{NumOfWalSegmentLogs: 2, NumOfSummarySegmentLogs: 10, MemtableStruct: "btree", SizeOfMemtable: 5, Trashold: 0.8, BTreeDegree: 2, SkipListMaxHeight: 10, NumOfFiles: "multiple",
		TokenBucketSize: 3, TokenBucketRefreshTime: 10000, CacheSize: 4, MenuPaginationSize: 3, MaxNumOfLSMLevels: 4, MaxNumOfSSTablesPerLevel: 2}
	return &config
}
