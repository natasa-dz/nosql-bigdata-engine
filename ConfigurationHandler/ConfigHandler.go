package ConfigurationHandler

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

const configFilePath = "NAiSP/ConfigurationHandler/config.json"

type ConfigHandler struct {
	MemtableStruct string  `json:"MemtableStruct"`
	SizeOfMemtable uint32  `json:"SizeOfMemtable"`
	Trashold       float64 `json:"Trashold"`
	//if memtable struct is btree
	BTreeDegree uint32 `json:"BTreeDegree"`
	//else struct == skipList
}

func UseCustomConfiguration() *ConfigHandler {
	file, err := os.Open(configFilePath)
	if err != nil {
		return nil
	}
	defer file.Close()

	jsonData, err := ioutil.ReadAll(file)
	if err != nil {
		return nil
	}

	var config ConfigHandler
	err = json.Unmarshal(jsonData, &config)
	if err != nil {
		return nil
	}
	return &config
}

func UseDefaultConfiguration() *ConfigHandler {
	config := ConfigHandler{MemtableStruct: "btree", SizeOfMemtable: 30, Trashold: 0.7, BTreeDegree: 2}
	return &config
}
