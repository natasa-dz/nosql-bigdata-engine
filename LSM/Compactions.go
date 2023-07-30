package LSM

import (
	. "NAiSP/Log"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
)

func Merge(data1 []Log, data2 []Log) []Log {
	data1Len := len(data1)
	data2Len := len(data2)
	var i int = 0
	var j int = 0
	mergedData := make([]Log, 0, data1Len+data2Len)

	for i < data1Len && j < data2Len {
		if string(data1[i].Key) < string(data2[j].Key) {
			mergedData = append(mergedData, data1[i])
			i++
		} else if string(data1[i].Key) > string(data2[j].Key) {
			mergedData = append(mergedData, data2[j])
			j++
		} else {
			//ako su isti prepisuje onaj noviji log
			if data1[i].Timestamp > data2[j].Timestamp {
				mergedData = append(mergedData, data1[i])
				i++
			} else {
				mergedData = append(mergedData, data2[j])
				j++
			}
		}
	}
	// kopira ostatak iz data1 ako ima
	for i < data1Len {
		mergedData = append(mergedData, data1[i])
		i++
	}
	// kopira ostatak iz data2 ako ima
	for j < data2Len {
		mergedData = append(mergedData, data2[j])
		j++
	}
	return mergedData

}

func GetAllFilesFromLevel(dirPath string, level int) ([]string, error) {
	var files []string

	// Read the directory and get a list of file and folder names
	fileInfos, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	//find files from same level of LSM tree
	for _, fileInfo := range fileInfos {
		numbers := strings.Split(fileInfo.Name(), "_")
		fileLevel, err := strconv.Atoi(numbers[1])
		if err != nil {
			fmt.Println("Error, wrong file format:", err)
			return nil, err
		}
		if !fileInfo.IsDir() && fileLevel == level {
			files = append(files, fileInfo.Name())
		}
	}

	return files, nil
}

func CompactLevel(level int) {

}
