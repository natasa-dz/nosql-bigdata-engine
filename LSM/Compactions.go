package LSM

import (
	. "NAiSP/Log"
	. "NAiSP/SSTable"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

const (
	LEVEL_TRASHOLD = 5
)

func Merge(data1 []*Log, data2 []*Log) []*Log {
	data1Len := len(data1)
	data2Len := len(data2)
	var i int = 0
	var j int = 0
	mergedData := make([]*Log, 0, data1Len+data2Len)

	for i < data1Len && j < data2Len {
		if data1[i].Tombstone == true {
			i++
		} else if data2[j].Tombstone == true {
			j++
		} else if string(data1[i].Key) < string(data2[j].Key) {
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
		numbers := strings.Split(fileInfo.Name(), "-")
		fileLevel, err := strconv.Atoi(numbers[2])
		if err != nil {
			fmt.Println("Error, wrong file format:", err)
			return nil, err
		}
		if fileLevel == level && numbers[0] == "Data" {
			files = append(files, fileInfo.Name())
		}
	}

	return files, nil
}

func GetMaxGenerationFromLevel(dirPath string, level int) (int, error) {
	// Read the directory and get a list of file and folder names
	fileInfos, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return 0, err
	}
	maxGeneration := 0
	//find files and max generation from same level of LSM tree
	for _, fileInfo := range fileInfos {
		numbers := strings.Split(fileInfo.Name(), "-")
		generation, err := strconv.Atoi(numbers[1])
		fileLevel, err2 := strconv.Atoi(numbers[2])
		if err != nil || err2 != nil {
			fmt.Println("Error, wrong file format:", err, err2)
			return 0, err
		}
		if fileLevel == level && generation > maxGeneration {
			maxGeneration = generation
		}
	}

	return maxGeneration, nil
}
func DeleteFilesFromLevel(level int, sstableType string) {
	files, err := GetAllFilesFromLevel("./Data/SSTables/"+sstableType, level)
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := 0; i < len(files); i++ {
		err := os.Remove(files[i])
		if err != nil {
			fmt.Println("Error deleting the file:", err)
			return
		}

		fmt.Println("File deleted successfully.")
	}

}

func SizeTieredCompaction(level int, sstableType string) {
	files, err := GetAllFilesFromLevel("./Data/SSTables/"+sstableType, level)
	if err != nil {
		fmt.Println(err)
		return
	}
	var finalLogs []*Log
	for i := 0; i < len(files); i++ {
		file1, err := os.Open(files[i])
		if err != nil {
			fmt.Println("Error opening file:", err)
			return
		}
		tempLogs, _ := GetAllLogs(file1, sstableType)
		file1.Close()
		if i == 0 {
			finalLogs = tempLogs
			continue
		}
		finalLogs = Merge(finalLogs, tempLogs)
	}
	maxGeneration, _ := GetMaxGenerationFromLevel("./Data/SSTables/"+sstableType, level+1)
	BuildSSTable(finalLogs, maxGeneration+1, level+1, sstableType)
	DeleteFilesFromLevel(level, sstableType)
	if maxGeneration+1 == LEVEL_TRASHOLD {
		SizeTieredCompaction(level+1, sstableType)
	}
}
