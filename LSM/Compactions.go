package LSM

import (
	. "NAiSP/Log"
	. "NAiSP/SSTable"
	"fmt"
	"io"
	"io/ioutil"
	//"log"
	"os"
	"strconv"
	"strings"
)

const (
	LEVEL_TRASHOLD = 3
)

func Merge(data1 []*Log, data2 []*Log) []*Log {
	data1Len := len(data1)
	data2Len := len(data2)
	var i int = 0
	var j int = 0
	mergedData := make([]*Log, 0, data1Len+data2Len)

	for i < data1Len && j < data2Len {
		fmt.Println(data1[i].Tombstone)
		fmt.Println(data2[j].Tombstone)
		if string(data1[i].Key) < string(data2[j].Key) {
			if data1[i].Tombstone == true {
				i++
			} else {
				mergedData = append(mergedData, data1[i])
				i++
			}
		} else if string(data1[i].Key) > string(data2[j].Key) {
			if data2[j].Tombstone == true {
				j++
			} else {
				mergedData = append(mergedData, data2[j])
				j++
			}
		} else {
			//ako su isti prepisuje onaj noviji log
			if data1[i].Timestamp > data2[j].Timestamp {
				if data1[i].Tombstone == true {
					//ako je noviji podatak da je log obrisan - preskoci oba
					i++
					j++
				} else {
					mergedData = append(mergedData, data1[i])
					i++
					j++
				}
			} else {
				if data2[j].Tombstone == true {
					//ako je noviji podatak da je log obrisan - preskoci oba
					i++
					j++
				} else {
					mergedData = append(mergedData, data2[j])
					j++
					i++
				}
			}
		}
	}
	// kopira ostatak iz data1 ako ima
	for i < data1Len {
		if data1[i].Tombstone == true {
			i++ //preskoci sve logove koji su obrisani
			continue
		}
		mergedData = append(mergedData, data1[i])
		i++
	}
	// kopira ostatak iz data2 ako ima
	for j < data2Len {
		if data2[j].Tombstone == true {
			j++ //preskoci sve logove koji su obrisani
			continue
		}
		mergedData = append(mergedData, data2[j])
		j++
	}
	return mergedData

}

func GetAllFilesFromLevel(dirPath string, level int, onlyData bool) ([]string, error) {
	var files []string

	// Read the directory and get a list of file and folder names
	fileInfos, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	//find files from same level of LSM tree
	for _, fileInfo := range fileInfos {
		numbers := strings.Split(fileInfo.Name(), "-")
		fileLevelSplit := strings.Split(numbers[2], ".")
		fileLevel, err := strconv.Atoi(fileLevelSplit[0])
		if err != nil {
			fmt.Println("Error, wrong file format:", err)
			return nil, err
		}
		if fileLevel == level {
			if onlyData {
				if numbers[0] == "Data" {
					files = append(files, fileInfo.Name())
				}
			} else {
				files = append(files, fileInfo.Name())
			}

		}
	}

	return files, nil
}

func GetMaxGenerationFromLevel(dirPath string, level int) (int, error) { //NOTE OVO
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
		fileLevelSplit := strings.Split(numbers[2], ".")
		fileLevel, err2 := strconv.Atoi(fileLevelSplit[0])
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
	files, err := GetAllFilesFromLevel("./Data/SSTables/"+sstableType, level, false)
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := 0; i < len(files); i++ {
		err := os.Remove("./Data/SSTables/" + sstableType + "/" + files[i])
		if err != nil {
			fmt.Println("Error deleting the file:", err)
			return
		}

		fmt.Println("File deleted successfully.")
	}

}

type FileInfo struct {
	File       *os.File
	CurrentLog *Log
	Header     *Header
}

func FindMinLog(filesInfo []*FileInfo) int {
	minLog := filesInfo[0].CurrentLog
	minIndex := 0
	for i := 0; i < len(filesInfo); i++ {
		if string(filesInfo[i].CurrentLog.Key) < string(minLog.Key) {
			minLog = filesInfo[i].CurrentLog
			minIndex = i
		} else if string(filesInfo[i].CurrentLog.Key) == string(minLog.Key) && filesInfo[i].CurrentLog.Timestamp > minLog.Timestamp {
			ReadLogSingle(filesInfo[minIndex])
			minLog = filesInfo[i].CurrentLog
			minIndex = i
		}
	}
	return minIndex
}

func IsLogsOffsetEnd(fileInfo *FileInfo) bool {
	offset, _ := fileInfo.File.Seek(0, io.SeekCurrent)
	if uint64(offset) == fileInfo.Header.IndexOffset {
		return true
	}
	return false
}

func ReadLogSingle(fileInfo *FileInfo) bool {
	var err error
	if !IsLogsOffsetEnd(fileInfo) {
		fileInfo.CurrentLog, err = ReadLog(fileInfo.File)
		if err != nil {
			fmt.Println("Error reading log:", fileInfo.CurrentLog.Key, err)
			return false
		}
		return true
	} else {
		fileInfo.CurrentLog = nil
		return false
	}
}
func removeNilElements(filesInfo []*FileInfo) []*FileInfo {
	result := make([]*FileInfo, 0, len(filesInfo))
	for _, value := range filesInfo {
		if value != nil {
			result = append(result, value)
		}
	}
	return result
}

func WriteLogToNewSSTable(file *os.File, log *Log) {

}

func MergeFiles(filesInfo []*FileInfo, file *os.File) {
	//ucitavanje prvog loga iz svakog SSTable
	for i := 0; i < len(filesInfo); i++ {
		ReadLogSingle(filesInfo[i])
	}

	for len(filesInfo) > 0 {
		minLogIndex := FindMinLog(filesInfo)
		WriteLogToNewSSTable(file, filesInfo[minLogIndex].CurrentLog)
		ReadLogSingle(filesInfo[minLogIndex])
		filesInfo = removeNilElements(filesInfo)
	}

}

/*
func SizeTieredCompactionSingle(level int, sstableType string) {
	files, err := GetAllFilesFromLevel("./Data/SSTables/"+sstableType, level, true)
	if err != nil {
		fmt.Println(err)
		return
	}

	var filesInfo []FileInfo

	for i := 0; i < len(files); i++ {
		filesInfo[i].File, err = os.Open("./Data/SSTables/" + sstableType + "/" + files[i])
		if err != nil {
			fmt.Println("Error opening file:", i, err)
			return
		}
		filesInfo[i].Header, err = ReadHeader(filesInfo[i].File)
		if err != nil {
			fmt.Println("Error reading file header:", i, err)
			return
		}
	}

	maxGeneration, _ := GetMaxGenerationFromLevel("./Data/SSTables/"+sstableType, level+1)

	WriteToFile(maxGeneration+1, level+1, "Data", sstableType, DataContent, &TOCData)
	file, err := os.OpenFile("./Data/SSTables/"+sstableType+"/"+"Data"+"-"+strconv.Itoa(maxGeneration)+"-"+strconv.Itoa(level+1)+".bin", os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Data to append to the file
	data := "This is the new content that will be appended.\n"

	// Write the data to the file
	_, err = file.WriteString(data)
	if err != nil {
		log.Fatal(err)
	}

	//BuildSSTable(finalLogs, maxGeneration+1, level+1, sstableType)
	for i := 0; i < len(files); i++ {
		filesInfo[i].File.Close()
	}
	DeleteFilesFromLevel(level, sstableType)
	if maxGeneration+1 == LEVEL_TRASHOLD {
		SizeTieredCompaction(level+1, sstableType)
	}
}*/
