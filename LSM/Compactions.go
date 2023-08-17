package LSM

import (
	. "NAiSP/Log"
	. "NAiSP/SSTable"
	"fmt"
	"io"
	"io/ioutil"
	"log"

	. "NAiSP/BloomFilter"
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
	for i := 1; i < len(filesInfo); i++ {
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
	if uint64(offset) == fileInfo.Header.BloomOffset {
		return true
	}
	return false
}

func ReadLogSingle(fileInfo *FileInfo) bool {
	var err error
	if !IsLogsOffsetEnd(fileInfo) {
		fileInfo.CurrentLog, err = ReadLog(fileInfo.File)
		//ReadLog2(fileInfo.File, &fileInfo.CurrentLog)
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
func RemoveNilElements(filesInfo []*FileInfo) []*FileInfo {
	result := make([]*FileInfo, 0, len(filesInfo))
	for _, value := range filesInfo {
		if value.CurrentLog != nil {
			result = append(result, value)
		}
	}
	return result
}

func WriteLogToSingleSSTable(file *os.File, log *Log) *int64 {
	_, err := file.Write(log.Serialize())
	offsetEnd, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return nil
	}
	fmt.Println("ovde")
	return &offsetEnd
}
func WriteIndex(file *os.File, log *Log, logOffset *int64) {
	indexEntry := &IndexEntry{
		KeySize: uint64(log.KeySize),
		Key:     string(log.Key),
		Offset:  uint64(*logOffset),
	}
	_, err := file.Write(indexEntry.SerializeIndexEntry())
	if err != nil {
		return
	}
}

func MergeFiles(filesInfo []*FileInfo, mainFile *os.File, indexFile *os.File) *int {
	//ucitavanje prvog loga iz svakog SSTable
	for i := 0; i < len(filesInfo); i++ {
		filesInfo[i].File.Seek(int64(filesInfo[i].Header.LogsOffset), io.SeekStart)
		ReadLogSingle(filesInfo[i])
	}
	//upis praznog prostora za header
	data := make([]byte, 32)
	mainFile.Write(data)

	numOfElements := 0
	var currentLogOffset int64
	currentLogOffset = 32

	for len(filesInfo) > 0 {
		minLogIndex := FindMinLog(filesInfo)
		WriteIndex(indexFile, filesInfo[minLogIndex].CurrentLog, &currentLogOffset)
		currentLogOffset = *WriteLogToSingleSSTable(mainFile, filesInfo[minLogIndex].CurrentLog)
		ReadLogSingle(filesInfo[minLogIndex])
		filesInfo = RemoveNilElements(filesInfo)
		numOfElements++
	}
	return &numOfElements
}

func WriteBloom(mainFile *os.File, numOfLogs *int, offsetEnd *uint64) {
	bloom := Bloom2{}
	bloom.InitializeEmptyBloom2(*numOfLogs, 0.1)

	mainFile.Seek(32, io.SeekStart)
	var loaded *Log
	var offset int64
	offset = 32
	//read until the end of logs
	for offset < int64(*offsetEnd) {
		loaded, _ = ReadLog(mainFile)
		offset, _ = mainFile.Seek(0, io.SeekCurrent)
		bloom.Add(loaded.Key)
	}
	_, err := mainFile.Write(bloom.Serialize().Bytes())
	if err != nil {
		return
	}
}

func RewriteIndex(mainFile *os.File, level int, sstableType string, maxGeneration int) {
	data, err := ioutil.ReadFile("./Data/SSTables/" + sstableType + "/" + "Index" + "-" + strconv.Itoa(maxGeneration+1) + "-" + strconv.Itoa(level+1) + ".bin")
	if err != nil {
		log.Fatal(err)
	}
	_, err = mainFile.Write(data)
	if err != nil {
		return
	}
}

func SizeTieredCompactionSingle(level int, sstableType string) {
	files, err := GetAllFilesFromLevel("./Data/SSTables/"+sstableType, level, true)
	if err != nil {
		fmt.Println(err)
		return
	}

	filesInfo := make([]*FileInfo, len(files))

	for i := 0; i < len(files); i++ {
		fileInfo := FileInfo{}
		fileInfo.File, err = os.Open("./Data/SSTables/" + sstableType + "/" + files[i])

		if err != nil {
			fmt.Println("Error opening file:", i, err)
			return
		}
		fileInfo.Header, err = ReadHeader(fileInfo.File)
		if err != nil {
			fmt.Println("Error reading file header:", i, err)
			return
		}
		filesInfo[i] = &fileInfo
	}

	maxGeneration, _ := GetMaxGenerationFromLevel("./Data/SSTables/"+sstableType, level+1)

	mainFile, err := os.OpenFile("./Data/SSTables/"+sstableType+"/"+"Data"+"-"+strconv.Itoa(maxGeneration+1)+"-"+strconv.Itoa(level+1)+".bin", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer mainFile.Close()
	indexFile, err := os.OpenFile("./Data/SSTables/"+sstableType+"/"+"Index"+"-"+strconv.Itoa(maxGeneration+1)+"-"+strconv.Itoa(level+1)+".bin", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}
	header := Header{}
	_ = MergeFiles(filesInfo, mainFile, indexFile)
	OffsetEnd, _ := mainFile.Seek(0, os.SEEK_END)
	header.BloomOffset = uint64(OffsetEnd)

	/*WriteBloom(mainFile, numOfLogs, &header.BloomOffset)
	OffsetEnd, _ = mainFile.Seek(0, os.SEEK_END)
	header.IndexOffset = uint64(OffsetEnd)

	indexFile.Close()
	RewriteIndex(mainFile, level, sstableType, maxGeneration)
	OffsetEnd, _ = mainFile.Seek(0, os.SEEK_END)
	header.SummaryOffset = uint64(OffsetEnd)
	_, err = mainFile.WriteAt(header.HeaderSerialize(), 0)
	if err != nil {
		return
	}*/

	//write summary
	//TOC pokupiti

	for i := 0; i < len(files); i++ {
		filesInfo[i].File.Close()
	}
	//DeleteFilesFromLevel(level, sstableType)
	if maxGeneration+1 == LEVEL_TRASHOLD {
		SizeTieredCompactionSingle(level+1, sstableType)
	}
}
