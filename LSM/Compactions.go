package LSM

import (
	. "NAiSP/Log"
	. "NAiSP/SSTable"
	"bytes"
	"encoding/binary"
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

/*func Merge(data1 []*Log, data2 []*Log) []*Log {
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

}*/

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

func FindMinLog(filesInfo []*FileInfo, fileType string) int {
	minLog := filesInfo[0].CurrentLog
	minIndex := 0
	for i := 1; i < len(filesInfo); i++ {
		if string(filesInfo[i].CurrentLog.Key) < string(minLog.Key) {
			minLog = filesInfo[i].CurrentLog
			minIndex = i
		} else if string(filesInfo[i].CurrentLog.Key) == string(minLog.Key) {
			if filesInfo[i].CurrentLog.Timestamp > minLog.Timestamp {
				if fileType == "Single" {
					ReadLogSingle(filesInfo[minIndex])
				} else {
					ReadLogMultiple(filesInfo[minIndex])
				}
				minLog = filesInfo[i].CurrentLog
				minIndex = i
			} else {
				//vamo udje kad min>current.time a usput su keys jednaki
				if fileType == "Single" {
					ReadLogSingle(filesInfo[i])
				} else {
					ReadLogMultiple(filesInfo[i])
				}
			}

		}
	}
	return minIndex
}

func IsLogsOffsetEnd(fileInfo *FileInfo, endOffset int64) bool {
	offset, _ := fileInfo.File.Seek(0, io.SeekCurrent)
	if offset == endOffset {
		return true
	}
	return false
}

func ReadLogSingle(fileInfo *FileInfo) bool {
	var err error
	if !IsLogsOffsetEnd(fileInfo, int64(fileInfo.Header.BloomOffset)) {
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

func ReadLogMultiple(fileInfo *FileInfo) bool {
	var err error
	offsetTemp, _ := fileInfo.File.Seek(0, io.SeekCurrent)
	offsetEnd, err := fileInfo.File.Seek(0, os.SEEK_END)
	fileInfo.File.Seek(offsetTemp, io.SeekStart)
	if !IsLogsOffsetEnd(fileInfo, offsetEnd) {
		fileInfo.CurrentLog, err = ReadLog(fileInfo.File)
		if err != nil {
			fmt.Println("Error reading log:", fileInfo.CurrentLog.Key, err)
			return false
		}
		return true
	} else {
		fmt.Println("Nil", string(fileInfo.CurrentLog.Key))
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

func WriteLog(file *os.File, log *Log) *int64 {
	_, err := file.Write(log.Serialize())
	offsetEnd, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return nil
	}
	return &offsetEnd
}
func WriteIndexEntry(file *os.File, log *Log, logOffset *int64) {
	indexEntry := &IndexEntry{
		KeySize: uint64(log.KeySize),
		Key:     string(log.Key),
		Offset:  uint64(*logOffset),
	}
	fmt.Println(log.KeySize, string(log.Key), *logOffset)
	_, err := file.Write(indexEntry.SerializeIndexEntry())
	if err != nil {
		return
	}
}

func MergeFiles(filesInfo []*FileInfo, mainFile *os.File, indexFile *os.File, fileType string) *int {
	//load first log from every SSTable
	for i := 0; i < len(filesInfo); i++ {
		if fileType == "Single" {
			filesInfo[i].File.Seek(int64(filesInfo[i].Header.LogsOffset), io.SeekStart)
			ReadLogSingle(filesInfo[i])
		} else {
			ReadLogMultiple(filesInfo[i])
		}
	}
	var currentLogOffset int64
	numOfElements := 0
	if fileType == "Single" {
		//write empty bytes for header
		data := make([]byte, 32)
		mainFile.Write(data)
		currentLogOffset = 32
	} else {
		currentLogOffset = 0
	}

	for len(filesInfo) > 0 {
		minLogIndex := FindMinLog(filesInfo, fileType)
		WriteIndexEntry(indexFile, filesInfo[minLogIndex].CurrentLog, &currentLogOffset)
		currentLogOffset = *WriteLog(mainFile, filesInfo[minLogIndex].CurrentLog)
		if fileType == "Single" {
			ReadLogSingle(filesInfo[minLogIndex])
		} else {
			ReadLogMultiple(filesInfo[minLogIndex])
		}
		filesInfo = RemoveNilElements(filesInfo)
		numOfElements++
	}
	return &numOfElements
}

func WriteBloom(mainFile *os.File, bloomFile *os.File, numOfLogs *int, offsetEnd *uint64, fileType string) (*Log, *Log) {
	bloom := Bloom2{}
	bloom.InitializeEmptyBloom2(*numOfLogs, 0.1)

	counter := 1
	var offset int64
	if fileType == "Single" {
		offset, _ = mainFile.Seek(32, io.SeekStart)
	} else {
		offset, _ = mainFile.Seek(0, io.SeekStart)
	}
	var loaded *Log
	var firstLog *Log
	var lastLog *Log
	//read logs and create bloom filter
	for offset < int64(*offsetEnd) {
		loaded, _ = ReadLog(mainFile)
		if counter == 1 {
			firstLog = loaded
		}
		if counter == *numOfLogs {
			lastLog = loaded
		}
		offset, _ = mainFile.Seek(0, io.SeekCurrent)
		bloom.Add(loaded.Key)
		counter++
	}
	if fileType == "Single" {
		mainFile.Write(bloom.Serialize().Bytes())
	} else {
		bloomFile.Write(bloom.Serialize().Bytes())
	}
	return firstLog, lastLog
}

func RewriteIndex(mainFile *os.File, level *int, sstableType *string, maxGeneration *int) {
	data, err := ioutil.ReadFile("./Data/SSTables/" + *sstableType + "/" + "Index" + "-" + strconv.Itoa(*maxGeneration+1) + "-" + strconv.Itoa(*level+1) + ".bin")
	if err != nil {
		log.Fatal(err)
	}
	_, err = mainFile.Write(data)
	if err != nil {
		return
	}
}

func WriteSummaryHeaderCompaction(mainFile *os.File, firstLog *Log, lastLog *Log, startOffset uint64) {
	mainFile.Seek(int64(startOffset), io.SeekStart)
	var SummaryContent = new(bytes.Buffer)
	binary.Write(SummaryContent, binary.LittleEndian, firstLog.KeySize) //min key
	binary.Write(SummaryContent, binary.LittleEndian, firstLog.Key)
	binary.Write(SummaryContent, binary.LittleEndian, lastLog.KeySize) //max key
	binary.Write(SummaryContent, binary.LittleEndian, lastLog.Key)
	mainFile.Write(SummaryContent.Bytes())
}

func WriteSummarySingle(mainFile *os.File, header *Header, summaryBlockSize int, firstLog *Log, lastLog *Log) {

	WriteSummaryHeaderCompaction(mainFile, firstLog, lastLog, header.SummaryOffset)

	counter := 1
	var loaded *IndexEntry
	offset, _ := mainFile.Seek(int64(header.IndexOffset), io.SeekStart)

	//read index offsets and write to summary
	for uint64(offset) < header.SummaryOffset {
		loaded, _ = ReadIndexEntry(mainFile, offset)
		offsetTemp, _ := mainFile.Seek(0, io.SeekCurrent)

		if (counter % summaryBlockSize) == 0 {
			mainFile.Seek(0, io.SeekEnd)
			loaded.Offset = uint64(offset)
			mainFile.Write(loaded.SerializeIndexEntry())
		}
		offset, _ = mainFile.Seek(offsetTemp, io.SeekStart)

		counter++
	}
}
func WriteSummaryMultiple(indexFile *os.File, summaryFile *os.File, summaryBlockSize int, firstLog *Log, lastLog *Log) {

	WriteSummaryHeaderCompaction(summaryFile, firstLog, lastLog, 0)

	counter := 1
	var loaded *IndexEntry
	offsetEnd, _ := indexFile.Seek(0, io.SeekEnd)
	offset, _ := indexFile.Seek(0, io.SeekStart)

	//read index offsets and write to summary
	for offset < offsetEnd {
		loaded, _ = ReadIndexEntry(indexFile, offset)

		if (counter%summaryBlockSize) == 0 || counter == 1 {
			loaded.Offset = uint64(offset)
			summaryFile.Write(loaded.SerializeIndexEntry())
		}
		offset, _ = indexFile.Seek(0, io.SeekCurrent)

		counter++
	}
}

func LoadFilesFromLevel(level *int, sstableType *string) []*FileInfo {
	files, err := GetAllFilesFromLevel("./Data/SSTables/"+*sstableType, *level, true)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	//open files and load their headers
	filesInfo := make([]*FileInfo, len(files))
	for i := 0; i < len(files); i++ {
		fmt.Println("File- ", files[i])
		fileInfo := FileInfo{}
		fileInfo.File, err = os.Open("./Data/SSTables/" + *sstableType + "/" + files[i])
		if err != nil {
			fmt.Println("Error opening file:", i, err)
			return nil
		}
		if *sstableType == "Single" {
			fileInfo.Header, err = ReadHeader(fileInfo.File)
			if err != nil {
				fmt.Println("Error reading file header:", i, err)
				return nil
			}
		}

		filesInfo[i] = &fileInfo
	}
	return filesInfo
}

func OpenDataAndIndexFiles(level *int, sstableType *string, maxGeneration *int) (*os.File, *os.File) {
	mainFile, err := os.OpenFile("./Data/SSTables/"+*sstableType+"/"+"Data"+"-"+strconv.Itoa(*maxGeneration+1)+"-"+strconv.Itoa(*level+1)+".bin", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}

	indexFile, err := os.OpenFile("./Data/SSTables/"+*sstableType+"/"+"Index"+"-"+strconv.Itoa(*maxGeneration+1)+"-"+strconv.Itoa(*level+1)+".bin", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}
	return mainFile, indexFile
}
func OpenFile(level *int, sstableType *string, maxGeneration *int, fileType string, TOCData *string) *os.File {
	mainFile, err := os.OpenFile("./Data/SSTables/"+*sstableType+"/"+fileType+"-"+strconv.Itoa(*maxGeneration+1)+"-"+strconv.Itoa(*level+1)+".bin", os.O_RDWR|os.O_CREATE, 0666)
	*TOCData += "./Data/SSTables/" + *sstableType + "/" + fileType + "-" + strconv.Itoa(*maxGeneration+1) + "-" + strconv.Itoa(*level+1) + ".bin\n"
	if err != nil {
		log.Fatal(err)
	}
	return mainFile
}
func OpenFileWrite(level *int, sstableType *string, maxGeneration *int, fileType string, TOCData *string) *os.File {
	mainFile, err := os.OpenFile("./Data/SSTables/"+*sstableType+"/"+fileType+"-"+strconv.Itoa(*maxGeneration+1)+"-"+strconv.Itoa(*level+1)+".bin", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	*TOCData += "./Data/SSTables/" + *sstableType + "/" + fileType + "-" + strconv.Itoa(*maxGeneration+1) + "-" + strconv.Itoa(*level+1) + ".bin\n"
	if err != nil {
		log.Fatal(err)
	}
	return mainFile
}

func SizeTieredCompactionMultiple(level *int, sstableType *string, summaryBlockSize *int) {
	filesInfo := LoadFilesFromLevel(level, sstableType)
	maxGeneration, _ := GetMaxGenerationFromLevel("./Data/SSTables/"+*sstableType, *level+1)
	TOCData := ""
	dataFile := OpenFile(level, sstableType, &maxGeneration, "Data", &TOCData)
	indexFile := OpenFile(level, sstableType, &maxGeneration, "Index", &TOCData)
	summaryFile := OpenFile(level, sstableType, &maxGeneration, "Summary", &TOCData)
	bloomFile := OpenFile(level, sstableType, &maxGeneration, "Bloom", &TOCData)
	//metadataFile := OpenFile(level, sstableType, &maxGeneration, "Metadata", &TOCData)

	numOfLogs := MergeFiles(filesInfo, dataFile, indexFile, "Multiple")

	offsetEnd, _ := dataFile.Seek(0, os.SEEK_END)
	offsetEndUint64 := uint64(offsetEnd)
	firstLog, lastLog := WriteBloom(dataFile, bloomFile, numOfLogs, &offsetEndUint64, "Multiple")

	fmt.Println("first", string(firstLog.Key))
	fmt.Println("last", string(lastLog.Key))
	WriteSummaryMultiple(indexFile, summaryFile, *summaryBlockSize, firstLog, lastLog)

	//metadata

	for i := 0; i < len(filesInfo); i++ {
		filesInfo[i].File.Close()
	}
	WriteToTxtFile(maxGeneration+1, *level+1, "TOC", *sstableType, TOCData, nil)

	//DeleteFilesFromLevel(*level, *sstableType)
	if maxGeneration+1 == LEVEL_TRASHOLD {
		*level++
		SizeTieredCompactionMultiple(level, sstableType, summaryBlockSize)
	}
}

func SizeTieredCompactionSingle(level *int, sstableType *string, summaryBlockSize *int) {
	filesInfo := LoadFilesFromLevel(level, sstableType)
	maxGeneration, _ := GetMaxGenerationFromLevel("./Data/SSTables/"+*sstableType, *level+1)
	mainFile, indexFile := OpenDataAndIndexFiles(level, sstableType, &maxGeneration)
	TOCData := "./Data/SSTables/" + *sstableType + "/" + "Data" + "-" + strconv.Itoa(maxGeneration+1) + "-" + strconv.Itoa(*level+1) + ".bin\n"
	TOCData += "./Data/SSTables/" + *sstableType + "/" + "Metadata" + "-" + strconv.Itoa(maxGeneration+1) + "-" + strconv.Itoa(*level+1) + ".bin\n"

	header := Header{}
	header.LogsOffset = 32
	numOfLogs := MergeFiles(filesInfo, mainFile, indexFile, "Single")
	OffsetEnd, _ := mainFile.Seek(0, os.SEEK_END)
	header.BloomOffset = uint64(OffsetEnd)

	firstLog, lastLog := WriteBloom(mainFile, nil, numOfLogs, &header.BloomOffset, "Single")
	OffsetEnd, _ = mainFile.Seek(0, os.SEEK_END)
	header.IndexOffset = uint64(OffsetEnd)

	indexFile.Close()
	RewriteIndex(mainFile, level, sstableType, &maxGeneration)
	OffsetEnd, _ = mainFile.Seek(0, os.SEEK_END)
	header.SummaryOffset = uint64(OffsetEnd)

	mainFile.Close()
	mainFile, _ = os.OpenFile("./Data/SSTables/"+*sstableType+"/"+"Data"+"-"+strconv.Itoa(maxGeneration+1)+"-"+strconv.Itoa(*level+1)+".bin", os.O_RDWR, 0666)
	mainFile.Seek(0, 0)
	mainFile.Write(header.HeaderSerialize())

	WriteSummarySingle(mainFile, &header, *summaryBlockSize, firstLog, lastLog)

	//metadata

	for i := 0; i < len(filesInfo); i++ {
		filesInfo[i].File.Close()
	}
	WriteToTxtFile(maxGeneration+1, *level+1, "TOC", *sstableType, TOCData, nil)
	os.Remove("./Data/SSTables/" + *sstableType + "/" + "Index" + "-" + strconv.Itoa(maxGeneration+1) + "-" + strconv.Itoa(*level+1) + ".bin")
	//DeleteFilesFromLevel(*level, *sstableType)
	if maxGeneration+1 == LEVEL_TRASHOLD {
		*level++
		SizeTieredCompactionSingle(level, sstableType, summaryBlockSize)
	}
}
