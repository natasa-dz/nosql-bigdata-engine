package FileManager

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

func Open(path string) *os.File {
	file, err := os.Open(path)

	if err != nil {
		fmt.Print("Error opening file", path)
		return nil
	}

	return file
}

func GetFilesWithWord(directoryPath string, searchWord string) []string {
	var files []string

	directory, err := os.Open(directoryPath)

	if err != nil {
		fmt.Println("Error opening directory, ", err)
	}

	fileInfos, err := directory.ReadDir(-1)
	if err != nil {
		fmt.Println("Error reading directory contents ", err)
	}

	for _, info := range fileInfos {
		if !info.IsDir() && strings.Contains(info.Name(), searchWord) {
			files = append(files, info.Name())
		}
	}

	return files
}

func GetFilesFromDir(directoryPath string) []string {
	var files []string

	directory, err := os.Open(directoryPath)
	if err != nil {
		fmt.Println("Error opening directory, ", directoryPath)
		fmt.Println(err)
	}

	fileInfos, err := directory.ReadDir(-1)
	if err != nil {
		fmt.Println("Erro opening directory contents ", err)
	}

	for _, info := range fileInfos {
		if !info.IsDir() {
			files = append(files, info.Name())
		}
	}

	return files
}

type FileName struct {
	Name       string
	Generation int
	Level      int
}

type FileNamesSlice []FileName

func (s FileNamesSlice) Len() int { return len(s) }
func (s FileNamesSlice) Less(i, j int) bool {
	return s[i].Level < s[j].Level || s[i].Level == s[j].Level && s[i].Generation > s[j].Generation
}
func (s FileNamesSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func SerializeFileName(fileName FileName) string {
	return fileName.Name + "-" + strconv.Itoa(fileName.Generation) + "-" + strconv.Itoa(fileName.Level) + ".bin"
}

func DeserializeFileName(fileNameStr string) FileName {
	words := strings.Split(fileNameStr, "-")
	name := words[0]
	generation, _ := strconv.Atoi(words[1])
	splited := strings.Split(words[2], ".")
	level, _ := strconv.Atoi(splited[0])
	fileName := FileName{
		Name:       name,
		Generation: generation,
		Level:      level,
	}

	return fileName
}

func SortFileNames(fileNamesStr []string, ascending bool) []string { //ascending - true - newst first; descending - false - oldest first
	var fileNames FileNamesSlice

	for _, fn := range fileNamesStr {
		fileNames = append(fileNames, DeserializeFileName(fn))
	}

	var compareFunction func(i, j int) bool
	if ascending {
		compareFunction = func(i, j int) bool {
			return fileNames[i].Level < fileNames[j].Level || fileNames[i].Level == fileNames[j].Level && fileNames[i].Generation > fileNames[j].Generation
		}
	} else {
		compareFunction = func(i, j int) bool {
			return fileNames[i].Level > fileNames[j].Level || fileNames[i].Level == fileNames[j].Level && fileNames[i].Generation < fileNames[j].Generation
		}
	}

	sort.Slice(fileNames, compareFunction)

	var fileNamesStrRet []string
	for _, f := range fileNames {
		fileNamesStrRet = append(fileNamesStrRet, SerializeFileName(f))
	}

	return fileNamesStrRet
}
