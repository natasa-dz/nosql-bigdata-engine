package FileManager

import (
	"fmt"
	"os"
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

func GetValue(offset int64) {

}
