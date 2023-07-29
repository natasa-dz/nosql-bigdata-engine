package LSM

import (
	. "NAiSP/Log"
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

/*func getAllFilesInDirectory(dirPath string) ([]string, error) {
	var files []string

	// Read the directory and get a list of file and folder names
	fileInfos, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	// Iterate over the fileInfos and add file names to the files slice
	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() {
			files = append(files, fileInfo.Name())
		}
	}

	return files, nil
}

func CompactLevel(level int) {

}*/
