package SSTable

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

type Summary struct {
	StartKeySize uint64
	EndKeySize   uint64
	StartKey     string
	EndKey       string
	Entries      []*IndexEntry
}

func BuildSummary(data []*IndexEntry, indexOffset uint64, SUMMARY_BLOCK_SIZE int) *bytes.Buffer {
	var SummaryContent = new(bytes.Buffer)
	var offset = indexOffset
	WriteSummaryHeaderSingle(data, SummaryContent) //u summary ce ispisati prvi i poslednji kljuc iz indexa
	for i, entry := range data {
		if ((i+1)%SUMMARY_BLOCK_SIZE) == 0 || i == 0 {
			WriteSummaryLog(SummaryContent, data[i].KeySize, []byte(data[i].Key), offset)
		}
		offset += uint64(len(entry.SerializeIndexEntry()))
	}

	return SummaryContent
}
func WriteSummaryHeaderSingle(sortedData []*IndexEntry, SummaryContent *bytes.Buffer) {
	binary.Write(SummaryContent, binary.LittleEndian, sortedData[0].KeySize) //min key
	binary.Write(SummaryContent, binary.LittleEndian, []byte(sortedData[0].Key))
	binary.Write(SummaryContent, binary.LittleEndian, sortedData[len(sortedData)-1].KeySize) //max key
	binary.Write(SummaryContent, binary.LittleEndian, []byte(sortedData[len(sortedData)-1].Key))
}

func ReadSummary(file *os.File, offset int64) (*Summary, error) {
	offsetEnd, err := file.Seek(0, os.SEEK_END)
	file.Seek(offset, io.SeekStart)
	var startKeySize uint64
	var endKeySize uint64

	// Read the StartKeySize
	var keySizeBytes = make([]byte, 8)
	_, err = file.Read(keySizeBytes)
	if err != nil {
		return nil, err
	}
	startKeySize = uint64(binary.LittleEndian.Uint64(keySizeBytes))
	//fmt.Println(startKeySize)
	// Read the StartKey
	var startKeyBytes = make([]byte, startKeySize)
	_, err = file.Read(startKeyBytes)
	if err != nil {
		return nil, err
	}
	startKey := string(startKeyBytes)
	//fmt.Println(startKey)
	// Read the EndKeySize
	var endKeySizeBytes = make([]byte, 8)
	_, err = file.Read(endKeySizeBytes)
	if err != nil {
		return nil, err
	}
	endKeySize = uint64(binary.LittleEndian.Uint64(endKeySizeBytes))
	//fmt.Println(endKeySize)
	// Read the EndKey
	var endKeyBytes = make([]byte, endKeySize)
	_, err = file.Read(endKeyBytes)
	if err != nil {
		return nil, err
	}
	endKey := string(endKeyBytes)
	//fmt.Println(endKey)
	offset, _ = file.Seek(0, io.SeekCurrent)
	var data []*IndexEntry
	var loaded *IndexEntry

	//read until the end of logs
	for uint64(offset) < uint64(offsetEnd) {
		loaded, _ = ReadIndexEntry(file, offset)
		offset, _ = file.Seek(0, io.SeekCurrent)
		data = append(data, loaded)
	}
	// Create and return the Summary struct
	summary := &Summary{
		StartKeySize: startKeySize,
		EndKeySize:   endKeySize,
		StartKey:     startKey,
		EndKey:       endKey,
		Entries:      data,
	}

	return summary, nil
}

//TREBA ZA SEARCH (Natasine neke funkcije od ranije)
/*func IsKeyInSummary(key []byte, file *os.File, offset int64) bool {
	summary, err := ReadSummary(file, offset)

	if err != nil {
		fmt.Println("ERRR, error!")
		return false
	}

	if bytes.Compare(key, []byte(summary.StartKey)) >= 0 && bytes.Compare(key, []byte(summary.EndKey)) <= 0 {
		return true
	}
	return false
}*/

//treba za search isto
// FindIndexEntry finds the index entry for the given key in the SSTable file.
/*func FindIndexEntry(key []byte, file *os.File, offset int64) (*IndexEntry, error) {
	// Read the summary from the file
	summary, err := ReadSummary(file, offset)
	if err != nil {
		return nil, err
	}

	// Check if the key is within the boundaries of the SSTable
	if bytes.Compare(key, []byte(summary.StartKey)) < 0 || bytes.Compare(key, []byte(summary.EndKey)) > 0 {
		return nil, fmt.Errorf("key not found in SSTable")
	}

	// Get the size of the summary and indexes section in bytes
	summarySize := 16 + summary.StartKeySize + summary.EndKeySize
	file.Seek(-int64(summarySize), os.SEEK_END)

	// Read the serialized indexes from the file
	serializedIndexes := make([]byte, summarySize)
	_, err = file.Read(serializedIndexes)
	if err != nil {
		return nil, err
	}

	// Deserialize the summary and indexes
	summary = DeserializeSummary(serializedIndexes[:summarySize])
	indexes := DeserializeIndexes(serializedIndexes[summarySize:])

	// Use binary search to find the index entry with the closest key
	indexEntry := SearchIndexEntry(indexes.Entries, key)
	return indexEntry, nil
}*/

func SearchIndexEntry(entries []IndexEntry, key []byte) *IndexEntry {
	// Binary search implementation to find the closest index entry
	low, high := 0, len(entries)-1
	for low <= high {
		mid := (low + high) / 2
		currentKey := []byte(entries[mid].Key)

		if bytes.Compare(key, currentKey) == 0 {
			// Found an exact match
			return &entries[mid]
		} else if bytes.Compare(key, currentKey) < 0 {
			// Key is smaller, search in the left half
			high = mid - 1
		} else {
			// Key is larger, search in the right half
			low = mid + 1
		}
	}

	// If the loop terminates without finding an exact match, 'low' will point to the closest larger element.
	// We return the previous index entry as the closest match.
	return &entries[low-1]
}
