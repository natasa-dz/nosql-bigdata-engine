package SSTable

import (
	"bytes"
	"encoding/binary"
	"fmt"
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

func BuildSummary(data []*IndexEntry, indexOffset uint64) *bytes.Buffer {
	var SummaryContent = new(bytes.Buffer)
	var offset = indexOffset
	fmt.Println("OFFSET", offset)
	WriteSummaryHeaderSingle(data, SummaryContent) //u summary ce ispisati prvi i poslednji kljuc iz indexa
	for i, entry := range data {
		if ((i+1)%SUMMARY_BLOCK_SIZE) == 0 || i == 0 {
			fmt.Println("OFFSET", offset)
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

// writes the summary header containing the boundaries of the SSTable (first and last keys).
/*func SerializeSummary(f *os.File, summary Summary) error {

	startKeySizeBytes := make([]byte, binary.Size(summary.StartKeySize))
	endKeySizeBytes := make([]byte, binary.Size(summary.EndKeySize))

	binary.LittleEndian.PutUint64(startKeySizeBytes, summary.StartKeySize)
	binary.LittleEndian.PutUint64(endKeySizeBytes, summary.EndKeySize)

	// Write the sizes of the first and last keys to the file
	_, err := f.Write(startKeySizeBytes)
	if err != nil {
		return err
	}

	_, err = f.Write(endKeySizeBytes)
	if err != nil {
		return err
	}

	// Write the first and last keys to the file
	_, err = f.WriteString(summary.StartKey)
	if err != nil {
		return err
	}

	_, err = f.WriteString(summary.EndKey)
	if err != nil {
		return err
	}

	return nil
}*/

/*func (summary Summary) Serialize() []byte {
	// Create a buffer to store the serialized data
	serializedSummary := new(bytes.Buffer)

	// Write the StartKeySize and EndKeySize to the buffer
	binary.Write(serializedSummary, binary.LittleEndian, summary.StartKeySize)
	binary.Write(serializedSummary, binary.LittleEndian, summary.EndKeySize)

	// Write the StartKey and EndKey to the buffer as bytes
	binary.Write(serializedSummary, binary.LittleEndian, []byte(summary.StartKey))
	binary.Write(serializedSummary, binary.LittleEndian, []byte(summary.EndKey))

	// Serialize the Indexes and append it to the buffer
	serializedIndexes := summary.Indexes.SerializeIndexes()
	serializedSummary.Write(serializedIndexes)

	return serializedSummary.Bytes()
}*/

// deserializeSummary deserializes the serializedSummary byte slice into a Summary struct.
/*func DeserializeSummary(serializedSummary []byte) Summary {

	var startKeySize = binary.LittleEndian.Uint64(serializedSummary[:8])
	var endKeySize = binary.LittleEndian.Uint64(serializedSummary[8:16])
	var startKey = string(serializedSummary[16 : 16+startKeySize])
	var endKey = string(serializedSummary[16+startKeySize : 16+startKeySize+endKeySize])

	// Calculate the offset for the Indexes data after startKey and endKey
	indexesOffset := 16 + startKeySize + endKeySize

	return Summary{
		StartKeySize: startKeySize,
		EndKeySize:   endKeySize,
		StartKey:     startKey,
		EndKey:       endKey,
		Indexes:      DeserializeIndexes(serializedSummary[indexesOffset:]),
	}
}*/
//treba za search
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

// readSummaryHeader reads the summary header from the file and returns the Summary struct.
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

	// Read the StartKey
	var startKeyBytes = make([]byte, startKeySize)
	_, err = file.Read(startKeyBytes)
	if err != nil {
		return nil, err
	}
	startKey := string(startKeyBytes)

	// Read the EndKeySize
	var endKeySizeBytes = make([]byte, 8)
	_, err = file.Read(endKeySizeBytes)
	if err != nil {
		return nil, err
	}
	endKeySize = uint64(binary.LittleEndian.Uint64(endKeySizeBytes))

	// Read the EndKey
	var endKeyBytes = make([]byte, endKeySize)
	_, err = file.Read(endKeyBytes)
	if err != nil {
		return nil, err
	}
	endKey := string(endKeyBytes)

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
