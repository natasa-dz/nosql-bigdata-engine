package SSTable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
)

type Summary struct {
	StartKeySize uint64
	EndKeySize   uint64
	StartKey     string
	EndKey       string
	Indexes      Index
}

// buildSummary constructs the Summary from the provided Data slice.

// pretpostavka da imamo data i da sadrzi sve info sto nam treba!
func buildSummary(data []IndexEntry) Summary {

	// Sort the Data slice based on keys to ensure it is properly ordered
	sort.Slice(data, func(i, j int) bool {
		return data[i].Key < data[j].Key
	})

	// Extract start and end keys from the sorted Data slice
	startKey := data[0].Key
	endKey := data[len(data)-1].Key
	startKeySize := uint64(len(startKey))
	endKeySize := uint64(len(endKey))

	// Create the Index struct from the sorted Data slice
	indexes := Index{Entries: data}

	// Construct and return the Summary
	summary := Summary{
		StartKeySize: startKeySize,
		EndKeySize:   endKeySize,
		StartKey:     startKey,
		EndKey:       endKey,
		Indexes:      indexes,
	}

	return summary
}

// writes the summary header containing the boundaries of the SSTable (first and last keys).
func SerializeSummary(f *os.File, summary Summary) error {

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
}

// deserializeSummary deserializes the serializedSummary byte slice into a Summary struct.
func DeserializeSummary(serializedSummary []byte) Summary {
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
}

func IsKeyInSummaryFile(key []byte, file *os.File) bool {
	summary, err := ReadSummary(file)

	if err != nil {
		fmt.Println("ERRR, error!")
		return false
	}

	if bytes.Compare(key, []byte(summary.StartKey)) >= 0 && bytes.Compare(key, []byte(summary.EndKey)) <= 0 {
		return true
	}
	return false
}

// readSummaryHeader reads the summary header from the file and returns the Summary struct.
func ReadSummary(file *os.File) (Summary, error) {
	var startKeySize uint64
	var endKeySize uint64

	// Read the StartKeySize
	if err := binary.Read(file, binary.LittleEndian, &startKeySize); err != nil {
		return Summary{}, err
	}

	// Read the EndKeySize
	if err := binary.Read(file, binary.LittleEndian, &endKeySize); err != nil {
		return Summary{}, err
	}

	// Read the StartKey
	startKeyBytes := make([]byte, startKeySize)
	if _, err := file.Read(startKeyBytes); err != nil {
		return Summary{}, err
	}
	startKey := string(startKeyBytes)

	// Read the EndKey
	endKeyBytes := make([]byte, endKeySize)
	if _, err := file.Read(endKeyBytes); err != nil {
		return Summary{}, err
	}
	endKey := string(endKeyBytes)

	// Create and return the Summary struct
	summary := Summary{
		StartKeySize: startKeySize,
		EndKeySize:   endKeySize,
		StartKey:     startKey,
		EndKey:       endKey,
	}

	return summary, nil
}
