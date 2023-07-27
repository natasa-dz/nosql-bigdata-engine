package main

import (
	log "NAiSP/Log"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"strings"
	"time"
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

var (
	walFile           *os.File
	bufferedRecords   []*log.Log
	bufferSize        int
	lowWaterMarkIndex int
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

//////////////////////////////////////////////////////////////////////

// Function to create a new WAL
func CreateWALInstance(tombstone bool, key, value []byte) *log.Log {
	crc := crc32.NewIEEE()

	timestamp := time.Now().UnixNano()
	b := make([]byte, log.TIMESTAMP_SIZE)
	binary.BigEndian.PutUint64(b, uint64(timestamp))
	crc.Write(b)

	b = make([]byte, log.TOMBSTONE_SIZE)
	if tombstone {
		b[0] = 1
	}
	crc.Write(b)

	keySize := uint64(len(key))
	b = make([]byte, log.KEY_SIZE_SIZE)
	binary.BigEndian.PutUint64(b, keySize)
	crc.Write(b)

	valueSize := uint64(len(value))
	b = make([]byte, log.VALUE_SIZE_SIZE)
	binary.BigEndian.PutUint64(b, valueSize)
	crc.Write(b)

	crc.Write(key)

	crc.Write(value)

	return &log.Log{
		CRC:       crc.Sum32(),
		Timestamp: timestamp,
		Tombstone: tombstone,
		KeySize:   int64(keySize),
		ValueSize: int64(valueSize),
		Key:       key,
		Value:     value,
	}
}

// Function to create a new WAL file and return its file handle
func CreateNewWAL() (*os.File, error) {
	// Get the list of existing WAL files to find the next available offset
	files, err := os.ReadDir("wal/")
	if err != nil {
		return nil, err
	}

	// Determine the next available offset for the new WAL file
	nextOffset := 1
	if len(files) > 0 {
		lastFilename := files[len(files)-1].Name()

		maxOffset, err := strconv.Atoi(strings.Split(lastFilename[:len(lastFilename)-4], "_")[1])
		if err != nil {
			return nil, err
		}
		nextOffset = maxOffset + 1
	}

	// Generate the new WAL filename based on the offset
	newFilename := fmt.Sprintf("wal/wal_%04d.log", nextOffset)

	// Create or open the new WAL file
	newFile, err := os.OpenFile(newFilename, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}

	// Optionally, initialize any necessary metadata in the new file here

	return newFile, nil
}

// Function to delete old segments from the WAL directory
func deleteSegments(lowWaterMark int) error {
	files, err := os.ReadDir("wal/")
	if err != nil {
		return err
	}

	// Filter segments that are older than the lowWaterMark
	var segmentsToDelete []os.DirEntry
	for _, file := range files {
		filename := file.Name()
		offsetStr := strings.TrimSuffix(filename, ".log")
		offset, err := strconv.Atoi(strings.Split(offsetStr, "_")[1]) // Extract the offset from the filename
		if err != nil {
			continue // Skip files with incorrect naming format
		}

		if offset < lowWaterMark {
			segmentsToDelete = append(segmentsToDelete, file)
		}
	}

	// Delete the segments
	for _, file := range segmentsToDelete {
		filename := "wal/" + file.Name()
		err := os.Remove(filename)
		if err != nil {
			return err
		}
		fmt.Println("Deleted segment:", filename)
	}

	// Rename the remaining segments to adjust offset numbers
	files, err = os.ReadDir("wal/")
	if err != nil {
		return err
	}

	for i, file := range files {
		filename := "wal/" + file.Name()
		newFilename := "wal/wal_" + fmt.Sprintf("%04d", i+1) + ".log"
		err := os.Rename(filename, newFilename)
		if err != nil {
			return err
		}
		fmt.Println("Renamed segment:", filename, "to", newFilename)
	}

	return nil
}

// Helper function to check if a file is in the slice of segmentsToDelete
func contains(slice []os.DirEntry, element os.DirEntry) (int, bool) {
	for i, item := range slice {
		if item == element {
			return i, true
		}
	}
	return -1, false
}

// AppendToWal appends the given WalRecord to the end of the WAL file.

func AppendToWal(walFile *os.File, record *log.Log) error {
	// Prepare the buffer to store the record data
	buf := new(bytes.Buffer)

	// Write CRC field (4 bytes) to the buffer
	if err := binary.Write(buf, binary.BigEndian, record.CRC); err != nil {
		return err
	}

	// Write Timestamp field (8 bytes) to the buffer
	if err := binary.Write(buf, binary.BigEndian, uint64(record.Timestamp)); err != nil {
		return err
	}

	// Write Tombstone field (1 byte) to the buffer
	if record.Tombstone {
		if err := buf.WriteByte(1); err != nil {
			return err
		}
	} else {
		if err := buf.WriteByte(0); err != nil {
			return err
		}
	}

	// Write Key Size field (8 bytes) to the buffer
	if err := binary.Write(buf, binary.BigEndian, uint64(record.KeySize)); err != nil {
		return err
	}

	// Write Value Size field (8 bytes) to the buffer
	if err := binary.Write(buf, binary.BigEndian, uint64(record.ValueSize)); err != nil {
		return err
	}

	// Write Key data to the buffer
	if _, err := buf.Write(record.Key); err != nil {
		return err
	}

	// Write Value data to the buffer
	if _, err := buf.Write(record.Value); err != nil {
		return err
	}

	// Seek to the end of the WAL file and append the record data
	_, err := walFile.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	if _, err := walFile.Write(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

/*
ReadNextRecordFromWal reads the next record from the WAL file and returns a *WalRecord.
If there are no more records, it returns nil and io.EOF error.
*/
func ReadNextRecordFromWal(walFile *os.File) (*log.Log, error) {

	// Read CRC field (4 bytes).
	crcData := make([]byte, log.CRC_SIZE)
	_, err := walFile.Read(crcData)
	if err != nil {
		return nil, err
	}

	// Read Timestamp field (8 bytes).
	timestampData := make([]byte, log.TIMESTAMP_SIZE)
	_, err = walFile.Read(timestampData)
	if err != nil {
		return nil, err
	}

	// Read Tombstone field (1 byte).
	tombstoneData := make([]byte, log.TOMBSTONE_SIZE)
	_, err = walFile.Read(tombstoneData)
	if err != nil {
		return nil, err
	}

	// Read Key Size field (8 bytes).
	keySizeData := make([]byte, log.KEY_SIZE_SIZE)
	_, err = walFile.Read(keySizeData)
	if err != nil {
		return nil, err
	}

	// Read Value Size field (8 bytes).
	valueSizeData := make([]byte, log.VALUE_SIZE_SIZE)
	_, err = walFile.Read(valueSizeData)
	if err != nil {
		return nil, err
	}

	// Parse the fields.
	crc := binary.BigEndian.Uint32(crcData)
	timestamp := int64(binary.BigEndian.Uint64(timestampData))
	tombstone := tombstoneData[0] == 1
	keySize := binary.BigEndian.Uint64(keySizeData)
	valueSize := binary.BigEndian.Uint64(valueSizeData)

	// Read Key data.
	keyData := make([]byte, keySize)
	_, err = walFile.Read(keyData)
	if err != nil {
		return nil, err
	}

	// Read Value data.
	valueData := make([]byte, valueSize)
	_, err = walFile.Read(valueData)
	if err != nil {
		return nil, err
	}

	// Create and return the WalRecord.
	return &log.Log{
		CRC:       crc,
		Timestamp: timestamp,
		Tombstone: tombstone,
		KeySize:   int64(keySize),
		ValueSize: int64(valueSize),
		Key:       keyData,
		Value:     valueData,
	}, nil
}

// ReadWal reads the whole WAL from the beginning to the end.
func ReadWal(walFile *os.File) ([]*log.Log, error) {
	var records []*log.Log

	// Rewind the WAL file to the beginning.
	_, err := walFile.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	for {
		record, err := ReadNextRecordFromWal(walFile)
		if err != nil {
			break // Reached the end of the WAL.
		}
		records = append(records, record)
	}

	return records, nil
}

/*func main() {
	err := os.Mkdir("wal", 0777)
	if err != nil && !os.IsExist(err) {
		log.Fatal("Failed to create 'wal' folder:", err)
	}

	// Create a new WAL instance
	key := []byte("sample_key")
	value := []byte("sample_value")
	walEntry := CreateWALInstance(false, key, value)

	// Create a new WAL file and append the WAL instance to it
	walFile, err := CreateNewWAL()
	if err != nil {
		log.Fatal("Failed to create new WAL:", err)
	}
	defer walFile.Close()

	err = AppendToWal(walFile, walEntry)
	if err != nil {
		log.Fatal("Failed to append to WAL:", err)
	}

	// Read the entire WAL file
	records, err := ReadWal(walFile)
	if err != nil {
		log.Fatal("Failed to read WAL:", err)
	}

	// Print the records read from the WAL file
	fmt.Println("WAL Records:")
	for _, record := range records {
		fmt.Printf("CRC: %d, Timestamp: %d, Tombstone: %t, KeySize: %d, ValueSize: %d, Key: %s, Value: %s\n",
			record.CRC, record.Timestamp, record.Tombstone, record.KeySize, record.ValueSize, record.Key, record.Value)
	}

	// Close the WAL file before deleting old segments
	walFile.Close()

	// Set a low watermark for deleting old segments (e.g., 3)

	lowWaterMark := 8

	err = deleteSegments(lowWaterMark)

	if err != nil {
		log.Fatal("Failed to delete old segments:", err)
	}

}*/
