package SSTable

import (
	. "NAiSP/Log"
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

type IndexEntry struct {
	KeySize uint64
	Key     string
	Offset  uint64
}

type Index struct {
	Entries []IndexEntry
}

func BuildIndex(logs []*Log, initialOffset uint64) []*IndexEntry {
	// Create the Index entries
	indexEntries := make([]*IndexEntry, len(logs))
	var offset = initialOffset

	for i, log := range logs {

		indexEntries[i] = &IndexEntry{
			KeySize: uint64(log.KeySize),
			Key:     string(log.Key),
			Offset:  offset,
		}
		// Calculate the offset for the next entry
		offset += uint64(len(log.Serialize()))
	}

	return indexEntries
}

func (index IndexEntry) SerializeIndexEntry() []byte {
	var serializedIndex = new(bytes.Buffer)

	binary.Write(serializedIndex, binary.LittleEndian, index.KeySize)
	binary.Write(serializedIndex, binary.LittleEndian, []byte(index.Key))
	binary.Write(serializedIndex, binary.LittleEndian, index.Offset)
	return serializedIndex.Bytes()
}

func ReadIndexEntry(file *os.File, offset int64) (*IndexEntry, error) {
	var indexEntry IndexEntry
	file.Seek(offset, io.SeekStart)
	// Read keysize
	var keySizeBytes = make([]byte, KEY_SIZE_SIZE)
	_, err := file.Read(keySizeBytes)
	if err != nil {
		return nil, err
	}
	indexEntry.KeySize = uint64(binary.LittleEndian.Uint64(keySizeBytes))

	// Read Key
	var keyBytes = make([]byte, indexEntry.KeySize)
	_, err = file.Read(keyBytes)
	if err != nil {
		return nil, err
	}
	indexEntry.Key = string(keyBytes)

	// Read offset
	var offsetBytes = make([]byte, KEY_SIZE_SIZE)
	_, err = file.Read(offsetBytes)
	if err != nil {
		return nil, err
	}
	indexEntry.Offset = uint64(binary.LittleEndian.Uint64(offsetBytes))
	return &indexEntry, nil
}
func ReadIndex(file *os.File, offset int64, offsetEnd int64) ([]*IndexEntry, error) {
	file.Seek(offset, io.SeekStart)

	var data []*IndexEntry
	var loaded *IndexEntry

	//read until the end of logs
	for uint64(offset) < uint64(offsetEnd) {
		loaded, _ = ReadIndexEntry(file, offset)
		offset, _ = file.Seek(0, io.SeekCurrent)
		data = append(data, loaded)
	}

	return data, nil
}

func SerializeIndexes(Entries []*IndexEntry) []byte {
	var serializedIndexes = new(bytes.Buffer)
	for _, entry := range Entries {
		binary.Write(serializedIndexes, binary.LittleEndian, entry.KeySize)
		binary.Write(serializedIndexes, binary.LittleEndian, []byte(entry.Key))
		binary.Write(serializedIndexes, binary.LittleEndian, entry.Offset)
	}

	return serializedIndexes.Bytes()
}
