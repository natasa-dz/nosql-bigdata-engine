package SSTable

import (
	. "NAiSP/Log"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"sort"
)

///////////////////////// INDEX

// pojedinacni index
type IndexEntry struct {
	KeySize uint64
	Key     string
	Offset  uint64
}

// lista index-a
type Index struct {
	Entries []IndexEntry
}

func BuildIndex(logs []*Log, initialOffset uint64) *Index {
	// Sort the logs by Key
	sort.Slice(logs, func(i, j int) bool {
		return string(logs[i].Key) < string(logs[j].Key)
	})

	// Create the Index entries
	indexEntries := make([]IndexEntry, len(logs))
	var offset = initialOffset

	for i, log := range logs {
		encodedKey := hex.EncodeToString([]byte(log.Key))
		keySize := uint64(len(encodedKey))

		indexEntries[i] = IndexEntry{
			KeySize: keySize,
			Key:     encodedKey,
			Offset:  offset,
		}

		// Calculate the offset for the next entry
		offset += 8 + keySize + 8 // 8 bytes for each uint64 field
	}

	// Create the Index object
	index := &Index{
		Entries: indexEntries,
	}

	return index
}

// Serialize an IndexEntry to bytes
func (index IndexEntry) SerializeIndexEntry() []byte {
	var serializedIndex = new(bytes.Buffer)

	binary.Write(serializedIndex, binary.LittleEndian, index.KeySize)
	binary.Write(serializedIndex, binary.LittleEndian, []byte(index.Key))
	binary.Write(serializedIndex, binary.LittleEndian, index.Offset)
	return serializedIndex.Bytes()
}

// Deserialize bytes to an IndexEntry
func DeserializeIndexEntry(serializedIndex []byte) IndexEntry {
	return IndexEntry{
		KeySize: binary.LittleEndian.Uint64(serializedIndex[:8]),
		Key:     string(serializedIndex[8 : 8+binary.LittleEndian.Uint64(serializedIndex[:8])]),
		Offset:  binary.LittleEndian.Uint64(serializedIndex[8+binary.LittleEndian.Uint64(serializedIndex[:8]):]),
	}
}

// Serialize an Index to bytes
func (index Index) SerializeIndexes() []byte {

	var serializedIndexes = new(bytes.Buffer)

	for _, entry := range index.Entries {
		binary.Write(serializedIndexes, binary.LittleEndian, entry.KeySize)
		binary.Write(serializedIndexes, binary.LittleEndian, []byte(entry.Key))
		binary.Write(serializedIndexes, binary.LittleEndian, entry.Offset)
	}

	return serializedIndexes.Bytes()
}

// Deserialize bytes to an Index
func DeserializeIndexes(serializedIndexes []byte) Index {
	var index Index
	index.Entries = make([]IndexEntry, 0)

	for i := 0; i < len(serializedIndexes); {
		keySize := binary.LittleEndian.Uint64(serializedIndexes[i : i+8])
		i += 8
		key := string(serializedIndexes[i : i+int(keySize)]) // Convert i to int here
		i += int(keySize)
		offset := binary.LittleEndian.Uint64(serializedIndexes[i : i+8])
		i += 8

		entry := IndexEntry{
			KeySize: keySize,
			Key:     key,
			Offset:  offset,
		}

		index.Entries = append(index.Entries, entry)
	}

	return index
}
