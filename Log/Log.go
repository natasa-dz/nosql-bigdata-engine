package Log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

// Definise Log odnosno jedan upis, kao jednu jedinicu podataka(ovo ce se sadrzati u WAL-u ali i u memtable strukturama a vrv i u svim ostalim?)

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

///////////////////////////////////////////////////////////////////////

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8
	//LOG_SIZE        = 29

	LOW_WATER_MARK = 5
	File           = "putanja koja se cita iz konfiguracije"
)

type Log struct {
	CRC       uint32
	Timestamp int64
	Tombstone bool
	KeySize   int64
	ValueSize int64
	Key       []byte
	Value     []byte
}

// fja koja ce da vrati CRC jednog Log-a
func CRC32(log *Log) uint32 {
	// Create a new CRC32 hash instance
	crc32Hash := crc32.NewIEEE()

	// Write relevant fields to the hash
	crc32Hash.Write(Int64ToBytes(log.Timestamp))
	crc32Hash.Write(BoolToBytes(log.Tombstone))
	crc32Hash.Write(Int64ToBytes(log.KeySize))
	crc32Hash.Write(Int64ToBytes(log.ValueSize))
	crc32Hash.Write(log.Key)
	crc32Hash.Write(log.Value)

	// Calculate and return the CRC32 value
	return crc32Hash.Sum32()
}

// kreiranje loga pri unosu
func CreateLog(key []byte, value []byte) *Log {
	log := Log{Key: key, Value: value, Tombstone: true, Timestamp: time.Now().Unix(), KeySize: int64(len(key)), ValueSize: int64(len(value))}
	log.CRC = CRC32(&log)
	return &log
}

func (log Log) Serialize() []byte {
	var serializedLog = new(bytes.Buffer)

	binary.Write(serializedLog, binary.LittleEndian, log.CRC)
	binary.Write(serializedLog, binary.LittleEndian, log.Timestamp)
	binary.Write(serializedLog, binary.LittleEndian, log.Tombstone)
	binary.Write(serializedLog, binary.LittleEndian, log.KeySize)
	binary.Write(serializedLog, binary.LittleEndian, log.ValueSize)
	binary.Write(serializedLog, binary.LittleEndian, log.Key)
	binary.Write(serializedLog, binary.LittleEndian, log.Value)
	return serializedLog.Bytes()
}

func ReadLogs(file *os.File, offsetStart int64, offsetEnd uint64) ([]*Log, error) {
	//za multiple 0, end
	//za single logOffset, bloomOffset
	file.Seek(offsetStart, io.SeekStart)
	var data []*Log
	var loaded *Log
	var offset int64
	offset = 0
	//read until the end of logs
	for uint64(offset) < offsetEnd {
		loaded, _ = ReadLog(file)
		offset, _ = file.Seek(0, io.SeekCurrent)
		data = append(data, loaded)
	}
	return data, nil
}

func (log Log) print() {
	fmt.Println(log.Key, log.KeySize, log.ValueSize, log.Value, log.Tombstone, log.Timestamp, log.CRC)
}

func ReadLog(file *os.File) (*Log, error) {
	var log Log

	// Read CRC
	var crcBytes = make([]byte, CRC_SIZE)
	_, err := file.Read(crcBytes)
	if err != nil {
		return nil, err
	}
	log.CRC = binary.LittleEndian.Uint32(crcBytes)

	// Read Timestamp
	var timestampBytes = make([]byte, TIMESTAMP_SIZE)
	_, err = file.Read(timestampBytes)
	if err != nil {
		return nil, err
	}
	log.Timestamp = int64(binary.LittleEndian.Uint64(timestampBytes))

	// Read Tombstone
	var tombstoneByte = make([]byte, TOMBSTONE_SIZE)
	_, err = file.Read(tombstoneByte)
	if err != nil {
		return nil, err
	}
	if tombstoneByte[0] == 1 {
		log.Tombstone = true
	} else {
		log.Tombstone = false
	}

	// Read KeySize
	var keySizeBytes = make([]byte, KEY_SIZE_SIZE)
	_, err = file.Read(keySizeBytes)
	if err != nil {
		return nil, err
	}
	log.KeySize = int64(binary.LittleEndian.Uint64(keySizeBytes))

	// Read ValueSize
	var valueSizeBytes = make([]byte, VALUE_SIZE_SIZE)
	_, err = file.Read(valueSizeBytes)
	if err != nil {
		return nil, err
	}
	log.ValueSize = int64(binary.LittleEndian.Uint64(valueSizeBytes))

	// Read Key
	var keyBytes = make([]byte, log.KeySize)
	_, err = file.Read(keyBytes)
	if err != nil {
		return nil, err
	}
	log.Key = keyBytes

	// Read Value
	var valueBytes = make([]byte, log.ValueSize)
	_, err = file.Read(valueBytes)
	if err != nil {
		return nil, err
	}
	log.Value = valueBytes

	return &log, nil
}
