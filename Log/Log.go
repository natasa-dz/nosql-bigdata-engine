package Log

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
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
	LOG_SIZE        = 29

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
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// kreiranje loga(pri unosu?)
func CreateLog(key []byte, value []byte) *Log {
	log := Log{Key: key, Value: value, Tombstone: true, Timestamp: time.Now().Unix(), KeySize: KEY_SIZE_SIZE, ValueSize: VALUE_SIZE_SIZE}
	log.CRC = CRC32(log.Value)
	return &log
}

func (log Log) Serialize() []byte {
	var serializedLog = new(bytes.Buffer)

	binary.Write(serializedLog, binary.LittleEndian, log.CRC)
	binary.Write(serializedLog, binary.LittleEndian, log.Timestamp)
	binary.Write(serializedLog, binary.LittleEndian, log.Tombstone)
	binary.Write(serializedLog, binary.LittleEndian, log.KeySize)
	binary.Write(serializedLog, binary.LittleEndian, log.ValueSize)
	binary.Write(serializedLog, binary.LittleEndian, []byte(log.Key))
	binary.Write(serializedLog, binary.LittleEndian, log.Value)
	return serializedLog.Bytes()
}

func DeserializeRecord(serializedRecord []byte) Log {
	var ret Log

	ret.CRC = binary.LittleEndian.Uint32(serializedRecord[:CRC_SIZE])

	ret.KeySize = int64(binary.LittleEndian.Uint64(serializedRecord[CRC_SIZE+TOMBSTONE_SIZE+TIMESTAMP_SIZE : CRC_SIZE+TOMBSTONE_SIZE+TIMESTAMP_SIZE+KEY_SIZE_SIZE]))

	ret.ValueSize = int64(binary.LittleEndian.Uint64(serializedRecord[CRC_SIZE+TOMBSTONE_SIZE+TIMESTAMP_SIZE+KEY_SIZE_SIZE : CRC_SIZE+TOMBSTONE_SIZE+TIMESTAMP_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE]))

	ret.Key = []byte(fmt.Sprintf("%s", serializedRecord[CRC_SIZE+TOMBSTONE_SIZE+TIMESTAMP_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE:CRC_SIZE+TOMBSTONE_SIZE+TIMESTAMP_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE+ret.KeySize]))

	ret.Value = serializedRecord[CRC_SIZE+TOMBSTONE_SIZE+TIMESTAMP_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE+ret.KeySize : CRC_SIZE+TOMBSTONE_SIZE+TIMESTAMP_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE+ret.KeySize+ret.ValueSize]

	ret.Timestamp = int64(uint64(binary.LittleEndian.Uint64(serializedRecord[CRC_SIZE : CRC_SIZE+TIMESTAMP_SIZE])))

	if serializedRecord[CRC_SIZE+TIMESTAMP_SIZE] == 1 {
		ret.Tombstone = true
	} else {
		ret.Tombstone = false
	}

	return ret
}
