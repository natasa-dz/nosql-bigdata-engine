package main

import (
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

	LOW_WATER_MARK = 5
)

type Log struct {
	CRC       uint32
	Timestamp time.Time
	Tombstone bool
	KeySize   int64
	ValueSize int64
	Key       string
	Value     []byte
}

// fja koja ce da vrati CRC jednog Log-a
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// TODO dodati CRC, otp znam sta je to ali nez koja bi mu bila defaultna vrednost i malo me jebu ove velicine videti sa natasom ona se time vec bavila
// kreiranje loga(pri unosu?)
func CreateLog(key string, value []byte) *Log {
	log := Log{Key: key, Value: value, Tombstone: true, Timestamp: time.Now(), KeySize: KEY_SIZE_SIZE, ValueSize: VALUE_SIZE_SIZE}
	log.CRC = CRC32(log.Value)
	return &log
}

// load Loga iz fajla
func LoadLog(CRC uint32, timestamp time.Time, tombstone bool, keySize, valueSize int64, key string, value []byte) *Log {
	log := Log{CRC: CRC, Timestamp: timestamp, Tombstone: tombstone, KeySize: keySize, ValueSize: valueSize, Key: key, Value: value}
	return &log
}
