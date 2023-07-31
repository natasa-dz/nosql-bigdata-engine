package SSTable

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

type Header struct {
	LogsOffset    uint64
	BloomOffset   uint64
	IndexOffset   uint64
	SummaryOffset uint64
}

func (header Header) HeaderSerialize() []byte {
	serialized := new(bytes.Buffer)

	binary.Write(serialized, binary.LittleEndian, header.LogsOffset)
	binary.Write(serialized, binary.LittleEndian, header.BloomOffset)
	binary.Write(serialized, binary.LittleEndian, header.SummaryOffset)
	binary.Write(serialized, binary.LittleEndian, header.IndexOffset)

	return serialized.Bytes()
}

func DeserializeHeader(serializedHeader []byte) Header {

	var logs = binary.LittleEndian.Uint64(serializedHeader[:8])
	var bloom = binary.LittleEndian.Uint64(serializedHeader[8:16])
	var summary = binary.LittleEndian.Uint64(serializedHeader[16:24])
	var index = binary.LittleEndian.Uint64(serializedHeader[24:])

	return Header{
		LogsOffset:    logs,
		BloomOffset:   bloom,
		SummaryOffset: summary,
		IndexOffset:   index,
	}
}
func ReadHeader(file *os.File) (*Header, error) {
	file.Seek(0, io.SeekStart)
	var headerBytes = make([]byte, 32)
	_, err := file.Read(headerBytes)
	if err != nil {
		return nil, err
	}
	header := DeserializeHeader(headerBytes)
	return &header, nil
}
