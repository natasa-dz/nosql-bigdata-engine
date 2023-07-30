package SSTable

import (
	"bytes"
	"encoding/binary"
)

type Header struct {
	logsOffset    uint64
	bloomOffset   uint64
	summaryOffset uint64
	indexOffset   uint64
}

func (header Header) HeaderSerialize() []byte {
	// Create a buffer to store the serialized data
	serialized := new(bytes.Buffer)

	// Write the StartKeySize and EndKeySize to the buffer
	binary.Write(serialized, binary.LittleEndian, header.logsOffset)
	binary.Write(serialized, binary.LittleEndian, header.bloomOffset)
	binary.Write(serialized, binary.LittleEndian, header.summaryOffset)
	binary.Write(serialized, binary.LittleEndian, header.indexOffset)

	return serialized.Bytes()
}

func DeserializeHeader(serializedHeader []byte) Header {

	var logs = binary.LittleEndian.Uint64(serializedHeader[:8])
	var bloom = binary.LittleEndian.Uint64(serializedHeader[8:16])
	var summary = binary.LittleEndian.Uint64(serializedHeader[16:24])
	var index = binary.LittleEndian.Uint64(serializedHeader[24:])

	return Header{
		logsOffset:    logs,
		bloomOffset:   bloom,
		summaryOffset: summary,
		indexOffset:   index,
	}
}
