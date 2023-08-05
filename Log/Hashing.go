package Log

func Int64ToBytes(toConvert int64) []byte {
	bytes := make([]byte, 8)
	for i := uint(0); i < 8; i++ {
		bytes[i] = byte(toConvert >> (8 * i))
	}
	return bytes
}

func BoolToBytes(toConvert bool) []byte {
	if toConvert {
		return []byte{1}
	}
	return []byte{0}
}
