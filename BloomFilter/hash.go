package BloomFilter

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"time"
)

type HashWithSeed struct {
	Seed []byte
}

func (h HashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func (h HashWithSeed) Serialize() []byte {
	// First, serialize the length of the Seed.
	seedLen := uint32(len(h.Seed))
	seedLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(seedLenBytes, seedLen)

	// Next, append the length bytes and the Seed bytes together.
	serializedData := append(seedLenBytes, h.Seed...)

	return serializedData
}

func (h *HashWithSeed) Deserialize(data []byte) error {
	// Ensure that the data is at least 4 bytes (length of Seed length).
	if len(data) < 4 {
		return fmt.Errorf("invalid serialized data for HashWithSeed")
	}

	// Extract the Seed length from the first 4 bytes.
	seedLen := binary.LittleEndian.Uint32(data[:4])

	// Ensure that the data contains enough bytes for the Seed.
	if len(data) < int(4+seedLen) {
		return fmt.Errorf("invalid serialized data for HashWithSeed")
	}

	// Extract the Seed bytes.
	h.Seed = make([]byte, seedLen)
	copy(h.Seed, data[4:4+seedLen])

	return nil
}

func CreateHashFunctions(k uint) []HashWithSeed {
	h := make([]HashWithSeed, k)
	ts := uint(time.Now().Unix()) //35632716357
	for i := uint(0); i < k; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(ts+i))
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}
