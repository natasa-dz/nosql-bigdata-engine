package BloomFilter

import (
	"bytes"
	"encoding/binary"
	"os"
)

type Bloom2 struct {
	M, K          int
	BitSlices     []byte
	HashFunctions []HashWithSeed
}

func (bloom *Bloom2) InitializeBloom2(expectedElements int, falsePositiveRate float64) {
	mu := CalculateM(expectedElements, falsePositiveRate)
	bloom.M = int(mu)

	ku := CalculateK(expectedElements, mu)
	bloom.K = int(ku)

	bloom.BitSlices = make([]byte, bloom.M)
	for i := 0; i < bloom.M; i++ {
		bloom.BitSlices[i] = 0
	}

	bloom.HashFunctions = CreateHashFunctions(uint(bloom.K))
}

func (bloom *Bloom2) BloomSearch2(data []byte) bool {

	exist := true

	for _, hf := range bloom.HashFunctions {
		hash := hf.Hash(data)
		bit := bloom.BitSlices[int(hash%uint64(bloom.M))]

		if bit == 0 {
			exist = false
			break
		}
	}

	if exist {
		return true
	} else {
		return false
	}
}

func (bloom *Bloom2) Add(data []byte) {

	for _, hf := range bloom.HashFunctions {
		hash := hf.Hash(data)
		bloom.BitSlices[int(hash%uint64(bloom.M))] = 1
	}
}

// Serialize serializes the Bloom2 filter to a byte slice.
func (bloom *Bloom2) Serialize() []byte {
	var serializedBloom = new(bytes.Buffer)

	// Serialize the filter parameters
	binary.Write(serializedBloom, binary.LittleEndian, uint64(bloom.M))
	binary.Write(serializedBloom, binary.LittleEndian, uint64(bloom.K))

	// Serialize the bit slices
	serializedBloom.Write(bloom.BitSlices)

	// Serialize the hash functions
	for _, hf := range bloom.HashFunctions {
		serializedHashFunc := hf.Serialize()
		binary.Write(serializedBloom, binary.LittleEndian, uint64(len(serializedHashFunc)))
		serializedBloom.Write(serializedHashFunc)
	}

	return serializedBloom.Bytes()
}

// procitaj Bloom filter iz fajla
func ReadBloom(file *os.File) *Bloom2 {
	var bloom = new(Bloom2)

	var m, k uint64
	var b = make([]byte, binary.Size(m))
	if _, err := file.Read(b); err != nil {
		panic(err)
	}
	m = binary.LittleEndian.Uint64(b)
	bloom.M = int(m)

	b = make([]byte, binary.Size(k))
	if _, err := file.Read(b); err != nil {
		panic(err)
	}
	k = binary.LittleEndian.Uint64(b)
	bloom.K = int(k)

	bloom.BitSlices = make([]byte, bloom.M)
	if _, err := file.Read(bloom.BitSlices); err != nil {
		panic(err)
	}

	bloom.HashFunctions = make([]HashWithSeed, bloom.K)
	for i := 0; i < bloom.K; i++ {
		var hashFuncSize uint64
		b = make([]byte, binary.Size(hashFuncSize))
		if _, err := file.Read(b); err != nil {
			panic(err)
		}
		hashFuncSize = binary.LittleEndian.Uint64(b)

		b = make([]byte, hashFuncSize)
		if _, err := file.Read(b); err != nil {
			panic(err)
		}

		hashFunc := HashWithSeed{}
		hashFunc.Deserialize(b)
		bloom.HashFunctions[i] = hashFunc
	}

	return bloom
}
