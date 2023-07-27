package BloomFilter

type Bloom2 struct {
	m, k          int
	BitSlices     []byte
	hashFunctions []HashWithSeed
}

func (bloom *Bloom2) InitializeBloom2(expectedElements int, falsePositiveRate float64) {
	mu := CalculateM(expectedElements, falsePositiveRate)
	bloom.m = int(mu)

	ku := CalculateK(expectedElements, mu)
	bloom.k = int(ku)

	bloom.BitSlices = make([]byte, bloom.m)
	for i := 0; i < bloom.m; i++ {
		bloom.BitSlices[i] = 0
	}

	bloom.hashFunctions = CreateHashFunctions(uint(bloom.k))
}

func (bloom *Bloom2) BloomSearch2(data []byte) bool {

	exist := true

	for _, hf := range bloom.hashFunctions {
		hash := hf.Hash(data)
		bit := bloom.BitSlices[int(hash%uint64(bloom.m))]

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

	for _, hf := range bloom.hashFunctions {
		hash := hf.Hash(data)
		bloom.BitSlices[int(hash%uint64(bloom.m))] = 1
	}
}
