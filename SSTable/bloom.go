package SSTable

type Bloom2 struct {
	m, k          int
	bitSlices     []byte
	hashFunctions []HashWithSeed
}

func (bloom *Bloom2) InitializeBloom2(expectedElements int, falsePositiveRate float64) {
	mu := CalculateM(expectedElements, falsePositiveRate)
	bloom.m = int(mu)

	ku := CalculateK(expectedElements, mu)
	bloom.k = int(ku)

	bloom.bitSlices = make([]byte, bloom.m)
	for i := 0; i < bloom.m; i++ {
		bloom.bitSlices[i] = 0
	}

	bloom.hashFunctions = CreateHashFunctions(uint(bloom.k))
}

func (bloom *Bloom2) BloomSearch2(data []byte) bool {

	exist := true

	for _, hf := range bloom.hashFunctions {
		hash := hf.Hash(data)
		bit := bloom.bitSlices[int(hash%uint64(bloom.m))]

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

func (bloom *Bloom2) add(data []byte) {

	for _, hf := range bloom.hashFunctions {
		hash := hf.Hash(data)
		bloom.bitSlices[int(hash%uint64(bloom.m))] = 1
	}
}
