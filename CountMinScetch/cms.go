package main

type CountMinScetch struct {
	k, m              uint
	hashFunctions     []HashWithSeed
	bitSlicesOfSlices [][]byte
}

func MinInt(values []int) int {

	min := 0

	for _, value := range values {
		if value < min {
			min = value
		}
	}

	return min
}

func (cms *CountMinScetch) initialize(epsilon float64, delta float64) {
	cms.k = CalculateK(delta)
	cms.m = CalculateM(epsilon)

	cms.hashFunctions = CreateHashFunctions(cms.k)

	cms.bitSlicesOfSlices = make([][]byte, cms.k, cms.m)
}

func (cms *CountMinScetch) search(data []byte) int {
	counts := make([]int, cms.k)

	for ki, hf := range cms.hashFunctions {
		hash := hf.Hash(data)
		bit := cms.bitSlicesOfSlices[ki][int(uint(hash)%cms.m)]

		counts[ki] = int(bit)

	}

	return MinInt(counts)
}

func (cms *CountMinScetch) add(data []byte) {
	for ki, hf := range cms.hashFunctions {
		hash := hf.Hash(data)

		cms.bitSlicesOfSlices[ki][int(uint(hash)%cms.m)] += 1
	}
}
