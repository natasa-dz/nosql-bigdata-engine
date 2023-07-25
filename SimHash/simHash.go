package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
)

type SimHash struct {
	valuesWithWeights map[string]int
	valuesWithHash    map[string][]int
	fingerprint       []int
}

func (simHash *SimHash) InitializeSimHash(values []string) {

	simHash.valuesWithWeights = make(map[string]int)

	for _, v := range values {
		simHash.valuesWithWeights[v] += 1
	}

	simHash.valuesWithHash = make(map[string][]int)

	simHash.CalculateFingerprint()
}

func (simHash *SimHash) CalculateFingerprint() {
	for key, element := range simHash.valuesWithWeights {
		hash := ToBinary(GetMD5Hash(key))
		for _, c := range hash {
			if c == 48 {
				simHash.valuesWithHash[key] = append(simHash.valuesWithHash[key], -element)
			} else {
				simHash.valuesWithHash[key] = append(simHash.valuesWithHash[key], element)
			}
		}
	}

	for i := 0; i < 256; i++ {
		sum := 0
		for _, el := range simHash.valuesWithHash {
			sum += el[i]
		}

		if sum > 0 {
			simHash.fingerprint = append(simHash.fingerprint, 1)
		} else {
			simHash.fingerprint = append(simHash.fingerprint, 0)
		}

	}

}

func (simHash *SimHash) GetFingerprint() []int {
	return simHash.fingerprint
}

func (simHash *SimHash) Compare(other SimHash) int {
	hammingDistance := 0
	for i := 0; i < len(simHash.fingerprint); i++ {
		if simHash.fingerprint[i] != other.fingerprint[i] {
			hammingDistance += 1
		}
	}
	return hammingDistance
}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func ToBinary(s string) string {
	res := ""
	for _, c := range s {
		res = fmt.Sprintf("%s%.8b", res, c)
	}
	return res
}
