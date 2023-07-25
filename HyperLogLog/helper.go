package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math"
	"math/bits"
	"strconv"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	m   uint64
	p   uint8
	reg []uint8
}

func (hll *HLL) InitializeSimHash(val uint8) {
	hll.p = val
	hll.m = uint64(math.Pow(2.0, float64(hll.p)))
	hll.reg = make([]uint8, hll.m)
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HLL) add(val string) {
	hash := ToBinary(GetMD5Hash(val))
	bucketS := hash[0:hll.p]
	bucket := ToDecimal(bucketS)
	fmt.Println(bucket)
	zeros := hash[246:]
	zeroNum := bits.TrailingZeros(uint(ToDecimal(zeros)))
	fmt.Println(zeros)
	hll.reg[bucket] = uint8(zeroNum)
	fmt.Println(zeroNum)
}

func ToDecimal(val string) uint64 {
	num, _ := strconv.Atoi(val)
	var ret uint64
	ret = 0
	index := 0
	for num != 0 {
		r := num % 10
		num = num / 10
		ret += uint64(r) * uint64(math.Pow(2.0, float64(index)))
		index++
	}
	//fmt.Println(ret)
	return ret
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
