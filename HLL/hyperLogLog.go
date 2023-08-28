package HLL

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math"
	"math/bits"
	"os"
	"strconv"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	Name string  `json:"name"`
	M    uint64  `json:"m"`
	P    uint8   `json:"p"`
	Reg  []uint8 `json:"reg"`
}

func ChooseHLL(hlls *[]HLL) int {
	fmt.Println("-----------------------------------------------------")
	for i, hll := range *hlls {
		fmt.Println(i+1, "Name:", hll.Name)
	}

	var hllNum string
	for true {
		fmt.Println("-----------------------------------------------------")
		fmt.Println("Choose hll number: ")
		scanner3 := bufio.NewScanner(os.Stdin)
		if scanner3.Scan() {
			hllNum = scanner3.Text()
		}
		num, err := strconv.Atoi(hllNum)

		if err == nil && num > 0 && num <= len(*hlls) {
			return num
		}
		fmt.Println("Invalid input...try again")
	}
	return 0
}

func Serialize(hlls *[]HLL) {
	data, err := json.MarshalIndent(hlls, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	// Write the JSON data to a file
	err = ioutil.WriteFile("./HLL/hll.json", data, 0644)
	if err != nil {
		log.Fatal(err)
	}

}

func DeserializeHLLs() *[]HLL {
	data, err := ioutil.ReadFile("./HLL/hll.json")
	if err != nil {
		log.Fatal(err)
	}
	var hlls []HLL

	// Decode the JSON data into the slice of Person
	err = json.Unmarshal(data, &hlls)
	if err != nil {
		log.Fatal(err)
	}
	return &hlls
}

func (hll *HLL) Initialize(val uint8, name string) {
	hll.Name = name
	hll.P = val
	hll.M = uint64(math.Pow(2.0, float64(hll.P)))
	hll.Reg = make([]uint8, hll.M)
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.Reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HLL) Add(val string) {
	hashValue := hash([]byte(val))
	k := 32 - hll.P
	bucket := hashValue >> uint64(k) //uzmi prvih p elemenata
	zeroNum := 1 + bits.TrailingZeros(uint(hashValue))

	if uint8(zeroNum) > hll.Reg[bucket] {
		hll.Reg[bucket] = uint8(zeroNum)
	}
}
func hash(stream []byte) uint32 {
	h := fnv.New32()
	h.Write(stream)
	sum := h.Sum32()
	return sum
}
