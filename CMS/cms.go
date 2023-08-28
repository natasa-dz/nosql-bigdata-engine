package CMS

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
)

type CountMinScetch struct {
	Name              string
	K, M              uint
	HashFunctions     []HashWithSeed
	BitSlicesOfSlices [][]byte
}

func ChooseCMS(cms *[]CountMinScetch) int {
	fmt.Println("-----------------------------------------------------")
	for i, hll := range *cms {
		fmt.Println(i+1, "Name:", hll.Name)
	}

	var hllNum string
	for true {
		fmt.Println("-----------------------------------------------------")
		fmt.Println("Choose cms number: ")
		scanner3 := bufio.NewScanner(os.Stdin)
		if scanner3.Scan() {
			hllNum = scanner3.Text()
		}
		num, err := strconv.Atoi(hllNum)

		if err == nil && num > 0 && num <= len(*cms) {
			return num
		}
		fmt.Println("Invalid input...try again")
	}
	return 0
}

func SerializeCMS(cms *[]CountMinScetch) {
	data, err := json.MarshalIndent(cms, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	// Write the JSON data to a file
	err = ioutil.WriteFile("./CMS/cms.json", data, 0644)
	if err != nil {
		log.Fatal(err)
	}

}

func DeserializeCMS() *[]CountMinScetch {
	data, err := ioutil.ReadFile("./CMS/cms.json")
	if err != nil {
		log.Fatal(err)
	}
	var cms []CountMinScetch

	// Decode the JSON data into the slice of Person
	err = json.Unmarshal(data, &cms)
	if err != nil {
		log.Fatal(err)
	}
	return &cms
}

func MinInt(values []int) int {

	min := math.MaxInt64
	for _, value := range values {
		if value < min {
			min = value
		}
	}

	return min
}

func (cms *CountMinScetch) Initialize(epsilon float64, delta float64, name string) {
	cms.Name = name
	cms.K = CalculateK(delta)
	cms.M = CalculateM(epsilon)
	cms.HashFunctions = CreateHashFunctions(cms.K)

	cms.BitSlicesOfSlices = make([][]byte, cms.K)
	for i := range cms.BitSlicesOfSlices {
		cms.BitSlicesOfSlices[i] = make([]byte, cms.M)
	}
}

func (cms *CountMinScetch) Search(data string) int {
	counts := make([]int, cms.K)

	for ki, hf := range cms.HashFunctions {
		hash := hf.Hash([]byte(data))
		bit := cms.BitSlicesOfSlices[ki][int(uint(hash)%cms.M)]

		counts[ki] = int(bit)

	}

	return MinInt(counts)
}

func (cms *CountMinScetch) Add(data string) {
	for ki, hf := range cms.HashFunctions {
		hash := hf.Hash([]byte(data))
		b := int(uint(hash) % cms.M)
		cms.BitSlicesOfSlices[ki][b] += 1
	}
}
