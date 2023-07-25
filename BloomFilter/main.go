package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	//var bloom Bloom
	//
	const (
		layer             = 3
		expectedElements  = 100
		falsePositiveRate = 0.1
	)
	//
	//bloom.InitializeBloom(layer, expectedElements, falsePositiveRate)
	//fmt.Println(bloom.m)
	//fmt.Println(bloom.k)
	//fmt.Println(bloom.layer)
	//fmt.Println(bloom.bitSlicesOfSlices)
	//

	var bloom Bloom2

	bloom.InitializeBloom2(expectedElements, falsePositiveRate)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		data := []byte(scanner.Text())
		bloom.add(data)

		if scanner.Text() == "x" {
			break
		}
	}

	data1 := []byte("abc")
	data2 := []byte("abd")
	data3 := []byte("rand")
	fmt.Println(bloom.BloomSearch2(data1))
	fmt.Println(bloom.BloomSearch2(data2))
	fmt.Println(bloom.BloomSearch2(data3))

}
