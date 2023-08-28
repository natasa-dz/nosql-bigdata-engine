package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"
)

func getRandomData() (out [][]byte, intout []uint32) {
	for i := 0; i < 200; i++ {
		rand.Seed(time.Now().UnixNano())
		i := rand.Uint32()
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, i)
		out = append(out, b)
		intout = append(intout, i)
	}
	return
}
func classicCountDistinct(input []uint32) int {
	m := map[uint32]struct{}{}
	for _, i := range input {
		if _, ok := m[i]; !ok {
			m[i] = struct{}{}
		}
	}
	return len(m)
}
func main() {

	bs, is := getRandomData()
	dt := classicCountDistinct(is)
	var hll2 HLL
	hll2.InitializeSimHash(6)
	fmt.Println("words", dt)
	for _, b := range bs {
		hll2.add(string(b))
	}
	est2 := hll2.Estimate()
	fmt.Println("estimated", est2)
	hll2.Name = "HLL1"
	//people := []HLL{hll2}

}
