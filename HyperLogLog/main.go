package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func ReadFile(fileName string) []string {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	//words := list.New()
	var words []string

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		word := scanner.Text()
		if strings.Contains(word, ".") ||
			strings.Contains(word, ",") {
			word = strings.ReplaceAll(word, ".", "")
			word = strings.ReplaceAll(word, ",", "")
		}

		word = strings.ToLower(word)

		//words.PushBack(word)
		words = append(words, word)

	}

	errS := scanner.Err()
	if errS != nil {
		log.Fatal(errS)
	}

	//wordsSlice := make([]string, words.Len())

	return words
}

func main() {
	words1 := ReadFile("text.txt")
	var hll HLL
	hll.InitializeSimHash(16)
	fmt.Println(len(words1))
	for i := 0; i < len(words1); i++ {
		hll.add(words1[i])
	}

	est := hll.Estimate()

	//for i := 0; i < int(hll.m); i++ {
	//	if hll.reg[i] != 0 {
	//		fmt.Println(hll.reg[i])
	//	}
	//
	//}

	fmt.Println(est)

}
