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
	//fmt.Println(GetMD5Hash("he"))
	//fmt.Println(ToBinary(GetMD5Hash("he")))

	words1 := ReadFile("tekst1.txt")
	words2 := ReadFile("tekst2.txt")

	var simHash1 SimHash
	simHash1.InitializeSimHash(words1)
	fmt.Println(len(simHash1.valuesWithWeights))

	var simHash2 SimHash
	simHash2.InitializeSimHash(words2)

	fmt.Println(simHash1.GetFingerprint())
	fmt.Println()
	fmt.Println(simHash1.GetFingerprint())
	fmt.Println()
	fmt.Println(simHash1.Compare(simHash2))
}
