package Menu

import (
	. "NAiSP/CMS"
	. "NAiSP/HLL"
	. "NAiSP/Log"
	. "NAiSP/SSTable"
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

func WriteAppInitializationMenu() string {
	var retVal string
	fmt.Println("\nYou started app. Do you want to use custom configuration, or you want to use configuration that we made?")
	for true {
		fmt.Println("Type \"custom\"/\"premade\"")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		retVal = scanner.Text()
		if strings.ToUpper(retVal) == "CUSTOM" || strings.ToUpper(retVal) == "PREMADE" {
			break
		}
		fmt.Println("You did not enter valid option...")
	}
	return strings.ToUpper(retVal)
}

func WriteMainMenu() string {
	fmt.Println("\n===========================MENU====================")
	fmt.Println("1. Insert new Log (key, value) [PUT]")
	fmt.Println("2. Search for Log [GET]")
	fmt.Println("3. Delete Log [DELETE]")
	fmt.Println("4. List Logs by prefix [LIST]")
	fmt.Println("5. List Logs by range [RANGE SCAN]")
	fmt.Println("6. Compact level of LSM tree")
	fmt.Println("7. HLL menu")
	fmt.Println("8. CMS menu")
	fmt.Println("X. Exit [EXIT]")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	retVal := scanner.Text()
	return strings.ToUpper(retVal)
}

func CompactionMenu(maxLevelForCompaction int, fileType string) int {
	var levelNum string
	var num int
	var err error
	for true {
		levels := GetALLLevels("./Data/SSTables/" + fileType)
		if len(levels) == 0 {
			fmt.Println("Compaction not possible")
			return 0
		}
		fmt.Println("Enter number of level to be compacted: ")
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			levelNum = scanner.Text()
		}
		num, err = strconv.Atoi(levelNum)

		if err == nil && num <= maxLevelForCompaction && ContainsElement(levels, num) {
			break
		}
		fmt.Println("Invalid input...try again")
	}
	fmt.Println("Compacting a level....")
	return num
}

func CMSMenu() {
	var userInput string

	for userInput != "X" {
		var cmss = DeserializeCMS()
		fmt.Println("===========================CMS MENU====================")
		fmt.Println("1. Create new CMS")
		fmt.Println("2. Add element to CMS")
		fmt.Println("3. Get number of occurrences of element")
		fmt.Println("X. Exit [EXIT]")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		userInput = scanner.Text()
		userInput = strings.ToUpper(userInput)
		if userInput == "1" {
			var cms CountMinScetch
			fmt.Println("-----------------------------------------------------")
			fmt.Println("Enter name: ")
			scanner2 := bufio.NewScanner(os.Stdin)
			scanner2.Scan()
			cms.Initialize(0.1, 0.1, scanner2.Text())
			*cmss = append(*cmss, cms)
			SerializeCMS(cmss)
		} else if userInput == "2" {
			hllNum := ChooseCMS(cmss)
			fmt.Println("-----------------------------------------------------")
			fmt.Println("Enter new element: ")
			scanner2 := bufio.NewScanner(os.Stdin)
			scanner2.Scan()
			(*cmss)[hllNum-1].Add(scanner2.Text())
			SerializeCMS(cmss)
		} else if userInput == "3" {
			hllNum := ChooseCMS(cmss)
			fmt.Println("-----------------------------------------------------")
			fmt.Println("Enter element for search: ")
			scanner2 := bufio.NewScanner(os.Stdin)
			scanner2.Scan()
			numOfEl := (*cmss)[hllNum-1].Search(scanner2.Text())
			fmt.Println("There are", numOfEl, "occurrences")
		}
	}
}

func HLLMenu() {
	var userInput string

	for userInput != "X" {
		var hlls = DeserializeHLLs()
		fmt.Println("===========================HLL MENU====================")
		fmt.Println("1. Create new HLL")
		fmt.Println("2. Add element to HLL")
		fmt.Println("3. Get cardinality")
		fmt.Println("X. Exit [EXIT]")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		userInput = scanner.Text()
		userInput = strings.ToUpper(userInput)
		if userInput == "1" {
			var hll HLL
			fmt.Println("-----------------------------------------------------")
			fmt.Println("Enter name: ")
			scanner2 := bufio.NewScanner(os.Stdin)
			scanner2.Scan()
			hll.Initialize(6, scanner2.Text())
			*hlls = append(*hlls, hll)
			Serialize(hlls)
		} else if userInput == "2" {
			hllNum := ChooseHLL(hlls)
			fmt.Println("-----------------------------------------------------")
			fmt.Println("Enter new element: ")
			scanner2 := bufio.NewScanner(os.Stdin)
			scanner2.Scan()
			(*hlls)[hllNum-1].Add(scanner2.Text())
			Serialize(hlls)
		} else if userInput == "3" {
			hllNum := ChooseHLL(hlls)
			numOfEl := (*hlls)[hllNum-1].Estimate()
			fmt.Println("Estimating that it has ", math.Round(numOfEl), "elements")
		}
	}
}

func PUT_Menu() ([]byte, []byte) {
	var key, value []byte
	for true {
		fmt.Println("\nEnter key: ")
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			key = scanner.Bytes()
		}
		fmt.Println("Enter value: ")
		scanner2 := bufio.NewScanner(os.Stdin)
		if scanner2.Scan() {
			value = scanner2.Bytes()
		}
		if string(key) == "" || string(value) == "" {
			fmt.Println("Both key and value can not be empty")
			continue
		}
		break
	}
	fmt.Println("Entering new log....")
	return key, value
}

func PUT_Response(success bool) {
	if success {
		fmt.Println("\n\tEntering was successful")
		return
	}
	fmt.Println("\n\tSomething went wrong...Log was not inserted")
}

func GET_Menu() string {
	var key string
	for true {
		fmt.Println("\nEnter key: ")
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			key = scanner.Text()
			if key == "" {
				fmt.Println("\n\tKey can not be empty!")
				continue
			}
			break
		}
	}
	return key
}

func GET_Response(result []byte, key string) {
	if result == nil {
		fmt.Println("\n\tThere is no such key")
	} else {
		fmt.Println("\n\t===Result for GET===")
		fmt.Println("\tKey : ", key)
		fmt.Println("\tValue: ", string(result))
	}
}

func DELETE_Menu() string {
	var key string
	for true {
		fmt.Println("\nEnter key: ")
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			key = scanner.Text()
			if key == "" {
				fmt.Println("\n\tKey can not be empty")
				continue
			}
		}
		break
	}
	return key
}

func DELETE_Response(success bool) {
	if success {
		fmt.Println("\n\tDeleting was successful")
		return
	}
	fmt.Println("\n\tSomething went wrong...Log was not deleted")
}

func LIST_Menu() string {
	var prefix string
	for true {
		fmt.Println("\nEnter prefix: ")
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			prefix = scanner.Text()
			if prefix == "" {
				fmt.Println("\n\tPrefix can not be empty")
				continue
			}
		}
		break
	}
	return prefix
}

func RANGESCAN_Menu() (string, string) {
	var minKey string
	var maxKey string
	for true {
		fmt.Println("\nEnter min key: ")
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			minKey = scanner.Text()
		}
		fmt.Println("Enter max key: ")
		scanner2 := bufio.NewScanner(os.Stdin)
		if scanner2.Scan() {
			maxKey = scanner2.Text()
		}
		if minKey == "" || maxKey == "" || maxKey < minKey {
			fmt.Println("\n\tKeys were not entered the right way")
			continue
		}
		break
	}
	return minKey, maxKey
}

func LIST_RANGESCAN_PaginationResponse(results []*Log, amountOfDataPerPage int) {
	if len(results) < amountOfDataPerPage {
		printPage(results, 1)
	} else {
		var input string
		numOfPage := 1
		totalNumOfPages := int(math.Ceil(float64(len(results)) / float64(amountOfDataPerPage)))
		resToShow := results[0:amountOfDataPerPage]
		printPage(resToShow, numOfPage)
		for true {
			input = paginationMenu(totalNumOfPages)
			if input == "NEXT" {
				if numOfPage+1 > totalNumOfPages { //poslednja strana nema dalje
					fmt.Println("\n\tYou have reached last page\n")
					continue
				}
				numOfPage++
				maxIndex := numOfPage * amountOfDataPerPage
				if numOfPage == totalNumOfPages { //na poslednjoj ima recimo samo 3
					resToShow = results[maxIndex-amountOfDataPerPage:] //a mi treba da prikazemo 5 po strani
					printPage(resToShow, numOfPage)
				} else {
					resToShow = results[maxIndex-amountOfDataPerPage : maxIndex]
					printPage(resToShow, numOfPage)
				}
			} else if input == "PREVIOUS" {
				if numOfPage-1 < 1 {
					fmt.Println("\n\tYou are at first page")
					continue
				}
				numOfPage--
				maxIndex := numOfPage * amountOfDataPerPage
				resToShow = results[maxIndex-amountOfDataPerPage : maxIndex]
				printPage(resToShow, numOfPage)
			} else if input == "X" {
				break
			} else {
				numOfPage, _ = strconv.Atoi(input)
				maxIndex := numOfPage * amountOfDataPerPage
				if numOfPage == totalNumOfPages {
					resToShow = results[maxIndex-amountOfDataPerPage:]
					printPage(resToShow, numOfPage)
					continue
				}
				resToShow = results[maxIndex-amountOfDataPerPage : maxIndex]
				printPage(resToShow, numOfPage)
			}
		}
		fmt.Println("Exiting...")
	}
}

func printPage(results []*Log, numOfPage int) {
	fmt.Println("\n=====================PAGE" + strconv.Itoa(numOfPage) + "===================")
	for i, data := range results {
		fmt.Println("\t", strconv.Itoa(i+1)+".", "Key: ", string(data.Key))
		fmt.Println("\t", "Value: ", string(data.Value))
	}
	fmt.Println("============================================\n")
}

func paginationMenu(totalNumOfPages int) string {
	var input string
	for true {
		fmt.Println("----  Results have " + strconv.Itoa(totalNumOfPages) + " of pages! You can:  ----")
		fmt.Println("- Enter \"Next\" for next page")
		fmt.Println("- Enter \"Previous\" for previous page")
		fmt.Println("- Enter num of page you want to see")
		fmt.Println("- Enter \"x\" for exiting")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		input = scanner.Text()
		//fmt.Println("--------------------------------------------------------")
		notDone := strings.ToUpper(input) != "NEXT" && strings.ToUpper(input) != "PREVIOUS" && !isValidInteger(input, totalNumOfPages) && strings.ToUpper(input) != "X"
		if notDone {
			fmt.Println("Wrong option try again")
		} else {
			break
		}
	}
	return strings.ToUpper(input)
}

func OutOfTokensNotification() {
	fmt.Println("You made to many requests at once. Please try again some time soon!")
}

func isValidInteger(input string, maxNumOfPage int) bool {
	numOfPage, err := strconv.Atoi(input)
	if err == nil && numOfPage <= maxNumOfPage && numOfPage >= 1 {
		return true
	}
	return false
}
