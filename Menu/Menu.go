package Menu

import (
	. "NAiSP/Log"
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func WriteAppInitializationMenu() string {
	var retVal string
	fmt.Println("You started app. Do you want to use custom configuration, or you want to use configuration that we made?")
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
	fmt.Println("===========================MENU====================")
	fmt.Println("1. Insert new Log (key, value) [PUT]")
	fmt.Println("2. Search for Log [GET]")
	fmt.Println("3. Delete Log [DELETE]")
	fmt.Println("4. List Logs by prefix [LIST]")
	fmt.Println("5. List Logs by range [RANGE SCAN]")
	fmt.Println("X. Exit [EXIT]")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	retVal := scanner.Text()
	return strings.ToUpper(retVal)
}

func PUT_Menu() ([]byte, []byte) {
	var key, value []byte
	for true {
		fmt.Println("Enter key: ")
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
		fmt.Println("Entering was successful")
		return
	}
	fmt.Println("Something went wrong...Log was not inserted")
}

func GET_Menu() string {
	var key string
	for true {
		fmt.Println("Enter key: ")
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			key = scanner.Text()
			if key == "" {
				fmt.Println("Key can not be empty")
				continue
			}
			break
		}
	}
	return key
}

func GET_Response(result []byte, key string) {
	if result == nil {
		fmt.Println("There is no such key")
	} else {
		fmt.Println("===Result for GET===")
		fmt.Println("Key : ", key)
		fmt.Println("Value: ", result)
	}
}

func DELETE_Menu() string {
	var key string
	for true {
		fmt.Println("Enter key: ")
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			key = scanner.Text()
			if key == "" {
				fmt.Println("Key can not be empty")
				continue
			}
		}
		break
	}
	return key
}

func DELETE_Response(success bool) {
	if success {
		fmt.Println("Deleting was successful")
		return
	}
	fmt.Println("Something went wrong...Log was not deleted")
}

func LIST_Menu() string {
	var prefix string
	for true {
		fmt.Println("Enter prefix: ")
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			prefix = scanner.Text()
			if prefix == "" {
				fmt.Println("Prefix can not be empty")
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
		fmt.Println("Enter min key: ")
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
			fmt.Println("Keys were not entered the right way")
			continue
		}
		break
	}
	return minKey, maxKey
}

func LIST_RANGESCAN_PaginationResponse(results []*Log) {
	minIndexToShow := 0
	maxIndexToShow := 5 //nece biti bkv najveci index za prikaz mora jedan veci jer je exclusive taj drugi index
	if len(results) < 5 {
		printPage(results)
	} else {
		var input string
		for true {
			resToShow := results[minIndexToShow:maxIndexToShow]
			printPage(resToShow)
			input = paginationMenu()
			if strings.ToUpper(input) == "NEXT" {
				if maxIndexToShow >= len(results) { //nema vise gde
					fmt.Println("You have reached last page.")
					continue
				}
				minIndexToShow += 5
				maxIndexToShow += 5
				if maxIndexToShow > len(results)-1 { //da ne bi bacao OUT OF RANGE err izjednacimo ga sa duzinom
					maxIndexToShow = len(results)
				}
			} else if strings.ToUpper(input) == "PREVIOUS" {
				if minIndexToShow == 0 { //stigli smo do kraja ne moze vise nazad
					fmt.Println("You cant go back. This is first page.")
					continue
				}
				maxIndexToShow -= 5
				minIndexToShow -= 5
				if maxIndexToShow != len(results) {
					maxIndexToShow = minIndexToShow + 5
				}
			} else if strings.ToUpper(input) == "X" {
				break
			} else {
				fmt.Println("Unspecified action. Try again!")
			}
		}
		fmt.Println("Exiting...")
	}
}

func printPage(results []*Log) {
	fmt.Println("=====================PAGE===================")
	for i, data := range results {
		fmt.Println(strconv.Itoa(i+1)+".", "Key: ", string(data.Key))
		fmt.Println("Value: ", string(data.Value))
	}
	fmt.Println("============================================")
}

func paginationMenu() string {
	fmt.Println("Enter \"Next\" for next page")
	fmt.Println("Enter \"Previous\" for previous page")
	fmt.Println("Enter \"x\" for exiting")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	retVal := scanner.Text()
	return retVal
}

func OutOfTokensNotification() {
	fmt.Println("You made to many requests at once. Please try again some time soon!")
}
