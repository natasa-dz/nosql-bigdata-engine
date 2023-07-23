package main

import (
	"fmt"
)

const q int = 2

// konstanta t je neki stepen ovog drveta koji ja mislim da se odredjuje iz nekog log fajla pri pokretanju
// necu je zakucati dok ne zavrsim
// ceo kod ali posle radi testa cu je raditi sa 2 jer je to vljd min stepen toga-->msm da je to broj kljuceva u cvoru(2*q-1)
type Node struct { //parametri i deklaracija strukture cvora
	leaf     bool
	keys     []int
	children []*Node //lista pokazivaca na Nodeove koji su children
}

type Tree struct {
	root *Node
}

// helpers from stack overflow(neke sam i ja kucao)
func (node *Node) Contains(value int) bool {
	for _, item := range node.keys {
		if item == value {
			return true
		}
	}
	return false
}
func (node *Node) RemoveKeyFromNode(keyToRemove int) {
	var result []int

	for _, v := range node.keys {
		if v != keyToRemove {
			result = append(result, v)
		}
	}

	node.keys = result
}
func (node *Node) RemoveChildByIndex(index int) {
	if index < 0 || index >= len(node.children) {
		return // Index out of range, do nothing
	}

	node.children = append(node.children[:index], node.children[index+1:]...)
}
func (node *Node) GetChildToContinueDeletion(key int) int {
	for ind, val := range node.keys {
		if val > key {
			return ind
		}
	}
	return len(node.keys)
}
func (node *Node) findKeyIndex(key int) int {
	for i, v := range node.keys {
		if v == node.keys[i] {
			return i
		}
	}
	return -1 // Value not found in the list
}

// deletion functions
func (t *Tree) Delete(key int) {
	if t.root == nil {
		fmt.Print("Prazno stablo")
		return
	}

	var end bool = false
	x := t.root
	for end != true {
		if x.Contains(key) {
			if x.leaf {
				x.DeleteFromLeaf(key)
				end = true
			} else {
				x.DeleteFromNonLeaf(key)
				end = true
			}
		} else {
			if x.leaf {
				fmt.Print("Nema kljuca u stablu")
				return
			}

			childIndex := x.GetChildToContinueDeletion(key) //treba ti index koje dete ces da proveris da li ima dovoljno kljuceva
			if len(x.children[childIndex].keys) < q {
				x.FillNode(childIndex)
			}
			x = x.children[childIndex]
		}
	}

	//y = nil // mesto gde se cuva roditelj od x
	//
	//for x.Contains(key) != true {
	//	if x.leaf == true {
	//		//TODO nema kljuca
	//	} else {
	//		//TODO nadji dobrog childa -> proveri da li ima t kljuceva
	//		/* if nema min t kljuceva -> odredi nacin da spustis kljuc iz x dole(Fill) -> spusti se dole i zovi brisanje
	//		 */
	//	}
	//}
	//if x.leaf == true {
	//	if len(x.keys) >= q {
	//		x.DeleteFromLeaf(key)
	//	} else {
	//		x.DeleteFromLeaf(key)
	//
	//	}
	//} else {
	//	//TODO obrisi, a da nije leaf, drugacija fja
	//}
}

func (node *Node) DeleteFromLeaf(key int) {
	node.RemoveKeyFromNode(key)
}

func (node *Node) FillNode(index int) {
	if len(node.children[index-1].keys) >= q { //TODO a sta cemo sa decom ovde jel to predstavlja problem?
		node.insertNonFull(node.children[index-1].keys[len(node.children[index-1].keys)-1], nil)                  //uzmi poslednji(najveci) kljuc levog deteta i sibni gore
		node.children[index-1].RemoveKeyFromNode(node.children[index-1].keys[len(node.children[index-1].keys)-1]) //izbaci poslednji iz levog deteta
		node.children[index].insertNonFull(node.keys[index], nil)
		node.RemoveKeyFromNode(node.keys[index])
	} else if len(node.children[index+1].keys) >= q {
		node.insertNonFull(node.children[index+1].keys[0], nil) //isto ko gore samo za desni
		node.children[index+1].RemoveKeyFromNode(node.children[index+1].keys[0])
		node.children[index].insertNonFull(node.keys[index], nil)
		node.RemoveKeyFromNode(node.keys[index])
	} else {
		vrednostiKljucevaKojeSeBrisuIzParenta := []int{}
		for i := len(node.children[index-1].keys) - 1; i != -1; i-- {
			if i <= index {
				node.children[index].insertNonFull(node.keys[i], nil)
				vrednostiKljucevaKojeSeBrisuIzParenta = append(vrednostiKljucevaKojeSeBrisuIzParenta, node.keys[i])
			}
			node.children[index].insertNonFull(node.children[index-1].keys[i], nil)
			insertNodeArray(node.children, 0, node.children[index-1].children[i])
		}
		node.RemoveChildByIndex(index - 1)
		for _, vrKljucaZaBrisanjeIzParenta := range vrednostiKljucevaKojeSeBrisuIzParenta {
			node.RemoveKeyFromNode(vrKljucaZaBrisanjeIzParenta)
		}
	}
}

func (node *Node) DeleteFromNonLeaf(key int) {
	indexOfKeyToDelete := node.findKeyIndex(key)
	if len(node.children[indexOfKeyToDelete].keys) >= q { //leva strana
		node.keys[indexOfKeyToDelete] = node.children[indexOfKeyToDelete].keys[len(node.children[indexOfKeyToDelete].keys)]
		node.children[indexOfKeyToDelete].RemoveKeyFromNode(node.keys[indexOfKeyToDelete])
	} else if len(node.children[indexOfKeyToDelete+1].keys) >= q {
		node.keys[indexOfKeyToDelete] = node.children[indexOfKeyToDelete+1].keys[0]
		node.children[indexOfKeyToDelete+1].RemoveKeyFromNode(node.keys[indexOfKeyToDelete])
	} else {
		for i := len(node.children[indexOfKeyToDelete-1].keys) - 1; i != -1; i-- {
			node.children[indexOfKeyToDelete].insertNonFull(node.children[indexOfKeyToDelete-1].keys[i], nil)
			insertNodeArray(node.children, 0, node.children[indexOfKeyToDelete-1].children[i])
		}
		node.RemoveKeyFromNode(key)
		node.RemoveChildByIndex(indexOfKeyToDelete - 1)
		//TODO After merging if the parent node has less than the minimum number of keys then, look for the siblings as in Case I.
		//videti sutra jos ovo al nmg nista mi ovo ne vredi (sem da predjem u glavi ja to) dok ne resim insert...
	}
	//TODO ostao mi je i Case III koji nisam ni pogledao jer ga nema u pseudo kodu na ChatGPT, zato nez dal to treba da radim ovde uopste ili je vec pokriveno jer je ovo sve kombinacija sajta i ChatGPT
}

// helper functions from stack overflow
// insertuje na odredjeni index u arrayu, a ove ostale pomeri za jedno mesto desno 										-> =====> mogucnost optimizacije je slanje argumenata kao pokazivace...menjace i poziv same fje al to mzd na kraju
func (node *Node) insertIntArray(index int, value int, t *Tree) {
	if len(node.keys) == index { // nil or empty slice or after last element
		//if value == 7 {
		//	fmt.Println(t.root.children[1].keys[0])
		//	fmt.Println(t.root.children[0].keys[1])
		//	fmt.Println(t.root.children[0] == node)
		//	fmt.Println(t.root.children[1] == node)
		//}
		node.keys = append(node.keys, value)
		//if value == 7 {
		//	fmt.Println(t.root.children[1].keys[0])
		//}
	} else {
		node.keys = append(node.keys[:index+1], node.keys[index:]...) // index < len(a)
		node.keys[index] = value
	}
}

// TODO: kad ustanovis zasto ne valja insertIntArray moras promeniti i insertNodeArray
func insertNodeArray(a []*Node, index int, value *Node) []*Node { //insertuje na odredjeni index u arrayu samo za Node-ove
	if len(a) == index { // nil or empty slice or after last element
		return append(a, value)
	}
	a = append(a[:index+1], a[index:]...) // index < len(a)
	a[index] = value
	return a
}
func getAppropriateChildIndex(keys []int, key int) int { //vratice index deteta koje treba da proveravamo
	for i, num := range keys {
		if num > key {
			return i
		}
		if i+1 == len(keys) {
			return i + 1
		}
	}
	return -1 // ovo se nece nikada desiti ali se baca error ako ne napisem
}

// insertion functions
func (node *Node) insertNonFull(key int, t *Tree) int {
	i := len(node.keys)
	for i > 0 && node.keys[i-1] > key { //proveravaj od poslednjeg i smanjuj dok ne dodjes do potrebnog indexa
		i-- //ili pocetka niza
	}
	node.insertIntArray(i, key, t) //insertuj na to potrebno mesto
	return i
}

func (node *Node) splitCurrent(root bool, parent *Node) *Node {
	if root == true { //izmeni da parent stoji ovde a da se poziva sa ili nill ili da bude skroz prazan pa da ga ovde initujemo
		parent = &Node{leaf: false, keys: []int{node.keys[len(node.keys)/2]}}
		childLeft := &Node{leaf: true}
		childLeft.keys = node.keys[:(len(node.keys) / 2)]
		childRight := &Node{leaf: true}
		childRight.keys = node.keys[(len(node.keys)/2)+1:]
		parent.children = []*Node{childLeft, childRight}
		return parent
	} else {
		keyForParent := node.keys[len(node.keys)/2]

		childLeft := &Node{leaf: node.leaf}
		childLeft.keys = node.keys[:(len(node.keys) / 2)]
		childRight := &Node{leaf: node.leaf}
		childRight.keys = node.keys[(len(node.keys)/2)+1:]

		indexForChildren := parent.insertNonFull(keyForParent, nil) //deca idu na index parenta i index parenta plus 1
		insertNodeArray(parent.children, indexForChildren, childLeft)
		insertNodeArray(parent.children, indexForChildren+1, childRight)
		return nil
	}
}

func (t *Tree) Insert(key int) {
	if t.root == nil { //postavljas koren
		t.root = &Node{leaf: true, keys: []int{key}}
		return
	}
	if len(t.root.keys) == 2*q-1 { //koren je pun
		t.root = t.root.splitCurrent(true, t.root)
	}

	// odavde krece ubacivanje...split gore za root je tu samo da splituje on nije tu da ubaci nista novo
	x := t.root //privremeni node da ne bih pomerao koren kao

	for x.leaf == false { //isto sto i while hahahah
		index := getAppropriateChildIndex(x.keys, key)
		y := x.children[index]
		if (len(y.keys)) == 2*q-1 {
			y.splitCurrent(false, x)
			indexOfNextChild := getAppropriateChildIndex(x.keys, key)
			x = x.children[indexOfNextChild]
		} else {
			x = y
		}
	}
	x.insertNonFull(key, t)
}

func main() {
	var t Tree
	t.Insert(10)
	t.Insert(20)
	t.Insert(5)
	t.Insert(6)
	t.Insert(7) //stavi mi 7 i na desno dete i zameni vrednost 20 sa 7 ne razumem zasto?
	t.Insert(12)
	//t.Insert(8)
	//t.Insert(30)
	//t.Insert(7)
	//t.Insert(17)
}
