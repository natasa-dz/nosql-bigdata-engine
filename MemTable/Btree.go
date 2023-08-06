package MemTable

import (
	Log "NAiSP/Log"
)

// konstanta t je neki stepen ovog drveta koji se odredjuje iz konfiguracionog fajla
//max broj kljuceva u cvoru: 2*q-1

type Node struct {
	leaf     bool
	keys     []Log.Log
	children []*Node //lista pokazivaca na Nodeove koji su children
}

type Tree struct {
	root      *Node
	numOfData uint
	Degree    int
}

func CreateTree(Degree int) *Tree {
	t := Tree{root: nil, numOfData: 0, Degree: Degree}
	return &t
}

func (t *Tree) Empty() {
	t.root = nil
	t.numOfData = 0
}

func (t *Tree) GetNumOfElements() uint {
	return t.numOfData
}

// Insertuje na odredjeni index u arrayu, a ove ostale pomeri za jedno mesto desno
func (node *Node) InsertDataIntoArray(index int, data Log.Log) {
	if len(node.keys) == index { //stavljamo ga na kraj
		list := make([]Log.Log, len(node.keys)+1)
		copy(list, node.keys)
		list[len(node.keys)] = data
		node.keys = list
	} else { //ako ide negde pre kraja odredjeni se pomeraju udesno da bi napravili mesto
		node.keys = append(node.keys[:index+1], node.keys[index:]...)
		node.keys[index] = data
	}
}

func (node *Node) InsertNodeIntoArray(index int, value *Node) { //Ista fja ko gore samo za Node-ove
	if len(node.children) == index {
		node.children = append(node.children, value)
		return
	}
	node.children = append(node.children[:index+1], node.children[index:]...)
	node.children[index] = value
}

func (node *Node) getAppropriateChildIndex(key string) int { //vratice index deteta koje treba da proveravamo dalje
	for i, num := range node.keys {
		if string(num.Key) > key {
			return i
		}
		if i+1 == len(node.keys) {
			return i + 1
		}
	}
	return -1 // nece se desiti
}
func (node *Node) Contains(key string) int { //ako cvor sadrzi kljuc vratice index gde se kljuc nalazi u Nodu
	for ind, item := range node.keys {
		if string(item.Key) == key {
			return ind
		}
	}
	return -1
}

// Insertion functions
func (node *Node) InsertNonFull(data Log.Log) int {
	i := len(node.keys)
	for i > 0 && string(node.keys[i-1].Key) > string(data.Key) { //proveravaj od poslednjeg i smanjuj dok ne dodjes do indexa na koji ces da insertujes Data
		i--
	}
	node.InsertDataIntoArray(i, data) //Insertuj na taj index koji si odredio
	return i
}

func (node *Node) splitCurrent(root bool, parent *Node) *Node {
	if root == true { //ako je u pitanju root malo je drugacije jer stvaramo potpuno novog parenta dok ako nije root samo dajemo cvoru iznad
		parent = &Node{leaf: false, keys: []Log.Log{node.keys[len(node.keys)/2]}}
		childLeft := &Node{leaf: node.children == nil}     //trenutni cvor, posto je pun se deli. Srednji elem u njemu ce ici 'gore'
		childLeft.keys = node.keys[:(len(node.keys) / 2)]  //u novi parent Node, a ostatak tako podeljen ce biti levo i desno dete.
		childRight := &Node{leaf: node.children == nil}    //deca ce se takodje rasporediti po toj podeli gde ce levo dete kljuca
		childRight.keys = node.keys[(len(node.keys)/2)+1:] //koji je otisao gore(3. nivo, ovo ce mu posle operacije biti unuk tehnicki)
		if node.children != nil {                          //postati dete levog, a desno desnog.
			childLeft.children = node.children[:len(node.keys)/2+1]
			childRight.children = node.children[len(node.keys)/2+1:]
		}
		parent.children = []*Node{childLeft, childRight}
		return parent
	} else {
		keyForParent := node.keys[len(node.keys)/2]        //uzmemo srednji element koji ce da ide u cvor gore(koji sigurno nije pun jer
		childLeft := &Node{leaf: node.leaf}                //da je bio pun bio bi podeljen prilikom silaska na dole po ovom algoritmu
		childLeft.keys = node.keys[:(len(node.keys) / 2)]  //kljucevi se podele na levi i na desni cvor nakon odlaska srednjeg gore
		childRight := &Node{leaf: node.leaf}               //ova dva novonastala cvora odlaskom gore se dodaju u decu parenta na
		childRight.keys = node.keys[(len(node.keys)/2)+1:] //potrebno mesto, a njihova deca (3. nivo) se rasporedjuje kako treba)
		indexForChildren := parent.InsertNonFull(keyForParent)
		parent.children[indexForChildren] = childLeft
		parent.InsertNodeIntoArray(indexForChildren+1, childRight)
		if node.children != nil {
			childLeft.children = node.children[:len(node.keys)/2+1]
			childRight.children = node.children[len(node.keys)/2+1:]
		}
		return parent
	}
}

func (t *Tree) Insert(data Log.Log) {

	if t.root == nil { //postavljas koren == bice u prvoj iteraciji
		t.root = &Node{leaf: true, keys: []Log.Log{data}}
		t.numOfData = 1
		return
	}
	if len(t.root.keys) == 2*t.Degree-1 { //koren je pun, znaci mora split pre daljeg nastavka
		t.root = t.root.splitCurrent(true, t.root)
	}

	x := t.root //privremeni node da ne bih pomerao koren

	for x.leaf == false { //isto sto i while hahahah
		index := x.getAppropriateChildIndex(string(data.Key))
		y := x.children[index] //dete na kojem bi trebalo dalje dole da idemo prema listovima
		if (len(y.keys)) == 2*t.Degree-1 {
			x = y.splitCurrent(false, x) //ako je to dete puno mora prvo da se splituje pre nastavka spustanja
			indexOfNextChild := x.getAppropriateChildIndex(string(data.Key))
			x = x.children[indexOfNextChild] //onda biramo jedno od dva novonastala deteta (podelom y)
		} else {
			x = y //samo idemo dole jer y nije pun
		}
	}
	x.InsertNonFull(data) //kad smo stigli do lista samo insertuj
	t.numOfData += 1
}

func (t *Tree) Search(key string) (int, *Node) { //retVal je index u Node(keys) na kom se nalazi trazena vrednost, -1 ako nema vrednosti
	if t.root == nil {
		return -1, nil
	}

	x := t.root
	for x.leaf != true {
		indexOfSearchedKey := x.Contains(key)
		if indexOfSearchedKey != -1 {
			return indexOfSearchedKey, x
		} else {
			indexOfChildToContinue := x.getAppropriateChildIndex(key)
			x = x.children[indexOfChildToContinue]
		}
	}

	indexOfSearchedKey := x.Contains(key)
	if indexOfSearchedKey != -1 {
		return indexOfSearchedKey, x
	}
	return -1, nil
}

func (t *Tree) Delete(key string) bool { //samo logicko brisanje izmenice tombstone, nece zapravo obrisati iz stabla
	indexInNode, node := t.Search(key)
	if node != nil {
		node.keys[indexInNode].Tombstone = false
		return true
	}
	return false
}

func (t *Tree) Traverse(node *Node) []*Node {
	var retVal []*Node

	if node.leaf {
		return append(retVal, node)
	}

	for !node.leaf {
		for i := 0; i != len(node.children); i++ {
			retVal = append(retVal, t.Traverse(node.children[i])...)
		}
		break
	}

	retVal = append(retVal, node)
	return retVal
}

func (t *Tree) GetAllNodes() []*Node {
	var allNodes []*Node
	allNodes = t.Traverse(t.root)
	return allNodes
}

func (t *Tree) GetAllLogs() []*Log.Log {
	allNodes := t.GetAllNodes()
	var allLogs []*Log.Log
	for _, node := range allNodes {
		for _, log := range (*node).keys {
			l := log
			allLogs = append(allLogs, &l)
		}
	}
	return allLogs
}

//func main() {
//TEST1
//var t Tree
//t.Insert(10)
//t.Insert(20)
//t.Insert(5)
//t.Insert(6)
//t.Insert(7) //stavi mi 7 i na desno dete i zameni vrednost 20 sa 7 ne razumem zasto?
//t.Insert(12)
//t.Insert(8)
//t.Insert(30)
//t.Insert(7)
//t.Insert(17)
//TEST 2
//t.Insert(10)
//t.Insert(20)
//t.Insert(30)
//t.Insert(40)
//t.Insert(50)
//t.Insert(60)
//t.Insert(70)
//t.Insert(80)
//t.Insert(90)
//t.Insert(100)
//}
