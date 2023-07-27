package MemTable

const q int = 2

// konstanta t je neki stepen ovog drveta koji se odredjuje iz konfiguracionog fajla
//max broj kljuceva u cvoru: 2*q-1

type Node struct {
	leaf     bool
	keys     []Data
	children []*Node //lista pokazivaca na Nodeove koji su children
}

type Data struct {
	key       string
	value     []byte
	tombstone bool //active-inactive
}

type Tree struct {
	root      *Node
	numOfData uint
}

func CreateTree() *Tree {
	t := Tree{root: nil, numOfData: 0}
	return &t
}

func (t *Tree) GetNumOfElements() uint {
	return t.numOfData
}

// Insertuje na odredjeni index u arrayu, a ove ostale pomeri za jedno mesto desno
func (node *Node) InsertDataIntoArray(index int, data Data) {

	if len(node.keys) == index { //stavljamo ga na kraj

		// MOJ WORKAROUND BUGA KOJI NEMAM BLAGE ZASTO SE DESAVA??

		//TODO: videti sa nekim, nez dal ce ovo praviti problem na nekim drugim mestima gde koristim append koji mi je pravio problem
		//		i da li je ovo resilo ceo problem ili radi samo za ovaj case iz nekog nepoznatog razloga, takodje sta raditi sa istom ovom
		//		fjom ali za Node-ove
		list := make([]Data, len(node.keys)+1)
		copy(list, node.keys)
		list[len(node.keys)] = data
		node.keys = list
		//******************KOD KOJI JE BIO RANIJE I PRAVIO ERROR NA InsertOVANJU 7 PRI REDOSLEDU(10,20,5,6,7)
		//node.keys = append(node.keys, value)

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
		if num.key > key {
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
		if item.key == key {
			return ind
		}
	}
	return -1
}

// Insertion functions
func (node *Node) InsertNonFull(data Data) int {

	i := len(node.keys)
	for i > 0 && node.keys[i-1].key > data.key { //proveravaj od poslednjeg i smanjuj dok ne dodjes do indexa na koji ces da insertujes Data
		i--
	}
	node.InsertDataIntoArray(i, data) //Insertuj na taj index koji si odredio
	return i
}

// TODO moglo bi se refaktorisati...
func (node *Node) splitCurrent(root bool, parent *Node) *Node {

	if root == true { //ako je u pitanju root malo je drugacije jer stvaramo potpuno novog parenta dok ako nije root samo dajemo cvoru iznad

		parent = &Node{leaf: false, keys: []Data{node.keys[len(node.keys)/2]}}
		childLeft := &Node{leaf: node.children == nil}     //trenutni cvor, posto je pun se deli. Srednji elem u njemu ce ici 'gore'
		childLeft.keys = node.keys[:(len(node.keys) / 2)]  //u novi parent Node, a ostatak tako podeljen ce biti levo i desno dete.
		childRight := &Node{leaf: node.children == nil}    //deca ce se takodje rasporediti po toj podeli gde ce levo dete kljuca
		childRight.keys = node.keys[(len(node.keys)/2)+1:] //koji je otisao gore(3. nivo, ovo ce mu posle operacije biti unuk tehnicki)

		if node.children != nil { //postati dete levog, a desno desnog.
			childLeft.children = node.children[:len(node.keys)/2+1]
			childRight.children = node.children[len(node.keys)/2+1:]
		}

		parent.children = []*Node{childLeft, childRight}
		return parent
	} else {

		keyForParent := node.keys[len(node.keys)/2]       //uzmemo srednji element koji ce da ide u cvor gore(koji sigurno nije pun jer
		childLeft := &Node{leaf: node.leaf}               //da je bio pun bio bi podeljen prilikom silaska na dole po ovom algoritmu
		childLeft.keys = node.keys[:(len(node.keys) / 2)] //kljucevi se podele na levi i na desni cvor nakon odlaska srednjeg gore

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

func (t *Tree) Insert(data Data) {

	indexInNode, nodePointer := t.Search(data.key) //pretrazi stablo da vidis da li ga sadrzi, ako sadrzi == samo update

	if indexInNode != -1 {
		nodePointer.keys[indexInNode].value = data.value
		return
	}

	if t.root == nil { //postavljas koren == bice u prvoj iteraciji

		t.root = &Node{leaf: true, keys: []Data{data}}
		t.numOfData = 1
		return
	}
	if len(t.root.keys) == 2*q-1 { //koren je pun, znaci mora split pre daljeg nastavka
		t.root = t.root.splitCurrent(true, t.root)
	}

	x := t.root //privremeni node da ne bih pomerao koren

	for x.leaf == false { //isto sto i while hahahah

		index := x.getAppropriateChildIndex(data.key)
		y := x.children[index] //dete na kojem bi trebalo dalje dole da idemo prema listovima
		if (len(y.keys)) == 2*q-1 {
			x = y.splitCurrent(false, x) //ako je to dete puno mora prvo da se splituje pre nastavka spustanja
			indexOfNextChild := x.getAppropriateChildIndex(data.key)
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

func (t *Tree) Delete(key string) { //samo logicko brisanje izmenice tombstone, nece zapravo obrisati iz stabla
	indexInNode, node := t.Search(key)
	if node != nil {
		node.keys[indexInNode].tombstone = false
	}
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

// ==================================================DELETION=======================================================================

// =====================OVAJ DEO CE VEROVATNO BITI TOTALNO NEKORISCEN I NEPOTREBAN(PROCITAJ LINIJU ISPOD)==================================
//AKO OVO BUDES TREBAO DA OTKOMENTARISES PRVO U OVOM OGROMNOM ZAKOMENTARISANOM KODU PRETRAZI 'URADITI:' I ZAMENI GA SA 'T O D O'
//IZMENIO SAM TA DVA DA MI NE BI STAJALO U TASK LISTI STVARI KOJE NE TREBA DA RADIM
// helpers from stack overflow(neke sam i ja kucao)
//func (node *Node) RemoveKeyFromNode(keyToRemove int) {
//	var result []int
//
//	for _, v := range node.keys {
//		if v != keyToRemove {
//			result = append(result, v)
//		}
//	}
//
//	node.keys = result
//}
//func (node *Node) RemoveChildByIndex(index int) {
//	if index < 0 || index >= len(node.children) {
//		return // Index out of range, do nothing
//	}
//
//	node.children = append(node.children[:index], node.children[index+1:]...)
//}
//func (node *Node) GetChildToContinueDeletion(key int) int {
//	for ind, val := range node.keys {
//		if val > key {
//			return ind
//		}
//	}
//	return len(node.keys)
//}
//func (node *Node) findKeyIndex(key int) int {
//	for i, v := range node.keys {
//		if v == node.keys[i] {
//			return i
//		}
//	}
//	return -1 // Value not found in the list
//}
//
//// deletion functions
//func (t *Tree) Delete(key int) {
//	if t.root == nil {
//		fmt.Print("Prazno stablo")
//		return
//	}
//
//	var end bool = false
//	x := t.root
//	for end != true {
//		if x.Contains(key) {
//			if x.leaf {
//				x.DeleteFromLeaf(key)
//				end = true
//			} else {
//				x.DeleteFromNonLeaf(key)
//				end = true
//			}
//		} else {
//			if x.leaf {
//				fmt.Print("Nema kljuca u stablu")
//				return
//			}
//
//			childIndex := x.GetChildToContinueDeletion(key) //treba ti index koje dete ces da proveris da li ima dovoljno kljuceva
//			if len(x.children[childIndex].keys) < q {
//				x.FillNode(childIndex)
//			}
//			x = x.children[childIndex]
//		}
//	}
//
//	//y = nil // mesto gde se cuva roditelj od x
//	//
//	//for x.Contains(key) != true {
//	//	if x.leaf == true {
//	//		//URADITI: nema kljuca
//	//	} else {
//	//		//URADITI: nadji dobrog childa -> proveri da li ima t kljuceva
//	//		/* if nema min t kljuceva -> odredi nacin da spustis kljuc iz x dole(Fill) -> spusti se dole i zovi brisanje
//	//		 */
//	//	}
//	//}
//	//if x.leaf == true {
//	//	if len(x.keys) >= q {
//	//		x.DeleteFromLeaf(key)
//	//	} else {
//	//		x.DeleteFromLeaf(key)
//	//
//	//	}
//	//} else {
//	//	//URADITI: obrisi, a da nije leaf, drugacija fja
//	//}
//}
//
//func (node *Node) DeleteFromLeaf(key int) {
//	node.RemoveKeyFromNode(key)
//}
//
//func (node *Node) FillNode(index int) {
//	if len(node.children[index-1].keys) >= q { //URADITI: a sta cemo sa decom ovde jel to predstavlja problem?
//		node.InsertNonFull(node.children[index-1].keys[len(node.children[index-1].keys)-1], nil)                  //uzmi poslednji(najveci) kljuc levog deteta i sibni gore
//		node.children[index-1].RemoveKeyFromNode(node.children[index-1].keys[len(node.children[index-1].keys)-1]) //izbaci poslednji iz levog deteta
//		node.children[index].InsertNonFull(node.keys[index], nil)
//		node.RemoveKeyFromNode(node.keys[index])
//	} else if len(node.children[index+1].keys) >= q {
//		node.InsertNonFull(node.children[index+1].keys[0], nil) //isto ko gore samo za desni
//		node.children[index+1].RemoveKeyFromNode(node.children[index+1].keys[0])
//		node.children[index].InsertNonFull(node.keys[index], nil)
//		node.RemoveKeyFromNode(node.keys[index])
//	} else {
//		vrednostiKljucevaKojeSeBrisuIzParenta := []int{}
//		for i := len(node.children[index-1].keys) - 1; i != -1; i-- {
//			if i <= index {
//				node.children[index].InsertNonFull(node.keys[i], nil)
//				vrednostiKljucevaKojeSeBrisuIzParenta = append(vrednostiKljucevaKojeSeBrisuIzParenta, node.keys[i])
//			}
//			node.children[index].InsertNonFull(node.children[index-1].keys[i], nil)
//			node.InsertNodeArray(0, node.children[index-1].children[i])
//		}
//		node.RemoveChildByIndex(index - 1)
//		for _, vrKljucaZaBrisanjeIzParenta := range vrednostiKljucevaKojeSeBrisuIzParenta {
//			node.RemoveKeyFromNode(vrKljucaZaBrisanjeIzParenta)
//		}
//	}
//}
//
//func (node *Node) DeleteFromNonLeaf(key int) {
//	indexOfKeyToDelete := node.findKeyIndex(key)
//	if len(node.children[indexOfKeyToDelete].keys) >= q { //leva strana
//		node.keys[indexOfKeyToDelete] = node.children[indexOfKeyToDelete].keys[len(node.children[indexOfKeyToDelete].keys)]
//		node.children[indexOfKeyToDelete].RemoveKeyFromNode(node.keys[indexOfKeyToDelete])
//	} else if len(node.children[indexOfKeyToDelete+1].keys) >= q {
//		node.keys[indexOfKeyToDelete] = node.children[indexOfKeyToDelete+1].keys[0]
//		node.children[indexOfKeyToDelete+1].RemoveKeyFromNode(node.keys[indexOfKeyToDelete])
//	} else {
//		for i := len(node.children[indexOfKeyToDelete-1].keys) - 1; i != -1; i-- {
//			node.children[indexOfKeyToDelete].InsertNonFull(node.children[indexOfKeyToDelete-1].keys[i], nil)
//			node.InsertNodeArray(0, node.children[indexOfKeyToDelete-1].children[i])
//		}
//		node.RemoveKeyFromNode(key)
//		node.RemoveChildByIndex(indexOfKeyToDelete - 1)
//		//URADITI: After merging if the parent node has less than the minimum number of keys then, look for the siblings as in Case I.
//		//videti sutra jos ovo al nmg nista mi ovo ne vredi (sem da predjem u glavi ja to) dok ne resim Insert...
//	}
//	//URADITI: ostao mi je i Case III koji nisam ni pogledao jer ga nema u pseudo kodu na ChatGPT, zato nez dal to treba da radim ovde uopste ili je vec pokriveno jer je ovo sve kombinacija sajta i ChatGPT
//}
