package Cache

//struktura je nesto nalik queue koji ima ogranicenu kolicinu mesta, kad novi hoce da dodje izbaci ovog sa 'kraja'
/*ima recimo listu vrednosti koja radi kao ovaj queue, ali ispred toga ima hashmapu u kojoj na tim kljucevima cuva mesta gde se te vrednosti
nalaze u listi. takodje mana mu je sto su ovo tehnicki receno dve strukture podataka zapravo.
Konfiguracioni fajl je onaj koji ce da odredi velicinu reda!*/
//sto se tice Read Patha: proverava se memtable(skipList ili BTree), zatim cache....
/*Napomena: ovde stoje elementi koji su korisceni, znaci lupam ako ubacim nov element on ce doci na pocetak ovog reda ili ako se podataka
azurira ili cita on ce doci na pocetak ovog reda. Podatak se alocira i pre nego sto se vrati korisniku on se upise u cache, ovo nam omogucava
da kod sledece pretrage MOZDA prvo ne moramo traziti po disku(bio on alociran za brisanje, citanje ili ako se tek upisuje)
*/

//TODO jedna nejasnoca: svi su implementirali da lista sadrzi (key, value), a mapa [key] = adresa gde je (key, value) u listi,
//	pitanje je cemu to jer nam onda key ne treba u listi, imamo ga u mapi???

import (
	"container/list"
)

type LRUCache struct {
	n     int                      //velicina koja ce se namestati kroz konfiguracioni fajl
	cache map[string]*list.Element //key-int; value-pokazivac na element u listi(adresa tog elementa);
	list  *list.List               //dvostruko spregnuta lista, bice prikaz tog reda ustv(sadrzace elemente(parove) koji se sastoje od kljuca i vrednosti)
}

type Elem struct {
	key   string
	value []byte
}

func CreateCache(size int) LRUCache {
	cache := LRUCache{size, map[string]*list.Element{}, list.New()}
	return cache
}

func (cache *LRUCache) Insert(newValue []byte, key string) {
	adrOfExistingElem, ok := cache.cache[key]
	if ok { //element je vec u catchu
		cache.list.MoveToFront(adrOfExistingElem)        //posto je nesto radjeno sa njime pomeri ga na 'pocetak' kao najskorije koriscen elem
		adrOfExistingElem.Value.(*Elem).value = newValue //azuriraj vrednost posto je insert u pitanju
	} else { //elemen nije u catchu
		if cache.list.Len() == cache.n { //catch je pun u ovom trenutku
			keyToRemove := cache.list.Back().Value.(*Elem).key
			cache.list.Remove(cache.list.Back()) //izbaci ga iz liste
			delete(cache.cache, keyToRemove)     //izbaci ga iz mape
		}
		newElem := &Elem{key, newValue}
		adrOfNewElem := cache.list.PushFront(newElem) //novokreirani element ubaci u listu na 'pocetak'
		cache.cache[key] = adrOfNewElem               //ubaci ga u mapu
	}
}

func (cache *LRUCache) Search(key string) []byte {
	//proveri mapu, ako ima pomeri elem na 'pocetak' liste i vrati vrednost elementa, ako nema vrati nil
	adrOfExistingElem, ok := cache.cache[key]
	if ok {
		cache.list.MoveToFront(adrOfExistingElem)
		return adrOfExistingElem.Value.(*Elem).value
	}
	return nil
}

//func main() {
//	//TEST dok su bili intovi
//	c := CreateCache(2)
//	c.Insert(1, 1)
//	c.Insert(2, 2)
//	fmt.Println(c.Search(1))
//	fmt.Println(c.Search(5))
//	c.Insert(3, 3)
//	fmt.Println(c.Search(2))
//	fmt.Println(c.Search(1))
//	c.Insert(5, 5)
//	fmt.Println(c.Search(3))
//
//}
