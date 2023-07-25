package main

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
	"fmt"
)

type LRUCache struct {
	n     int                   //velicina koja ce se namestati kroz konfiguracioni fajl
	cache map[int]*list.Element //key-int; value-pokazivac na element u listi(adresa tog elementa);
	list  *list.List            //dvostruko spregnuta lista, bice prikaz tog reda ustv(sadrzace elemente(parove) koji se sastoje od kljuca i vrednosti)
}

type Elem struct {
	key   int
	value int
}

func CreateCache(size int) LRUCache {
	cache := LRUCache{size, map[int]*list.Element{}, list.New()}
	return cache
}

func (cache *LRUCache) Insert(newValue, key int) {
	adrOfExistingElem, ok := cache.cache[key]
	if ok {
		cache.list.MoveToFront(adrOfExistingElem)
		adrOfExistingElem.Value.(*Elem).value = newValue
	} else {
		if cache.list.Len() == cache.n {
			keyToRemove := cache.list.Back().Value.(*Elem).key
			cache.list.Remove(cache.list.Back())
			delete(cache.cache, keyToRemove)
		}
		newElem := &Elem{key, newValue}
		adrOfNewElem := cache.list.PushFront(newElem) //vratice adresu ovog elementa u listi
		cache.cache[key] = adrOfNewElem
	}
}

func (cache *LRUCache) Search(key int) int {
	//proveri mapu, ako ima pomeri napred i vrati vrednost, ako nema vrati -1
	adrOfExistingElem, ok := cache.cache[key]
	if ok {
		cache.list.MoveToFront(adrOfExistingElem)
		return adrOfExistingElem.Value.(*Elem).value
	}
	return -1
}

func main() {
	c := CreateCache(2)
	c.Insert(1, 1)
	c.Insert(2, 2)
	fmt.Println(c.Search(1))
	fmt.Println(c.Search(5))
	c.Insert(3, 3)
	fmt.Println(c.Search(2))
	fmt.Println(c.Search(1))
	c.Insert(5, 5)
	fmt.Println(c.Search(3))

}
