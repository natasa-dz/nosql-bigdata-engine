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

import (
	. "NAiSP/Log"
	"container/list"
)

type LRUCache struct {
	n     int                      //velicina koja ce se namestati kroz konfiguracioni fajl
	cache map[string]*list.Element //key-int; value-pokazivac na element u listi(adresa tog elementa);
	list  *list.List               //dvostruko spregnuta lista, bice prikaz tog reda ustv(sadrzace elemente(parove) koji se sastoje od kljuca i vrednosti)
}

func CreateCache(size int) *LRUCache {
	cache := LRUCache{size, map[string]*list.Element{}, list.New()}
	return &cache
}

func (cache *LRUCache) Insert(newLog *Log) {
	adrOfExistingElem, ok := cache.cache[string(newLog.Key)]
	if ok { //element je vec u catchu
		cache.list.MoveToFront(adrOfExistingElem)           //posto je nesto radjeno sa njime pomeri ga na 'pocetak' kao najskorije koriscen elem
		adrOfExistingElem.Value.(*Log).Value = newLog.Value //azuriraj vrednost posto je insert u pitanju
	} else { //elemen nije u catchu
		if cache.list.Len() == cache.n { //catch je pun u ovom trenutku
			keyToRemove := cache.list.Back().Value.(*Log).Key
			cache.list.Remove(cache.list.Back())     //izbaci ga iz liste
			delete(cache.cache, string(keyToRemove)) //izbaci ga iz mape
		}
		adrOfNewElem := cache.list.PushFront(newLog)   //novokreirani element ubaci u listu na 'pocetak'
		cache.cache[string(newLog.Key)] = adrOfNewElem //ubaci ga u mapu
	}
}

func (cache *LRUCache) Search(key string) []byte {
	//proveri mapu, ako ima pomeri elem na 'pocetak' liste i vrati vrednost elementa, ako nema vrati nil
	adrOfExistingElem, ok := cache.cache[key]
	if ok {
		cache.list.MoveToFront(adrOfExistingElem)
		return adrOfExistingElem.Value.(*Log).Value
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
