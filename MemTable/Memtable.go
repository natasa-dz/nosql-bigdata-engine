package MemTable

import (
	. "NAiSP/Log"
	"sort"
)

//trashold - granica/prag zapisa (< 100%)
//kapacitet strukture memtabla nije isto sto i ovaj trashold!!!
//poenta: trashold je procenat recimo da kazemo da je kapacitet b stabla 10elem i onda kazemo kad se popuni vise od 75% stabla ti flushuj na disk
//kada se trashold popuni Flushujemo na disk --> SSTable

type Memtable struct {
	size        uint
	trashold    float64
	tableStruct IMemtableStruct
}

func GenerateMemtable(kapacitetStrukture uint, pragZaFlush float64, imeStrukture string) *Memtable {
	table := Memtable{size: kapacitetStrukture, trashold: pragZaFlush}
	if imeStrukture == "btree" {
		table.tableStruct = CreateTree()
	} else {
		//TODO treba da se inicijalizuje skip lista
	}
	return &table
}

/*func (table *Memtable) Flush() {
	//size ce ici na 0 (ako planiramo da ih rotiramo, lakse ih je obrisati i uzeti sledeci iz liste)
	//treba da iz strukture uzmes i sortiras kljuceve
	//tako sortirane treba da uzmes i ubacis ih u SSTable
	//zatim treba da ga obrises
	//NAPOMENA: U VECOJ APSTRAKCIJI (WRITE PATH) TREBA DA IMAMO LISTU PRAZNIH MEMTABLOVA I KAKO SE JEDAN POPUNI TAKO SE TAJ FLUSHUJE I BRISE,
		//SLEDECI KRECE DA SE PUNI I TAKODJE SE PRAVI I JEDAN PRAZAN DA NE BI LISTA OSTALA BEZ MEMTABLOVA U JEDNOM MOMENTU
}*/

func (table *Memtable) Insert(data Log) {
	//if:
	//proveri da li postoji, ako postoji samo updatuj podatak i returnuj
	//else:
	//insertuj
	//proveri da li je popunjenost > trashold
	//jeste onda flushuj
	indexInNode, AddressOfNode := table.tableStruct.Search(string(data.Key))
	if AddressOfNode != nil {
		AddressOfNode.keys[indexInNode] = data
		return
	} else {
		table.tableStruct.Insert(data)
		if (float64(table.tableStruct.GetNumOfElements()) / float64(table.size)) > table.trashold {
			//unsortedData := table.tableStruct.GetAllLogs()
			//table.Flush() TODO Flushuj i zameni tabele ili isprazni kako god ti volja
		}
	}

}

// ///////////////////////////////////// PRIMER KAKO BI MOGAO OTRPILIKE IZGLEDATI FLUSH
//OSTAVIO SEBI DA POGLEDAM -- DUSAN(JA CU I OBRISATI)!!
/*
	func (table *Memtable) FlushTry() {
		elements := table.GetElements()
		generation := 0 // You might need a way to manage generation numbers

		// Create an SSTable and serialize it (for demonstration purposes, adjust as needed)
		sstable := &SSTable{
			Generation: generation,
			Elements:   elements,
		}
		sstable.Serialize("usertable", generation, "db")

		// Clear the Memtable
		table.tableStruct = CreateTree()
	}
*/

func SortData(entries []Log) []Log { //ISKORISTICU KASNIJE!!!
	// Sort the entries by Key
	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].Key) < string(entries[j].Key)
	})
	return entries
}
