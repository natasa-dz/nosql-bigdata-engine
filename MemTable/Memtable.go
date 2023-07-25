package MemTable

//trashold - granica/prag zapisa
//kapacitet strukture memtabla nije isto sto i ovaj trashold!!!
//poenta: trashold je procenat recimo da kazemo da je kapacitet b stabla 10elem i onda kazemo kad se popuni vise od 75% stabla ti flushuj na disk
//kada se trashold popuni Flushujemo na disk --> SSTable

type Memtable struct {
	size        int
	trashold    float64
	tableStruct IMemtableStruct
}

func GenerateMemtable(kapacitetStrukture int, pragZaFlush float64, imeStrukture string) *Memtable {
	table := Memtable{size: kapacitetStrukture, trashold: pragZaFlush}
	if imeStrukture == "btree" {
		table.tableStruct = CreateTree()
	} else {
		//TODO treba da se inicijalizuje skip lista
	}
	return &table
}

func Insert() {
	//if:
	//proveri da li postoji, ako postoji samo updatuj podatak i returnuj
	//else:
	//insertuj
	//proveri da li je popunjenost > trashold
	//jeste onda flushuj
}
