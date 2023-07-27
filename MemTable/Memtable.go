package MemTable

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

func (table *Memtable) Flush() {
	//size ce ici na 0 (ako planiramo da ih rotiramo, lakse ih je obrisati i uzeti sledeci iz liste)
	//treba da iz strukture uzmes i sortiras kljuceve
	//tako sortirane treba da uzmes i ubacis ih u SSTable
	//kad provalim kako SSTable radi mozda se izmeni ovaj algoritam, ali svakako otprilike je ovako
	//mozda nije lose imati neku strukturu koja ce biti prelazna izmedju svih memtablestruktura (btree i skiplist) i SSTable struktova
}

func (table *Memtable) Insert(data Data) {
	//if:
	//proveri da li postoji, ako postoji samo updatuj podatak i returnuj
	//else:
	//insertuj
	//proveri da li je popunjenost > trashold
	//jeste onda flushuj
	indexInNode, AddressOfNode := table.tableStruct.Search(data.key)
	if AddressOfNode != nil {
		AddressOfNode.keys[indexInNode] = data
		return
	} else {
		table.tableStruct.Insert(data)
		if (float64(table.tableStruct.GetNumOfElements()) / float64(table.size)) > table.trashold {
			//table.Flush() TODO Flushuj i zameni tabele ili isprazni kako god ti volja
		}
	}

}

// ///////////////////////////////////// PRIMER KAKO BI MOGAO OTRPILIKE IZGLEDATI FLUSH

/*func (table *Memtable) FlushTry() {
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

func (table *Memtable) GetElements() []Data {
	elements := make([]Data, 0)
	iterator := table.tableStruct.GetIterator()
	for iterator.Next() {
		record := iterator.Value().(Data)
		if !record.Tombstone {
			elements = append(elements, record)
		}
	}
	return elements
}*/
