package MemTable

//interfejs koji ce obuhvatiti b stablo i skip listu i omoguciti da se oba pozivaju preko ovoga

type IMemtableStruct interface {
	Insert(key Data)
	Search(key string) (int, *Node) //vraca pokazivac na node gde je kljuc i index gde se nalazi kljuc u tom Nodu1
	Delete(key string)
}

//kad se pravi napravimo objekat ovako: var struct IMemtableStruct := btreeConstructor/SkipListConstructor i dalje ga koristimo preko struct svuda
