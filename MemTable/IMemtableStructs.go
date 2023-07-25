package MemTable

//interfejs koji ce obuhvatiti b stablo i skip listu i omoguciti da se oba pozivaju preko ovoga

type IMemtableStruct interface {
	Insert(key int)
	Search(key int) (int, *Node)
	Delete(key int)
}

//kad se pravi napravimo objekat ovako: var struct IMemtableStruct := btreeConstructor/SkipListConstructor i dalje ga koristimo preko struct svuda
