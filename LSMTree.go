package main

//LSM stabla se sastoje od SSTableova?
//LSM je struktura podataka tako dizajnirana da jefitino indeksira datoteke koje imaju veliku stopu brisanja i dodavanja
//LSM ima prednost nad ostalim strukturama u ovoj ulozi zbog visoke stope pisanja u odnosu na stopu citanja
/*LSM stablo ima 2 ili vise nivoa. npr dvokomponentno stabloima komponentu C0 koja je u memoriji u potpunosti(manja) i C1 koja je na disku
podaci se prvo ubacuju u C0 (nalik nekog bafera za zapise), a onda se odatle ubacuju u C1(disk)*/
//broj nivoa ce opet ici kroz konfiguracioni fajl

//Po logici naseg projekta C0 je memtable, C1 je SSTable

//Kompakcije:
/*uzmes dva SSTable i spojis podatke (i onda ako za neki sa kljucem '12' npr u prvom SSTablu vrednost je 10 a u drugom 15 ti uzmes onu koja je
kasnije zapisana*/
