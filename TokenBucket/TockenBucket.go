package TokenBucket

import (
	"time"
)

//struktura koja sprecava da se baci milion zahteva u sekundi vec lupam smemo da posaljemo 5 zahteva kroz minut i vise od toga nam ne dozvoljava

//broj tokena max dozvoljenih, ucitati iz konfiguracionog fajla
//vremenski period na koji ce se broj tokena resetovati, isto iz konfiguracionog

type TockenBucket struct {
	TokensLeft       int //koliko mu je tokena ostalo
	maxNumOfTokens   int
	lastReset        time.Time //poslednji timestamp kad je resetovan broj tokena koje ima
	durationForReset time.Duration
}

func CreateBucket(maxNumOfTokens int, durationForReset time.Duration) *TockenBucket {
	bucket := TockenBucket{TokensLeft: maxNumOfTokens, lastReset: time.Now(), durationForReset: durationForReset, maxNumOfTokens: maxNumOfTokens}
	return &bucket
}

func (bucket *TockenBucket) refillBucket() { //fja koja ce da resetuje broj tokena
	bucket.TokensLeft = bucket.maxNumOfTokens
	bucket.lastReset = time.Now() //zapis kad smo poslednji put resetovali
}

func (bucket *TockenBucket) MakeRequest() bool { //provera da li ce se zahtev odobriti ili nece
	if (time.Now().Sub(bucket.lastReset)) > bucket.durationForReset { //ako je proslo dovoljno vremena, automatski resetuj
		bucket.refillBucket()
		bucket.TokensLeft -= 1 //i kad si resetovao smanji broj zahteva za jedan jer si ovaj odobrio
		return true
	} else {
		if bucket.TokensLeft > 0 { //jos ne treba da resetuje znaci proveri da li imamo jos slobodnih zahteva
			bucket.TokensLeft -= 1 //i u zavisnosti od toga odobri/odbij
			return true
		}
		return false
	}
}

//func main() {
//	bucket := createBucket()
//	for i := 0; i != 7; i++ {
//		fmt.Println(bucket.MakeRequest())
//	}
//	time.Sleep(time.Minute)
//	fmt.Println("prosao minut")
//	for i := 0; i != 7; i++ {
//		fmt.Println(bucket.MakeRequest())
//	}
//
//}
