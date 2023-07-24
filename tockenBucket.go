package main

import (
	"fmt"
	"time"
)

const n = 5                     //broj tokena max dozvoljenih, resetuje se i ucitava se iz konfiguracionog fajla kasnije
const timeToReset = time.Minute //vremenski period na koji ce se broj tokena resetovati

type TockenBucket struct {
	TokensLeft int
	lastReset  time.Time
}

func createBucket() TockenBucket {
	bucket := TockenBucket{TokensLeft: n, lastReset: time.Now()}
	return bucket
}

func (bucket *TockenBucket) RefillBucket() {
	bucket.TokensLeft = n
	bucket.lastReset = time.Now()
}

func (bucket *TockenBucket) MakeRequest() bool {
	if (time.Now().Sub(bucket.lastReset)) > timeToReset {
		bucket.RefillBucket()
		bucket.TokensLeft -= 1
		return true
	} else {
		if bucket.TokensLeft > 0 {
			bucket.TokensLeft -= 1
			return true
		}
		return false
	}
}

func main() {
	bucket := createBucket()
	for i := 0; i != 7; i++ {
		fmt.Println(bucket.MakeRequest())
	}
	time.Sleep(time.Minute)
	fmt.Println("prosao minut")
	for i := 0; i != 7; i++ {
		fmt.Println(bucket.MakeRequest())
	}

}
