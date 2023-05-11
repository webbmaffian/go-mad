package main

import (
	"log"

	"github.com/webbmaffian/go-mad/matrix"
)

func main() {
	m, err := matrix.NewSym[float64]("test.db", 100)

	if err != nil {
		log.Fatal(err)
	}

	defer m.Close()

	// m.Set(1, 2, 345.678)

	log.Println(*m.Get(2, 1))
	log.Println(m.Dims())
}
