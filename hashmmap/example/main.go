package main

import (
	"log"

	"github.com/webbmaffian/go-mad/hashmmap"
)

func main() {
	m, err := hashmmap.NewRaw[uint32, uint32]("test.db", 255)

	if err != nil {
		log.Fatal(err)
	}

	defer m.Close()

	m.Add(123, 456)
	m.Add(123, 789)

	log.Println(m.Len(), "items")

	iter := m.Iterate()

	for iter.Next() {
		key, val := iter.Key(), iter.Val()

		log.Println(key, "=", *val)
	}

	log.Println("tadaaa")
}
