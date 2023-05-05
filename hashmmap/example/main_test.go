package main

import (
	"os"
	"testing"

	"github.com/webbmaffian/go-mad/hashmmap"
)

type val [256]byte

func BenchmarkAdd(b *testing.B) {
	const filename = "bench.db"
	m, err := hashmmap.NewRaw[uint64, val](filename, uint64(b.N))

	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		m.Close()
		os.Remove(filename)
	})

	var v val

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Add(uint64(i), v)
	}
}

func BenchmarkFind(b *testing.B) {
	const filename = "bench.db"
	m, err := hashmmap.NewRaw[uint64, val](filename, uint64(b.N))

	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		m.Close()
		os.Remove(filename)
	})

	var v val

	for i := 0; i < b.N; i++ {
		m.Add(uint64(i), v)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iter := m.Find(uint64(i))

		for iter.Next() {
			_ = iter.Key()
		}
	}
}

func BenchmarkFindRO(b *testing.B) {
	const filename = "bench.db"
	m, err := hashmmap.NewRaw[uint64, val](filename, uint64(b.N))

	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		m.Close()
		os.Remove(filename)
	})

	var v val

	for i := 0; i < b.N; i++ {
		m.Add(uint64(i), v)
	}

	if err = m.Close(); err != nil {
		b.Fatal(err)
	}

	m, err = hashmmap.OpenRawRO[uint64, val](filename)

	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iter := m.Find(uint64(i))

		for iter.Next() {
			_ = iter.Key()
		}
	}
}
