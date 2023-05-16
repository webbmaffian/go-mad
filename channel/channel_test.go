package channel

import (
	"testing"
)

func BenchmarkWrite(b *testing.B) {
	ch, err := NewAckByteChannel("bench1.bin", b.N, 8, false, func(msg []byte, key uint64) bool {
		return true
	})

	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		ch.Close()
	})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ch.Write(func(b []byte) {
			b[0] = 1
		})
	}
}

func BenchmarkReadWrite(b *testing.B) {
	ch, err := NewAckByteChannel("bench1.bin", b.N, 8, false, func(msg []byte, key uint64) bool {
		return true
	})

	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		ch.Close()
	})
	b.ResetTimer()

	for i := 0; i < b.N-2; i++ {
		ch.Write(func(b []byte) {
			b[0] = 1
		})
		b := ch.ReadAndAck()
		_ = b
	}
}

func BenchmarkReadWriteAck(b *testing.B) {
	ch, err := NewAckByteChannel("bench1.bin", b.N, 8, false, func(msg []byte, key uint64) bool {
		return true
	})

	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		ch.Close()
	})
	b.ResetTimer()

	for i := 0; i < b.N-2; i++ {
		ch.Write(func(b []byte) {
			b[0] = 1
		})
		b := ch.Read()
		_ = b
		ch.AckRead(123)
	}
}
