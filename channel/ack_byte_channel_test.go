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

func BenchmarkConcurrentWrite(b *testing.B) {
	const filepath = "bench1.bin"
	ch, err := NewAckByteChannel(filepath, b.N, 8, false, func(msg []byte, key uint64) bool {
		return true
	})

	if err != nil {
		b.Fatal(err)
	}

	go func() {
		for {
			b := ch.ReadAndAck()

			if b == nil {
				break
			}
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ch.Write(func(b []byte) {
			b[0] = 1
		})
	}

	b.StopTimer()
	ch.Close()
}

func BenchmarkConcurrentRead(b *testing.B) {
	const filepath = "bench1.bin"
	ch, err := NewAckByteChannel(filepath, b.N, 8, false, func(msg []byte, key uint64) bool {
		return true
	})

	if err != nil {
		b.Fatal(err)
	}

	go func() {
		for i := 0; i < b.N; i++ {
			ch.Write(func(b []byte) {
				b[0] = 1
			})
		}

		ch.DoneWriting()
	}()

	b.ResetTimer()

	for {
		b := ch.ReadAndAck()

		if b == nil {
			break
		}
	}

	b.StopTimer()
	ch.Close()
}

func BenchmarkConcurrentMultipleWrite(b *testing.B) {
	const filepath = "bench1.bin"
	ch, err := NewAckByteChannel(filepath, 1, 8, false, func(msg []byte, key uint64) bool {
		return true
	})

	if err != nil {
		b.Fatal(err)
	}

	go func() {
		for {
			b := ch.ReadAndAck()

			if b == nil {
				break
			}
		}
	}()

	b.SetParallelism(100)
	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			ch.Write(func(b []byte) {
				b[0] = 1
			})
		}
	})

	b.StopTimer()
	ch.Close()
}
