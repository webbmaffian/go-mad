package channel

import (
	"testing"
)

func BenchmarkWrite(b *testing.B) {
	ch, err := NewAckByteChannel("bench1.bin", b.N, 8)

	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		ch.Close()
	})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ch.WriteOrFail(func(b []byte) {
			b[0] = 1
		})
	}
}

func BenchmarkReadWrite(b *testing.B) {
	ch, err := NewAckByteChannel("bench1.bin", b.N, 8)

	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		ch.Close()
	})
	b.ResetTimer()

	for i := 0; i < b.N-2; i++ {
		ch.WriteOrFail(func(b []byte) {
			b[0] = 1
		})
		_, _ = ch.ReadAndAckOrFail()
	}
}

func BenchmarkReadWriteAck(b *testing.B) {
	ch, err := NewAckByteChannel("bench1.bin", b.N, 8)

	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		ch.Close()
	})
	b.ResetTimer()

	for i := 0; i < b.N-2; i++ {
		ch.WriteOrFail(func(b []byte) {
			b[0] = 1
		})
		_, _ = ch.ReadOrFail()
		ch.AckAll()
	}
}

func BenchmarkConcurrentWrite(b *testing.B) {
	const filepath = "bench1.bin"
	ch, err := NewAckByteChannel(filepath, b.N, 8)

	if err != nil {
		b.Fatal(err)
	}

	go func() {
		for {
			_, ok := ch.ReadAndAckOrFail()

			if !ok {
				break
			}
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ch.WriteOrFail(func(b []byte) {
			b[0] = 1
		})
	}

	b.StopTimer()
	ch.Close()
}

func BenchmarkConcurrentRead(b *testing.B) {
	const filepath = "bench1.bin"
	ch, err := NewAckByteChannel(filepath, b.N, 8)

	if err != nil {
		b.Fatal(err)
	}

	go func() {
		for i := 0; i < b.N; i++ {
			ch.WriteOrFail(func(b []byte) {
				b[0] = 1
			})
		}

		ch.CloseWriting()
	}()

	b.ResetTimer()

	for {
		_, ok := ch.ReadAndAckOrFail()

		if !ok {
			break
		}
	}

	b.StopTimer()
	ch.Close()
}

func BenchmarkConcurrentMultipleWrite(b *testing.B) {
	const filepath = "bench1.bin"
	ch, err := NewAckByteChannel(filepath, 1, 8)

	if err != nil {
		b.Fatal(err)
	}

	go func() {
		for {
			_, ok := ch.ReadAndAckOrFail()

			if !ok {
				break
			}
		}
	}()

	b.SetParallelism(100)
	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			ch.WriteOrFail(func(b []byte) {
				b[0] = 1
			})
		}
	})

	b.StopTimer()
	ch.Close()
}
