package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/webbmaffian/go-mad/channel"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	ch, err := channel.NewAckByteChannel("channel.bin", 10, 16)

	if err != nil {
		log.Println(err)
		return
	}

	go func() {
		for {
			log.Println("a: awaiting message...")

			b, ok := ch.ReadAndAckOrFail()

			if !ok {
				return
			}

			log.Println("a: received message:", b)
		}
	}()

	go func() {
		var i uint8

		for {
			if err := ctx.Err(); err != nil {
				log.Println("b:", err)
				return
			}

			// log.Println("b: sending message...")
			ok := ch.WriteOrFail(func(b []byte) {
				b[0] = i
			})

			i++

			if !ok {
				log.Println("b: NOT sent", i)
				// return
			} else {
				log.Println("b: sent", i)
			}

			if i == 255 {
				return
			}

			time.Sleep(1 * time.Second)
		}
	}()

	<-ctx.Done()

	if err := ch.Close(); err != nil {
		log.Println(err)
	}
}
