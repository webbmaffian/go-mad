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

	ch, err := channel.NewAckByteChannel("channel.bin", 10, 16, false, func(msg []byte, key uint64) bool {
		return true
	})

	if err != nil {
		log.Println(err)
		return
	}

	go func() {
		for {
			if err := ctx.Err(); err != nil {
				log.Println("a:", err)
				return
			}

			log.Println("a: awaiting message...")
			log.Println("a: received message:", ch.Read())
			// time.Sleep(2 * time.Second)
			log.Println("a: acknowledged", ch.AckRead(456))
			// break
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
			ok := ch.Write(func(b []byte) {
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
