package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/webbmaffian/go-mad/channel"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	ch, err := channel.NewAckByteChannel("channel.bin", 10, 8)

	if err != nil {
		log.Println(err)
		return
	}

	go runServer(ch)
	go runClient(ch)

	<-ctx.Done()

	if err := ch.Close(); err != nil {
		log.Println(err)
	}
}

func runServer(ch *channel.AckByteChannel) {
	log.Println("server: started")

	for {
		msg, ok := ch.ReadOrBlock()

		if !ok {
			break
		}

		stats(ch, "server", "READ", string(msg))
		// ch.Ack(func(b []byte) bool { return bytes.Equal(b, msg) })
		// stats(ch, "server", "ACK", string(msg))

	}

	log.Println("server: closing")
}

func runClient(ch *channel.AckByteChannel) {
	log.Println("client: started")
	stats(ch, "client", "INIT", "")

	ch.Rewind()

	var i int

	for {
		i++

		msg := fmt.Sprintf("msg %03d", i)

		ok := ch.WriteOrBlock(func(b []byte) {
			copy(b, msg)
		})

		if !ok {
			break
		}

		time.Sleep(time.Second)

		stats(ch, "client", "WRITE", string(msg))
	}

	log.Println("client: closing")
}

func stats(ch *channel.AckByteChannel, who, what, msg string) {
	log.Printf("%s: %s - %5s | %02d messages (%02d not acknowledged)\n", who, msg, what, ch.Len(), ch.AwaitingAck())
}
