package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/gosuri/uilive"
	"github.com/webbmaffian/go-mad/channel"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	args := os.Args[1:]

	if len(args) != 1 {
		log.Println("Exactly one (1) argument expected, and this must be the path to the file.")
		return
	}

	ch, err := channel.OpenAckByteChannelReadonly(args[0])

	if err != nil {
		log.Println(err)
		return
	}

	defer ch.Close()

	ticker := time.NewTicker(time.Second)
	writer := uilive.New()

	length := writer.Newline()
	unread := writer.Newline()
	awaitingAck := writer.Newline()
	capacity := writer.Newline()
	itemSize := writer.Newline()
	startIdx := writer.Newline()

	// start listening for updates and render
	writer.Start()
	defer writer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:

			fmt.Fprintf(capacity, "Capacity: %d\n", ch.Cap())
			fmt.Fprintf(itemSize, "Item size: %d\n", ch.ItemSize())
			fmt.Fprintf(startIdx, "Start index: %d\n", ch.StartIndex())
			fmt.Fprintf(length, "Length: %d\n", ch.Len())
			fmt.Fprintf(unread, "Unread: %d\n", ch.Unread())
			fmt.Fprintf(awaitingAck, "Awaiting ack: %d\n", ch.AwaitingAck())
		}
	}
}
