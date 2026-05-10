// consumer is a tiny demo: tails a WAL, prints records, Acks each one.
//
//	go run ./cmd/consumer -dir /tmp/axwal -name worker-1
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	axwal "github.com/axgrid/ax-wal"
)

func main() {
	dir := flag.String("dir", "/tmp/axwal", "WAL directory")
	name := flag.String("name", "consumer-1", "cursor name (must be unique per process)")
	from := flag.String("from", "earliest", "earliest | latest | <index>")
	flag.Parse()

	opts := axwal.ReaderOptions{
		Dir:               *dir,
		Name:              *name,
		HeartbeatInterval: 5 * time.Second,
	}
	switch *from {
	case "earliest":
		opts.StartFrom = axwal.StartEarliest
	case "latest":
		opts.StartFrom = axwal.StartLatest
	default:
		idx, err := strconv.ParseUint(*from, 10, 64)
		if err != nil || idx == 0 {
			log.Fatalf("invalid -from %q: must be 'earliest', 'latest', or a positive integer", *from)
		}
		opts.StartFrom = axwal.StartAt
		opts.StartAtIndex = idx
	}

	r, err := axwal.OpenReader(opts)
	if err != nil {
		log.Fatalf("open reader: %v", err)
	}
	defer r.Close()

	log.Printf("reader %q starting at next index = %d", *name, r.NextIndex())

	ctx, cancel := signalContext()
	defer cancel()

	for {
		payload, idx, err := r.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("shutting down at index=%d", r.CurrentIndex())
				return
			}
			log.Fatalf("next: %v", err)
		}
		log.Printf("idx=%d payload=%s", idx, string(payload))
		if err := r.Ack(idx); err != nil {
			log.Fatalf("ack: %v", err)
		}
	}
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	return ctx, cancel
}
