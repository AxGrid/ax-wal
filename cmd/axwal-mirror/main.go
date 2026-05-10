// axwal-mirror runs a foreground replication loop from a source WAL to a
// destination WAL. Resumable via a stable cursor name; designed to live under
// systemd / supervisord for hot-standby followers.
//
//	axwal-mirror -src /shared/leader -dst /local/follower -name follower-1
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	axwal "github.com/axgrid/ax-wal"
)

func main() {
	src := flag.String("src", "", "source WAL directory (read-from)")
	dst := flag.String("dst", "", "destination WAL directory (append-to)")
	name := flag.String("name", "", "cursor name in source — stable across restarts")
	from := flag.String("from", "earliest", "first-time start: earliest | latest | <index>")
	batch := flag.Int("batch", 256, "records per round trip")
	srcKey := flag.String("src-key-file", "", "32B AES key file (read encrypted source)")
	dstKey := flag.String("dst-key-file", "", "32B AES key file (write encrypted destination)")
	statusEvery := flag.Duration("status", 5*time.Second, "log a progress line at this interval; 0 disables")
	flag.Parse()

	if *src == "" || *dst == "" || *name == "" {
		fmt.Fprintln(os.Stderr, "axwal-mirror: -src, -dst, and -name are required")
		flag.Usage()
		os.Exit(2)
	}

	opts := axwal.MirrorOptions{
		SrcDir:    *src,
		DstDir:    *dst,
		Name:      *name,
		BatchSize: *batch,
	}
	switch *from {
	case "earliest":
		opts.StartFrom = axwal.StartEarliest
	case "latest":
		opts.StartFrom = axwal.StartLatest
	default:
		var idx uint64
		_, err := fmt.Sscanf(*from, "%d", &idx)
		if err != nil || idx == 0 {
			log.Fatalf("invalid -from %q: must be 'earliest', 'latest', or a positive uint64", *from)
		}
		opts.StartFrom = axwal.StartAt
		opts.StartAtIndex = idx
	}
	if k := loadKeyFile(*srcKey); k != nil {
		opts.SrcReaderOptions.Encryption = k
	}
	if k := loadKeyFile(*dstKey); k != nil {
		opts.DstWriterOptions.Encryption = k
	}

	ctx, cancel := signalContext()
	defer cancel()

	// Light status reporter goroutine.
	if *statusEvery > 0 {
		go statusLoop(ctx, *src, *name, *statusEvery)
	}

	log.Printf("mirror: %s -> %s (cursor=%s, batch=%d)", *src, *dst, *name, *batch)
	mirrored, err := axwal.Mirror(ctx, opts)
	if err != nil {
		log.Fatalf("mirror: %v", err)
	}
	log.Printf("mirror: stopped after %d records", mirrored)
}

func loadKeyFile(path string) *axwal.EncryptionConfig {
	if path == "" {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read key %s: %v", path, err)
	}
	if len(b) != 32 {
		log.Fatalf("key file %s must be 32 bytes, got %d", path, len(b))
	}
	return &axwal.EncryptionConfig{Key: b}
}

// statusLoop is a tiny independent reader registered with a different cursor
// name that just observes Lag(). Avoids depending on internals from the mirror
// loop.
func statusLoop(ctx context.Context, srcDir, mirrorName string, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	statusName := mirrorName + ".status"
	r, err := axwal.OpenReader(axwal.ReaderOptions{
		Dir:               srcDir,
		Name:              statusName,
		StartFrom:         axwal.StartLatest,
		HeartbeatInterval: every,
		PollInterval:      time.Second,
	})
	if err != nil {
		// non-fatal — running without status is fine
		log.Printf("mirror status: %v (continuing without status)", err)
		return
	}
	defer r.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			lag, bytes, err := r.Lag()
			if err != nil {
				log.Printf("mirror status: %v", err)
				continue
			}
			head, _ := r.HeadIndex()
			log.Printf("status: head=%d  lag=%d records  ~%d bytes", head, lag, bytes)
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
