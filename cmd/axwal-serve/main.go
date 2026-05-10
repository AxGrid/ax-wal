// axwal-serve exposes a local WAL over the network for RemoteReader / Mirror
// clients. The server itself is plain TCP — bind it to 127.0.0.1 and let an
// SSH tunnel (built into RemoteReader) handle confidentiality and auth.
//
// Recommended deployment on the primary host:
//
//	axwal-serve -dir /var/wal -listen 127.0.0.1:9001
//
// Followers connect with `axwal-mirror -tunnel root@primary:22 -addr ...`
// — see that binary's docs for the full picture.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	axwal "github.com/axgrid/ax-wal"
)

func main() {
	dir := flag.String("dir", "", "WAL directory (read-only access)")
	listen := flag.String("listen", "127.0.0.1:9001", "TCP address to listen on (use 127.0.0.1 + SSH tunnel)")
	keepalive := flag.Duration("keepalive", 15*time.Second, "PING interval on idle conns; 0 disables")
	ackBatch := flag.Duration("ack-batch", 100*time.Millisecond, "AckBatchInterval forwarded to per-conn Reader")
	flag.Parse()

	if *dir == "" {
		log.Fatalf("-dir required")
	}

	ctx, cancel := signalCtx()
	defer cancel()

	if err := axwal.Serve(ctx, axwal.ServerOptions{
		Dir:               *dir,
		Listen:            *listen,
		KeepAliveInterval: *keepalive,
		AckBatchInterval:  *ackBatch,
	}); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func signalCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	return ctx, cancel
}
