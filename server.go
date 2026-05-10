package axwal

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

// ServerOptions configures Serve, the network endpoint that exposes a local
// WAL to RemoteReader clients.
//
// We deliberately ship Serve without TLS or token auth — the recommended
// deployment is to bind on 127.0.0.1 and let an SSH tunnel (or WireGuard /
// other VPN) handle confidentiality and authentication. RemoteReader has
// in-process SSH dialing built in (see SSHTunnelConfig) so a single follower
// binary is enough; no external `ssh -L` process required.
type ServerOptions struct {
	// Dir is the source WAL the server reads from.
	Dir string

	// Listen is the TCP address. Default "127.0.0.1:9001". Use a localhost
	// bind unless you know what you're doing.
	Listen string

	// KeepAliveInterval — how often the server sends PING frames on idle
	// connections. Defaults to 15s. Set to 0 to disable.
	KeepAliveInterval time.Duration

	// AckBatchInterval is forwarded to the per-connection Reader on the source.
	// Default 100ms — tunes how often the source cursor fsyncs in response to
	// follower acks.
	AckBatchInterval time.Duration

	// Logger receives server events. Defaults to slog.Default().
	Logger *slog.Logger
}

// Serve runs the network endpoint until ctx is canceled or Listen fails.
// Each accepted connection gets its own goroutine and its own Reader on the
// source WAL (cursor name = the follower's HELLO.Name).
func Serve(ctx context.Context, opts ServerOptions) error {
	if opts.Dir == "" {
		return errors.New("axwal: ServerOptions.Dir required")
	}
	if opts.Listen == "" {
		opts.Listen = "127.0.0.1:9001"
	}
	if opts.KeepAliveInterval <= 0 {
		opts.KeepAliveInterval = 15 * time.Second
	}
	if opts.AckBatchInterval <= 0 {
		opts.AckBatchInterval = 100 * time.Millisecond
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	log := opts.Logger.With("component", "axwal.server", "listen", opts.Listen)

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", opts.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	log.Info("serving WAL", "dir", opts.Dir)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	var wg sync.WaitGroup
	for {
		conn, err := ln.Accept()
		if err != nil {
			wg.Wait()
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			handleConn(ctx, opts, conn, log)
		}()
	}
}

func handleConn(ctx context.Context, opts ServerOptions, conn net.Conn, baseLog *slog.Logger) {
	defer conn.Close()
	log := baseLog.With("peer", conn.RemoteAddr().String())

	// HELLO must arrive first.
	magic, body, err := readFrame(conn)
	if err != nil {
		log.Warn("hello read failed", "err", err)
		return
	}
	if magic != magicHello {
		_ = sendErr(conn, "expected HELLO frame")
		return
	}
	var hello helloPayload
	if err := json.Unmarshal(body, &hello); err != nil {
		_ = sendErr(conn, "bad HELLO json: "+err.Error())
		return
	}
	if hello.Name == "" {
		_ = sendErr(conn, "HELLO.name required")
		return
	}
	if hello.BatchSize <= 0 {
		hello.BatchSize = 256
	}
	log = log.With("follower", hello.Name)

	rOpts := ReaderOptions{
		Dir:               opts.Dir,
		Name:              hello.Name,
		AckBatchInterval:  opts.AckBatchInterval,
		HeartbeatInterval: 5 * time.Second,
		PollInterval:      50 * time.Millisecond,
		Logger:            opts.Logger,
	}
	if hello.StartIndex == 0 {
		rOpts.StartFrom = StartEarliest
	} else {
		rOpts.StartFrom = StartAt
		rOpts.StartAtIndex = hello.StartIndex
	}
	r, err := OpenReader(rOpts)
	if err != nil {
		_ = sendErr(conn, "open source reader: "+err.Error())
		return
	}
	defer r.Close()

	head, _ := r.HeadIndex()
	if err := sendOK(conn, head); err != nil {
		log.Warn("send OK failed", "err", err)
		return
	}

	// Connection-scoped serializer for writes (BATCH and PING).
	var writeMu sync.Mutex

	// Goroutine reading client ACK frames.
	connCtx, connCancel := context.WithCancel(ctx)
	defer connCancel()
	go func() {
		defer connCancel() // any wire error tears down the whole conn
		for {
			m, b, err := readFrame(conn)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					log.Info("client closed read side", "err", err)
				}
				return
			}
			if m != magicAck {
				log.Warn("unexpected client frame", "magic", string(m[:]))
				return
			}
			if len(b) != 8 {
				log.Warn("malformed ACK")
				return
			}
			idx := binary.BigEndian.Uint64(b)
			if err := r.Ack(idx); err != nil {
				log.Warn("source ack failed", "err", err, "idx", idx)
				return
			}
		}
	}()

	// Optional keepalive ticker.
	if opts.KeepAliveInterval > 0 {
		go func() {
			t := time.NewTicker(opts.KeepAliveInterval)
			defer t.Stop()
			for {
				select {
				case <-connCtx.Done():
					return
				case <-t.C:
					writeMu.Lock()
					_ = writeFrame(conn, magicPing, nil)
					writeMu.Unlock()
				}
			}
		}()
	}

	log.Info("follower connected", "head", head, "start_index", hello.StartIndex)
	for {
		payloads, indices, err := r.NextBatch(connCtx, hello.BatchSize)
		if err != nil {
			if connCtx.Err() != nil {
				return
			}
			log.Warn("NextBatch failed", "err", err)
			return
		}
		if len(payloads) == 0 {
			continue
		}
		body := encodeBatch(indices, payloads)
		writeMu.Lock()
		err = writeFrame(conn, magicBatch, body)
		writeMu.Unlock()
		if err != nil {
			log.Info("client gone", "err", err)
			return
		}
	}
}

func sendOK(conn net.Conn, head uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], head)
	return writeFrame(conn, magicOK, b[:])
}

func sendErr(conn net.Conn, msg string) error {
	body, _ := json.Marshal(errPayload{Error: msg})
	return writeFrame(conn, magicErr, body)
}
