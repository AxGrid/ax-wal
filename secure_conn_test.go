package axwal

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	tkey1 = bytes.Repeat([]byte{0x11}, 32)
	tkey2 = bytes.Repeat([]byte{0x22}, 32)
)

// tapProxy forwards TCP to target, records every byte in both directions and
// can corrupt the stream.
type tapProxy struct {
	ln   net.Listener
	mu   sync.Mutex
	seen bytes.Buffer
	// flipAfter: once this many server→client bytes have passed, flip one
	// bit in the next chunk (0 = never).
	flipAfter int
	passed    int
	flipped   bool
}

func newTapProxy(t *testing.T, target string) *tapProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &tapProxy{ln: ln}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", target)
			if err != nil {
				c.Close()
				continue
			}
			pipe := func(dst, src net.Conn, fromServer bool) {
				defer dst.Close()
				buf := make([]byte, 32<<10)
				for {
					n, err := src.Read(buf)
					if n > 0 {
						p.mu.Lock()
						p.seen.Write(buf[:n])
						if fromServer && p.flipAfter > 0 && !p.flipped {
							if p.passed >= p.flipAfter {
								buf[n/2] ^= 0x01
								p.flipped = true
							}
							p.passed += n
						}
						p.mu.Unlock()
						if _, werr := dst.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}
			go pipe(up, c, false)
			go pipe(c, up, true)
		}
	}()
	return p
}

func (p *tapProxy) addr() string { return p.ln.Addr().String() }

func (p *tapProxy) sawPlaintext(s string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return bytes.Contains(p.seen.Bytes(), []byte(s))
}

func startKeyedServer(t *testing.T, dir, addr string, key []byte, w *Writer) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, ServerOptions{Dir: dir, Writer: w, Listen: addr, TransportKey: key,
			KeepAliveInterval: 100 * time.Millisecond, AckBatchInterval: 10 * time.Millisecond})
	}()
	require.Eventually(t, func() bool {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)
	return func() {
		cancel()
		require.NoError(t, <-done)
	}
}

// The secure channel by itself: arbitrary write sizes survive, in both
// directions, and both sides must hold the same key.
func TestSecureConn_Roundtrip(t *testing.T) {
	pair := func(clientKey, serverKey []byte) (net.Conn, net.Conn, error, error) {
		a, b := net.Pipe()
		var sc net.Conn
		var serr error
		done := make(chan struct{})
		go func() { sc, serr = secureServer(b, serverKey); close(done) }()
		cc, cerr := secureClient(a, clientKey)
		if cerr != nil {
			a.Close()
		}
		<-done
		return cc, sc, cerr, serr
	}

	cc, sc, cerr, serr := pair(tkey1, tkey1)
	require.NoError(t, cerr)
	require.NoError(t, serr)
	for _, size := range []int{0, 1, 100, secureMaxFrame - 1, secureMaxFrame, secureMaxFrame + 1, 5*secureMaxFrame + 77} {
		msg := make([]byte, size)
		_, _ = rand.Read(msg)
		for _, dir := range [][2]net.Conn{{cc, sc}, {sc, cc}} {
			go func() { _, _ = dir[0].Write(msg) }()
			got := make([]byte, size)
			_, err := io.ReadFull(dir[1], got)
			require.NoError(t, err)
			require.True(t, bytes.Equal(msg, got), "size %d", size)
		}
	}
	cc.Close()
	sc.Close()

	_, _, cerr, serr = pair(tkey1, tkey2)
	require.ErrorIs(t, cerr, ErrTransportAuth)
	require.ErrorIs(t, serr, ErrTransportAuth)
	_, err := secureClient(nil, []byte("short"))
	require.ErrorIs(t, err, ErrTransportAuth)
}

func TestTransportKey_AccessControl(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir})
	require.NoError(t, err)
	defer w.Close()
	appendRecs(t, w, 1, 20)

	keyed, plain := freePort(t), freePort(t)
	defer startKeyedServer(t, dir, keyed, tkey1, nil)()
	defer startKeyedServer(t, dir, plain, nil, nil)()

	dial := func(addr string, key []byte) (*RemoteReader, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return DialRemoteReader(ctx, RemoteReaderOptions{Addr: addr, Name: fmt.Sprintf("c%d", time.Now().UnixNano()),
			TransportKey: key, DialTimeout: 3 * time.Second})
	}

	// Right key: works.
	rr, err := dial(keyed, tkey1)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for want := uint64(1); want <= 20; want++ {
		p, idx, err := rr.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, want, idx)
		require.Equal(t, fmt.Sprintf("rec-%d", want), string(p))
	}
	require.NoError(t, rr.Close())

	// Wrong key, no key, key against a keyless server: all refused, each
	// with a message that says which side is misconfigured.
	_, err = dial(keyed, tkey2)
	require.ErrorIs(t, err, ErrTransportAuth)
	require.ErrorContains(t, err, "same transport key")

	_, err = dial(keyed, nil)
	require.ErrorIs(t, err, ErrTransportAuth)
	require.ErrorContains(t, err, "requires a transport key")

	start := time.Now()
	_, err = dial(plain, tkey1)
	require.ErrorIs(t, err, ErrTransportAuth)
	require.ErrorContains(t, err, "no transport key configured")
	require.Less(t, time.Since(start), 2*time.Second, "a mismatch must be reported, not time out")
}

// What actually goes over the wire, and what happens when it is tampered with.
func TestTransportKey_WireIsEncryptedAndTamperProof(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir})
	require.NoError(t, err)
	defer w.Close()
	for i := 1; i <= 200; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("top-secret-payload-%d", i)))
		require.NoError(t, err)
	}
	read := func(addr string, key []byte, n int) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rr, err := DialRemoteReader(ctx, RemoteReaderOptions{Addr: addr, Name: fmt.Sprintf("c%d", time.Now().UnixNano()),
			TransportKey: key, BatchSize: 8})
		if err != nil {
			return err
		}
		defer rr.Close()
		for i := 0; i < n; i++ {
			if _, _, err := rr.Next(ctx); err != nil {
				return err
			}
		}
		return nil
	}

	// Baseline: without a key the payloads are right there on the wire.
	plain := freePort(t)
	stopPlain := startKeyedServer(t, dir, plain, nil, nil)
	tap := newTapProxy(t, plain)
	require.NoError(t, read(tap.addr(), nil, 200))
	require.True(t, tap.sawPlaintext("top-secret-payload-17"))
	stopPlain()

	keyed := freePort(t)
	defer startKeyedServer(t, dir, keyed, tkey1, nil)()
	tap = newTapProxy(t, keyed)
	require.NoError(t, read(tap.addr(), tkey1, 200))
	for _, leak := range []string{"top-secret-payload", "AXWH", "AXRB", `"name"`} {
		require.False(t, tap.sawPlaintext(leak), "%q visible on the wire", leak)
	}

	// One flipped bit in transit: the connection dies with an auth error
	// instead of delivering a damaged record.
	tap = newTapProxy(t, keyed)
	tap.flipAfter = 600
	err = read(tap.addr(), tkey1, 200)
	require.ErrorIs(t, err, ErrTransportAuth)
}

// The whole point: a replica over plain TCP, protected by the shared key —
// including reconnects and synchronous mode.
func TestReplica_OverKeyedTCP(t *testing.T) {
	primary, replica := t.TempDir(), t.TempDir()
	addr := freePort(t)
	w, err := OpenWriter(WriterOptions{Dir: primary, SyncMode: SyncNone,
		SyncReplication: &SyncReplicationConfig{Timeout: 2 * time.Second, Cooldown: 50 * time.Millisecond}})
	require.NoError(t, err)
	defer w.Close()
	appendRecs(t, w, 1, 60)

	stopServer := startKeyedServer(t, "", addr, tkey1, w)
	tap := newTapProxy(t, addr)
	stopReplica := startReplicaWith(t, replica, RemoteReaderOptions{
		Addr: tap.addr(), TransportKey: tkey1, IdleTimeout: 400 * time.Millisecond})
	waitFor(t, "initial sync", func() bool { return replicaHead(t, replica) == 60 })
	waitFor(t, "sync mode", func() bool { return w.Stats().SyncReplicationActive })

	// Synchronous through the encrypted link: on return the replica has it.
	for i := 61; i <= 90; i++ {
		idx, err := w.Append([]byte(fmt.Sprintf("rec-%d", i)))
		require.NoError(t, err)
		require.Equal(t, idx, replicaHead(t, replica))
	}
	require.False(t, tap.sawPlaintext("rec-7"), "record payloads visible on the wire")

	// Server restart: new handshake, new session keys, catch-up.
	stopServer()
	appendRecs(t, w, 91, 150)
	stopServer = startKeyedServer(t, "", addr, tkey1, w)
	defer stopServer()
	waitFor(t, "catch-up after reconnect", func() bool { return replicaHead(t, replica) == 150 })
	require.NoError(t, stopReplica())
	requireSameRecords(t, replica, 1, 150)

	// A replica with the wrong key gives up with a clear error rather than
	// hammering the server forever.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = RunReplica(ctx, ReplicaOptions{Dir: t.TempDir(),
		Source: RemoteReaderOptions{Addr: addr, Name: "intruder", TransportKey: tkey2}})
	require.ErrorIs(t, err, ErrTransportAuth)
}
