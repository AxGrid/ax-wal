package axwal

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// startServer serves dir on addr until the returned stop func is called.
func startServer(t *testing.T, dir, addr string) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, ServerOptions{Dir: dir, Listen: addr,
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

// startReplica runs RunReplica in the background; stop returns its error.
func startReplica(t *testing.T, dir, addr string) (stop func() error) {
	return startReplicaWith(t, dir, RemoteReaderOptions{Addr: addr})
}

func startReplicaWith(t *testing.T, dir string, src RemoteReaderOptions) (stop func() error) {
	t.Helper()
	src.Name, src.BatchSize = "replica-1", 16
	if src.DialTimeout == 0 {
		src.DialTimeout = 2 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunReplica(ctx, ReplicaOptions{
			Dir:       dir,
			Source:    src,
			Writer:    WriterOptions{MaxSegmentRecords: 25},
			Reconnect: Backoff{Min: 10 * time.Millisecond, Max: 50 * time.Millisecond},
		})
	}()
	return func() error {
		cancel()
		return <-done
	}
}

// replicaHead reads the replica's head through an ordinary Reader.
func replicaHead(t *testing.T, dir string) uint64 {
	t.Helper()
	r, err := OpenReader(ReaderOptions{Dir: dir, Name: fmt.Sprintf("probe-%d", time.Now().UnixNano())})
	if err != nil {
		return 0
	}
	defer r.Close()
	head, _ := r.HeadIndex()
	return head
}

// requireSameRecords checks that the replica holds exactly indices from..to
// with the payloads the primary wrote.
func requireSameRecords(t *testing.T, dir string, from, to uint64) {
	t.Helper()
	r, err := OpenReader(ReaderOptions{Dir: dir, Name: "verify"})
	require.NoError(t, err)
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for want := from; want <= to; want++ {
		p, idx, err := r.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, want, idx)
		require.Equal(t, fmt.Sprintf("rec-%d", want), string(p))
	}
}

func appendRecs(t *testing.T, w *Writer, from, to int) {
	t.Helper()
	for i := from; i <= to; i++ {
		idx, err := w.Append([]byte(fmt.Sprintf("rec-%d", i)))
		require.NoError(t, err)
		require.Equal(t, uint64(i), idx)
	}
}

func TestReplica_FollowsReconnectsAndPromotes(t *testing.T) {
	primary, replica := t.TempDir(), t.TempDir()
	addr := freePort(t)

	w, err := OpenWriter(WriterOptions{Dir: primary, MaxSegmentRecords: 40})
	require.NoError(t, err)
	defer w.Close()
	appendRecs(t, w, 1, 100)

	stopServer := startServer(t, primary, addr)
	stopReplica := startReplica(t, replica, addr)
	waitFor(t, "initial sync", func() bool { return replicaHead(t, replica) == 100 })

	// Live tailing.
	appendRecs(t, w, 101, 150)
	waitFor(t, "live records", func() bool { return replicaHead(t, replica) == 150 })

	// Nobody else may write into the replica.
	require.True(t, IsReplica(replica))
	_, err = OpenWriter(WriterOptions{Dir: replica})
	require.ErrorIs(t, err, ErrReplicaDir)
	require.Error(t, PromoteReplica(replica), "promote must fail while the replica runs")

	// Primary's server goes away; records keep arriving on the primary.
	stopServer()
	appendRecs(t, w, 151, 230)
	time.Sleep(100 * time.Millisecond) // a few failed reconnects
	stopServer = startServer(t, primary, addr)
	defer stopServer()
	waitFor(t, "catch-up after reconnect", func() bool { return replicaHead(t, replica) == 230 })

	// Restart of the replica process itself: resumes from its own WAL.
	require.NoError(t, stopReplica())
	appendRecs(t, w, 231, 260)
	stopReplica = startReplica(t, replica, addr)
	waitFor(t, "catch-up after restart", func() bool { return replicaHead(t, replica) == 260 })
	require.NoError(t, stopReplica())

	// Same indices, same payloads, nothing skipped or duplicated.
	requireSameRecords(t, replica, 1, 260)

	// Failover: promote and continue the same index sequence.
	_, err = OpenWriter(WriterOptions{Dir: replica})
	require.ErrorIs(t, err, ErrReplicaDir, "still read-only after the replica stopped")
	require.NoError(t, PromoteReplica(replica))
	require.False(t, IsReplica(replica))
	nw, err := OpenWriter(WriterOptions{Dir: replica})
	require.NoError(t, err)
	defer nw.Close()
	idx, err := nw.Append([]byte("after-failover"))
	require.NoError(t, err)
	require.Equal(t, uint64(261), idx)
}

func TestReplica_StartsWhereSourceHistoryStarts(t *testing.T) {
	primary, replica := t.TempDir(), t.TempDir()
	addr := freePort(t)

	w, err := OpenWriter(WriterOptions{Dir: primary, MaxSegmentRecords: 10})
	require.NoError(t, err)
	defer w.Close()
	appendRecs(t, w, 1, 35)
	w.awaitRotations()
	n, err := w.TruncatePrefix(21) // drops wal-1-10 and wal-11-20
	require.NoError(t, err)
	require.Equal(t, 2, n)

	defer startServer(t, primary, addr)()
	stopReplica := startReplica(t, replica, addr)
	waitFor(t, "sync", func() bool { return replicaHead(t, replica) == 35 })
	require.NoError(t, stopReplica())
	requireSameRecords(t, replica, 21, 35)
}

func TestReplica_RefusesToDiverge(t *testing.T) {
	addr := freePort(t)

	// A replica that holds 1..10 of source A…
	srcA, replica := t.TempDir(), t.TempDir()
	wa, err := OpenWriter(WriterOptions{Dir: srcA})
	require.NoError(t, err)
	appendRecs(t, wa, 1, 10)
	stopA := startServer(t, srcA, addr)
	stopReplica := startReplica(t, replica, addr)
	waitFor(t, "sync", func() bool { return replicaHead(t, replica) == 10 })
	require.NoError(t, stopReplica())
	stopA()
	require.NoError(t, wa.Close())

	run := func(src string) error {
		stop := startServer(t, src, addr)
		defer stop()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return RunReplica(ctx, ReplicaOptions{Dir: replica,
			Source: RemoteReaderOptions{Addr: addr, Name: "replica-1"}})
	}

	// …meets a source that has already dropped 11..20: a hole, not a copy.
	srcB := t.TempDir()
	wb, err := OpenWriter(WriterOptions{Dir: srcB, MaxSegmentRecords: 10})
	require.NoError(t, err)
	appendRecs(t, wb, 1, 35)
	wb.awaitRotations()
	_, err = wb.TruncatePrefix(21)
	require.NoError(t, err)
	require.NoError(t, wb.Close())
	require.ErrorIs(t, run(srcB), ErrReplicaDiverged)

	// …or a source that is shorter than the replica.
	srcC := t.TempDir()
	wc, err := OpenWriter(WriterOptions{Dir: srcC})
	require.NoError(t, err)
	appendRecs(t, wc, 1, 4)
	require.NoError(t, wc.Close())
	require.ErrorIs(t, run(srcC), ErrReplicaDiverged)

	// Either way the replica's own data is untouched.
	requireSameRecords(t, replica, 1, 10)
	require.Equal(t, uint64(10), replicaHead(t, replica))
}

func TestReplica_RefusesNonReplicaDir(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir})
	require.NoError(t, err)
	appendRecs(t, w, 1, 3)
	require.NoError(t, w.Close())

	err = RunReplica(context.Background(), ReplicaOptions{Dir: dir,
		Source: RemoteReaderOptions{Addr: "127.0.0.1:1", Name: "x"}})
	require.ErrorContains(t, err, "not a replica")
	require.Error(t, PromoteReplica(dir))
}

// blackholeProxy forwards TCP to target. freeze() makes every connection
// open at that moment go silent in both directions WITHOUT closing it — what
// a dropped network path looks like to both ends. Later connections work.
type blackholeProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	gates  []*atomic.Bool
}

func newBlackholeProxy(t *testing.T, target string) *blackholeProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &blackholeProxy{ln: ln, target: target}
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
			frozen := &atomic.Bool{}
			p.mu.Lock()
			p.gates = append(p.gates, frozen)
			p.mu.Unlock()
			pipe := func(dst, src net.Conn) {
				buf := make([]byte, 32<<10)
				for {
					n, err := src.Read(buf)
					if err != nil {
						if !frozen.Load() {
							dst.Close()
						}
						return
					}
					if frozen.Load() {
						continue // swallow
					}
					if _, err := dst.Write(buf[:n]); err != nil {
						return
					}
				}
			}
			go pipe(up, c)
			go pipe(c, up)
		}
	}()
	return p
}

func (p *blackholeProxy) addr() string { return p.ln.Addr().String() }

func (p *blackholeProxy) freeze() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, g := range p.gates {
		g.Store(true)
	}
}

// The link dies without a FIN/RST: nothing fails, data just stops. The
// replica has to notice on its own, reconnect, and the server has to let the
// same follower name back in although the old connection still looks alive.
func TestReplica_ReconnectsAfterSilentDrop(t *testing.T) {
	primary, replica := t.TempDir(), t.TempDir()
	addr := freePort(t)
	w, err := OpenWriter(WriterOptions{Dir: primary})
	require.NoError(t, err)
	defer w.Close()
	appendRecs(t, w, 1, 50)

	defer startServer(t, primary, addr)() // pings every 100ms
	proxy := newBlackholeProxy(t, addr)
	stopReplica := startReplicaWith(t, replica, RemoteReaderOptions{
		Addr: proxy.addr(), IdleTimeout: 400 * time.Millisecond,
	})
	waitFor(t, "initial sync", func() bool { return replicaHead(t, replica) == 50 })

	proxy.freeze()
	appendRecs(t, w, 51, 120)
	waitFor(t, "catch-up after silent drop", func() bool { return replicaHead(t, replica) == 120 })

	// And again, to make sure a second eviction works too.
	proxy.freeze()
	appendRecs(t, w, 121, 150)
	waitFor(t, "catch-up after second drop", func() bool { return replicaHead(t, replica) == 150 })

	require.NoError(t, stopReplica())
	requireSameRecords(t, replica, 1, 150)
}

// testSSHServer is a minimal SSH server that only does what the tunnel
// needs: public-key auth and direct-tcpip forwarding.
type testSSHServer struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func startSSHServer(t *testing.T, addr string, hostKey ssh.Signer, allowed ssh.PublicKey, moreHostKeys ...ssh.Signer) *testSSHServer {
	t.Helper()
	cfg := &ssh.ServerConfig{
		PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if string(key.Marshal()) != string(allowed.Marshal()) {
				return nil, errors.New("unknown key")
			}
			return nil, nil
		},
	}
	cfg.AddHostKey(hostKey)
	for _, k := range moreHostKeys {
		cfg.AddHostKey(k)
	}
	var ln net.Listener
	require.Eventually(t, func() bool {
		var err error
		ln, err = net.Listen("tcp", addr)
		return err == nil
	}, 2*time.Second, 10*time.Millisecond)
	s := &testSSHServer{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, c)
			s.mu.Unlock()
			go func() {
				sc, chans, reqs, err := ssh.NewServerConn(c, cfg)
				if err != nil {
					c.Close()
					return
				}
				defer sc.Close()
				go ssh.DiscardRequests(reqs)
				for nc := range chans {
					if nc.ChannelType() != "direct-tcpip" {
						_ = nc.Reject(ssh.UnknownChannelType, "only direct-tcpip")
						continue
					}
					var req struct {
						DestAddr string
						DestPort uint32
						OrigAddr string
						OrigPort uint32
					}
					if err := ssh.Unmarshal(nc.ExtraData(), &req); err != nil {
						_ = nc.Reject(ssh.ConnectionFailed, "bad request")
						continue
					}
					up, err := net.Dial("tcp", net.JoinHostPort(req.DestAddr, fmt.Sprint(req.DestPort)))
					if err != nil {
						_ = nc.Reject(ssh.ConnectionFailed, err.Error())
						continue
					}
					ch, chReqs, err := nc.Accept()
					if err != nil {
						up.Close()
						continue
					}
					go ssh.DiscardRequests(chReqs)
					go func() { _, _ = io.Copy(up, ch); up.Close() }()
					go func() { _, _ = io.Copy(ch, up); ch.Close() }()
				}
			}()
		}
	}()
	return s
}

// kill drops the listener and every session, like sshd or the host going away.
func (s *testSSHServer) kill() {
	s.ln.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		c.Close()
	}
}

// The real transport: replica → SSH (key auth, known_hosts check) →
// axwal-serve bound to localhost on the primary.
func TestReplica_OverSSHTunnel(t *testing.T) {
	primary, replica, keys := t.TempDir(), t.TempDir(), t.TempDir()
	serveAddr, sshAddr := freePort(t), freePort(t)

	// Host key + known_hosts, client key file.
	_, hostPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	hostSigner, err := ssh.NewSignerFromKey(hostPriv)
	require.NoError(t, err)
	knownHosts := filepath.Join(keys, "known_hosts")
	require.NoError(t, os.WriteFile(knownHosts,
		[]byte(knownhosts.Line([]string{sshAddr}, hostSigner.PublicKey())+"\n"), 0600))

	_, clientPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	clientSigner, err := ssh.NewSignerFromKey(clientPriv)
	require.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(clientPriv, "")
	require.NoError(t, err)
	keyFile := filepath.Join(keys, "id_ed25519")
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(block), 0600))

	w, err := OpenWriter(WriterOptions{Dir: primary})
	require.NoError(t, err)
	defer w.Close()
	appendRecs(t, w, 1, 80)

	// Like a real sshd the server also has an ECDSA host key, which the Go
	// client would negotiate by default; known_hosts lists only the ED25519
	// one. The dial must ask for the key type it can verify.
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ecSigner, err := ssh.NewSignerFromKey(ecKey)
	require.NoError(t, err)

	defer startServer(t, primary, serveAddr)()
	sshd := startSSHServer(t, sshAddr, hostSigner, clientSigner.PublicKey(), ecSigner)

	tunnel := &SSHTunnelConfig{Host: sshAddr, User: "wal", KeyFile: keyFile,
		KnownHostsFile: knownHosts, DialTimeout: 2 * time.Second}
	stopReplica := startReplicaWith(t, replica, RemoteReaderOptions{
		Addr: serveAddr, SSHTunnel: tunnel, IdleTimeout: 400 * time.Millisecond,
	})
	waitFor(t, "initial sync over ssh", func() bool { return replicaHead(t, replica) == 80 })

	appendRecs(t, w, 81, 100)
	waitFor(t, "live over ssh", func() bool { return replicaHead(t, replica) == 100 })

	// The SSH server dies; the primary keeps taking writes.
	sshd.kill()
	appendRecs(t, w, 101, 180)
	time.Sleep(150 * time.Millisecond) // reconnect attempts fail meanwhile
	require.Equal(t, uint64(100), replicaHead(t, replica))

	sshd = startSSHServer(t, sshAddr, hostSigner, clientSigner.PublicKey(), ecSigner)
	defer sshd.kill()
	waitFor(t, "catch-up after ssh came back", func() bool { return replicaHead(t, replica) == 180 })
	require.NoError(t, stopReplica())
	requireSameRecords(t, replica, 1, 180)

	// A host key that isn't in known_hosts is refused.
	wrong := filepath.Join(keys, "known_hosts.wrong")
	require.NoError(t, os.WriteFile(wrong,
		[]byte(knownhosts.Line([]string{sshAddr}, clientSigner.PublicKey())+"\n"), 0600))
	bad := *tunnel
	bad.KnownHostsFile = wrong
	_, err = DialRemoteReader(context.Background(), RemoteReaderOptions{
		Addr: serveAddr, Name: "intruder-check", SSHTunnel: &bad})
	require.ErrorContains(t, err, "ssh handshake")
}

// Readers on the replica: a local one tailing it live while replication
// runs, and a remote one through axwal-serve pointed at the replica.
func TestReplica_ReadableWhileReplicating(t *testing.T) {
	primary, replica := t.TempDir(), t.TempDir()
	addr, replicaServeAddr := freePort(t), freePort(t)
	w, err := OpenWriter(WriterOptions{Dir: primary})
	require.NoError(t, err)
	defer w.Close()
	appendRecs(t, w, 1, 30)

	defer startServer(t, primary, addr)()
	stopReplica := startReplica(t, replica, addr)
	defer stopReplica()
	waitFor(t, "initial sync", func() bool { return replicaHead(t, replica) == 30 })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	local, err := OpenReader(ReaderOptions{Dir: replica, Name: "analytics"})
	require.NoError(t, err)
	defer local.Close()
	defer startServer(t, replica, replicaServeAddr)()
	remote, err := DialRemoteReader(ctx, RemoteReaderOptions{Addr: replicaServeAddr, Name: "downstream"})
	require.NoError(t, err)
	defer remote.Close()

	// Both block on the tail and receive records as they arrive from the primary.
	go func() {
		time.Sleep(50 * time.Millisecond)
		for i := 31; i <= 90; i++ {
			if _, err := w.Append([]byte(fmt.Sprintf("rec-%d", i))); err != nil {
				return
			}
		}
	}()
	for want := uint64(1); want <= 90; want++ {
		p, idx, err := local.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, want, idx)
		require.Equal(t, fmt.Sprintf("rec-%d", want), string(p))
		require.NoError(t, local.Ack(idx))

		p, idx, err = remote.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, want, idx)
		require.Equal(t, fmt.Sprintf("rec-%d", want), string(p))
	}
}

func TestSyncReplication_WaitsAndFallsBack(t *testing.T) {
	primary, replica := t.TempDir(), t.TempDir()
	addr := freePort(t)
	w, err := OpenWriter(WriterOptions{
		Dir:      primary,
		SyncMode: SyncNone, // keep local fsync cost out of the timing assertions
		SyncReplication: &SyncReplicationConfig{
			Timeout: 300 * time.Millisecond, Cooldown: 100 * time.Millisecond,
		},
	})
	require.NoError(t, err)
	defer w.Close()

	// No replica yet: asynchronous, appends don't wait for anyone.
	start := time.Now()
	appendRecs(t, w, 1, 20)
	require.Less(t, time.Since(start), 250*time.Millisecond)
	require.False(t, w.Stats().SyncReplicationActive)

	// The server lives in the writer's process and reports acks to it.
	srvCtx, srvCancel := context.WithCancel(context.Background())
	srvDone := make(chan error, 1)
	go func() {
		srvDone <- Serve(srvCtx, ServerOptions{Writer: w, Listen: addr,
			KeepAliveInterval: 100 * time.Millisecond, AckBatchInterval: 10 * time.Millisecond})
	}()
	defer func() { srvCancel(); <-srvDone }()

	proxy := newBlackholeProxy(t, addr)
	var stopReplica func() error
	require.Eventually(t, func() bool {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)
	stopReplica = startReplicaWith(t, replica, RemoteReaderOptions{
		Addr: proxy.addr(), IdleTimeout: 400 * time.Millisecond,
	})
	defer func() { require.NoError(t, stopReplica()) }()

	waitFor(t, "sync mode on", func() bool { return w.Stats().SyncReplicationActive })
	require.Equal(t, 1, w.Stats().ReplicasConnected)

	// Synchronous: the moment Append returns, the record is on the replica.
	for i := 21; i <= 60; i++ {
		idx, err := w.Append([]byte(fmt.Sprintf("rec-%d", i)))
		require.NoError(t, err)
		require.GreaterOrEqual(t, w.Stats().ReplicaAckedIndex, idx)
		require.Equal(t, idx, replicaHead(t, replica), "record %d returned before it reached the replica", idx)
	}
	require.Zero(t, w.Stats().SyncReplicationFallbacks)

	// The link dies silently. One append pays the timeout, then the writer
	// is asynchronous and fast again — and nothing fails.
	proxy.freeze()
	start = time.Now()
	appendRecs(t, w, 61, 61)
	took := time.Since(start)
	require.GreaterOrEqual(t, took, 250*time.Millisecond)
	require.Less(t, took, 2*time.Second)
	require.False(t, w.Stats().SyncReplicationActive)
	require.Equal(t, uint64(1), w.Stats().SyncReplicationFallbacks)

	start = time.Now()
	appendRecs(t, w, 62, 100)
	require.Less(t, time.Since(start), 250*time.Millisecond, "appends must not wait while degraded")

	// The replica notices, reconnects, catches up — sync mode comes back.
	waitFor(t, "replica caught up", func() bool { return replicaHead(t, replica) == 100 })
	waitFor(t, "sync mode back on", func() bool { return w.Stats().SyncReplicationActive })
	for i := 101; i <= 120; i++ {
		idx, err := w.Append([]byte(fmt.Sprintf("rec-%d", i)))
		require.NoError(t, err)
		require.Equal(t, idx, replicaHead(t, replica))
	}

	// Replica stopped on purpose: immediate fallback, no timeout to pay.
	require.NoError(t, stopReplica())
	stopReplica = func() error { return nil }
	waitFor(t, "fallback on disconnect", func() bool { return !w.Stats().SyncReplicationActive })
	start = time.Now()
	appendRecs(t, w, 121, 140)
	require.Less(t, time.Since(start), 250*time.Millisecond)
	requireSameRecords(t, replica, 1, 120)
}

// Concurrent appenders share replica round trips instead of queuing behind
// each other.
func TestSyncReplication_ConcurrentAppenders(t *testing.T) {
	primary, replica := t.TempDir(), t.TempDir()
	addr := freePort(t)
	w, err := OpenWriter(WriterOptions{Dir: primary, SyncMode: SyncNone,
		SyncReplication: &SyncReplicationConfig{Timeout: 5 * time.Second}})
	require.NoError(t, err)
	defer w.Close()

	srvCtx, srvCancel := context.WithCancel(context.Background())
	srvDone := make(chan error, 1)
	go func() { srvDone <- Serve(srvCtx, ServerOptions{Writer: w, Listen: addr}) }()
	defer func() { srvCancel(); <-srvDone }()
	require.Eventually(t, func() bool {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond)
	stopReplica := startReplica(t, replica, addr)
	defer stopReplica()
	waitFor(t, "sync mode on", func() bool { return w.Stats().SyncReplicationActive })

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				idx, err := w.Append([]byte("x"))
				if err != nil || w.Stats().ReplicaAckedIndex < idx {
					t.Errorf("append %d: err=%v acked=%d", idx, err, w.Stats().ReplicaAckedIndex)
					return
				}
			}
		}()
	}
	wg.Wait()
	st := w.Stats()
	require.Equal(t, uint64(400), st.LastIndex)
	require.Zero(t, st.SyncReplicationFallbacks)
	require.True(t, st.SyncReplicationActive)
}
