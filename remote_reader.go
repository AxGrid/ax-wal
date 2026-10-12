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
	"sync/atomic"
	"time"

	"golang.org/x/crypto/ssh"
)

// RemoteReaderOptions configures a RemoteReader. Pass either a plain TCP
// address (Addr) for direct dialing, or also fill SSHTunnel to dial through
// an in-process SSH connection (no external `ssh -L` needed).
//
// When SSHTunnel is set, Addr is the address as seen FROM the remote SSH
// host — typically "127.0.0.1:9001" because axwal-serve listens on
// localhost on the primary.
type RemoteReaderOptions struct {
	// Addr is the host:port of axwal-serve. Without SSHTunnel it's the
	// primary's externally reachable address; with SSHTunnel it's the
	// address the SSH host can reach internally.
	Addr string

	// SSHTunnel, if non-nil, dials Addr through an SSH session.
	SSHTunnel *SSHTunnelConfig

	// TransportKey (32 bytes) must match ServerOptions.TransportKey when the
	// server has one: the two sides authenticate each other with it and
	// encrypt the connection. This is the way to replicate over plain TCP
	// without an SSH tunnel. It can also be combined with SSHTunnel.
	TransportKey []byte

	// Name is the cursor name registered on the SOURCE WAL via HELLO. Stable
	// names let the source remember progress across follower restarts.
	Name string

	// StartIndex is sent in HELLO. 0 means "earliest available on source".
	StartIndex uint64

	// BatchSize hints the server how many records it batches per BATCH frame.
	// Defaults to 256.
	BatchSize int

	// AckEvery sends an ACK frame back to the server every N records, so the
	// source-side cursor advances and retention can reclaim consumed segments.
	// Defaults to BatchSize. Set lower for tighter retention; higher for
	// lighter wire chatter.
	AckEvery int

	// DialTimeout for the initial TCP / SSH connect. Defaults to 15s.
	DialTimeout time.Duration

	// IdleTimeout: if nothing arrives from the server for this long — no
	// records and no keepalive PING — the connection is considered dead and
	// is closed, so Next returns an error instead of waiting forever. This
	// is what catches a link that died silently (network drop, frozen SSH
	// session). Must be comfortably above the server's KeepAliveInterval
	// (default 15s). Default 60s.
	IdleTimeout time.Duration

	// QueueDepth bounds how many records may be buffered in memory between
	// the network read goroutine and Next/NextBatch callers. Defaults to
	// BatchSize * 4.
	QueueDepth int

	Logger *slog.Logger

	// exact asks the server to start at StartIndex even if it remembers a
	// different position for Name. Set by RunReplica.
	exact bool
}

// RemoteReader reads records from a remote axwal-serve. It satisfies the
// same access pattern as Reader: Next / NextBatch / Ack / Close, but the
// records flow over the network instead of the filesystem.
type RemoteReader struct {
	opts RemoteReaderOptions
	log  *slog.Logger

	conn      net.Conn
	sshClient *ssh.Client // non-nil iff using tunnel

	queue chan recordItem
	errCh chan error // buffered with at most one error from the read loop

	closeOnce sync.Once
	closed    chan struct{}
	wg        sync.WaitGroup

	writeMu sync.Mutex // serializes ACK frames

	// progress tracking for periodic ACK
	pendingMu  sync.Mutex
	pendingIdx uint64
	lastAcked  uint64

	headIdx uint64 // initial head from OK

	lastRx atomic.Int64 // UnixNano of the last frame received
}

type recordItem struct {
	idx     uint64
	payload []byte
}

// DialRemoteReader connects to an axwal-serve endpoint and starts streaming
// records into an internal queue. Returns once the HELLO/OK handshake has
// completed; the actual record stream is consumed lazily via Next/NextBatch.
func DialRemoteReader(ctx context.Context, opts RemoteReaderOptions) (*RemoteReader, error) {
	if opts.Addr == "" {
		return nil, errors.New("axwal: RemoteReaderOptions.Addr required")
	}
	if opts.Name == "" {
		return nil, errors.New("axwal: RemoteReaderOptions.Name required")
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 256
	}
	if opts.AckEvery <= 0 {
		opts.AckEvery = opts.BatchSize
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 15 * time.Second
	}
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = 60 * time.Second
	}
	if opts.QueueDepth <= 0 {
		opts.QueueDepth = opts.BatchSize * 4
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	log := opts.Logger.With("component", "axwal.remote-reader", "addr", opts.Addr, "name", opts.Name)

	var (
		conn      net.Conn
		sshClient *ssh.Client
		err       error
	)
	closeAll := func() {
		if conn != nil {
			_ = conn.Close()
		}
		if sshClient != nil {
			_ = sshClient.Close()
		}
	}
	if opts.SSHTunnel != nil {
		sshClient, err = dialSSH(ctx, opts.SSHTunnel)
		if err != nil {
			return nil, fmt.Errorf("ssh dial: %w", err)
		}
		if opts.SSHTunnel.KnownHostsFile == "" {
			log.Warn("SSH host-key verification DISABLED (no KnownHostsFile)",
				"ssh_host", opts.SSHTunnel.Host)
		}
	}

	// Everything up to the OK frame is bounded by DialTimeout (and ctx).
	// Channels over SSH don't support deadlines, so the guard works by
	// closing the transport, which unblocks whatever is stuck.
	handshakeDone := make(chan struct{})
	guardFired := make(chan struct{})
	go func() {
		t := time.NewTimer(opts.DialTimeout)
		defer t.Stop()
		select {
		case <-handshakeDone:
			return
		case <-t.C:
		case <-ctx.Done():
		}
		close(guardFired)
		if sshClient != nil {
			_ = sshClient.Close()
		}
	}()
	fail := func(format string, a ...any) (*RemoteReader, error) {
		close(handshakeDone)
		closeAll()
		select {
		case <-guardFired:
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("axwal: connecting to %s timed out after %s", opts.Addr, opts.DialTimeout)
		default:
		}
		return nil, fmt.Errorf(format, a...)
	}

	if sshClient != nil {
		conn, err = sshClient.Dial("tcp", opts.Addr)
		if err != nil {
			return fail("tunnel dial %s: %w", opts.Addr, err)
		}
	} else {
		d := net.Dialer{Timeout: opts.DialTimeout}
		conn, err = d.DialContext(ctx, "tcp", opts.Addr)
		if err != nil {
			return fail("dial: %w", err)
		}
		_ = conn.SetDeadline(time.Now().Add(opts.DialTimeout))
	}

	if opts.TransportKey != nil {
		sc, err := secureClient(conn, opts.TransportKey)
		if err != nil {
			return fail("%w", err)
		}
		conn = sc
	}

	hello := helloPayload{Name: opts.Name, StartIndex: opts.StartIndex, BatchSize: opts.BatchSize, Exact: opts.exact}
	body, _ := json.Marshal(hello)
	if err := writeFrame(conn, magicHello, body); err != nil {
		return fail("send HELLO: %w", err)
	}
	magic, body, err := readFrame(conn)
	if err != nil {
		return fail("read OK: %w", err)
	}
	if magic == magicErr {
		var ep errPayload
		_ = json.Unmarshal(body, &ep)
		if ep.Error == errMsgKeyRequired {
			return fail("%w: %s (set RemoteReaderOptions.TransportKey)", ErrTransportAuth, ep.Error)
		}
		return fail("server: %s", ep.Error)
	}
	if magic != magicOK {
		return fail("expected OK, got %q", string(magic[:]))
	}
	if len(body) != 8 {
		return fail("malformed OK body (%d bytes)", len(body))
	}
	head := binary.BigEndian.Uint64(body)
	close(handshakeDone)
	select {
	case <-guardFired:
		closeAll()
		return nil, fmt.Errorf("axwal: connecting to %s timed out after %s", opts.Addr, opts.DialTimeout)
	default:
	}
	if sshClient == nil {
		_ = conn.SetDeadline(time.Time{})
	}

	rr := &RemoteReader{
		opts:      opts,
		log:       log,
		conn:      conn,
		sshClient: sshClient,
		queue:     make(chan recordItem, opts.QueueDepth),
		errCh:     make(chan error, 1),
		closed:    make(chan struct{}),
		headIdx:   head,
	}
	rr.lastRx.Store(time.Now().UnixNano())
	rr.wg.Add(2)
	go rr.readLoop()
	go rr.watchdog()
	return rr, nil
}

// HeadIndex returns the head index reported by the server at HELLO time.
// This is a snapshot — call again on a fresh connection for an updated value.
func (rr *RemoteReader) HeadIndex() uint64 { return rr.headIdx }

// Next blocks for the next record from the network queue. ctx cancellation
// returns ctx.Err() — the underlying connection is left intact.
func (rr *RemoteReader) Next(ctx context.Context) ([]byte, uint64, error) {
	select {
	case it, ok := <-rr.queue:
		if !ok {
			// Channel closed: read loop ended. Surface the error if any.
			select {
			case err := <-rr.errCh:
				return nil, 0, err
			default:
				return nil, 0, io.EOF
			}
		}
		return it.payload, it.idx, nil
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

// NextBatch reads up to max records. Blocks for the first; drains best-effort
// for the rest.
func (rr *RemoteReader) NextBatch(ctx context.Context, max int) ([][]byte, []uint64, error) {
	if max <= 0 {
		return nil, nil, errors.New("axwal: NextBatch max must be > 0")
	}
	first, idx, err := rr.Next(ctx)
	if err != nil {
		return nil, nil, err
	}
	payloads := [][]byte{first}
	indices := []uint64{idx}
	for len(payloads) < max {
		select {
		case it, ok := <-rr.queue:
			if !ok {
				return payloads, indices, nil
			}
			payloads = append(payloads, it.payload)
			indices = append(indices, it.idx)
		default:
			return payloads, indices, nil
		}
	}
	return payloads, indices, nil
}

// Ack notifies the source-side cursor of progress. It's batched: we send an
// actual ACK frame only every AckEvery records (or on Close).
func (rr *RemoteReader) Ack(index uint64) error {
	rr.pendingMu.Lock()
	if index < rr.pendingIdx {
		rr.pendingMu.Unlock()
		return fmt.Errorf("axwal: remote ack %d < pending %d", index, rr.pendingIdx)
	}
	rr.pendingIdx = index
	gap := index - rr.lastAcked
	rr.pendingMu.Unlock()
	if int(gap) >= rr.opts.AckEvery {
		return rr.flushAck(index)
	}
	return nil
}

func (rr *RemoteReader) flushAck(index uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], index)
	rr.writeMu.Lock()
	defer rr.writeMu.Unlock()
	if err := writeFrame(rr.conn, magicAck, b[:]); err != nil {
		return err
	}
	rr.pendingMu.Lock()
	rr.lastAcked = index
	rr.pendingMu.Unlock()
	return nil
}

// Close terminates the network connection. Sends one final ACK with the most
// recently observed index so the server-side cursor is up to date.
func (rr *RemoteReader) Close() error {
	rr.closeOnce.Do(func() {
		// best-effort: flush the final ACK before tearing down
		rr.pendingMu.Lock()
		idx := rr.pendingIdx
		last := rr.lastAcked
		rr.pendingMu.Unlock()
		if idx > last {
			_ = rr.flushAck(idx)
		}
		close(rr.closed)
		_ = rr.conn.Close()
		if rr.sshClient != nil {
			_ = rr.sshClient.Close()
		}
		rr.wg.Wait()
	})
	return nil
}

// readLoop is the single goroutine consuming frames from the wire and
// forwarding records into the queue.
func (rr *RemoteReader) readLoop() {
	defer rr.wg.Done()
	defer close(rr.queue)
	for {
		magic, body, err := readFrame(rr.conn)
		if err != nil {
			// A clean EOF is a lost connection too (server restarted): callers
			// following a live WAL must see it as an error and reconnect.
			if errors.Is(err, io.EOF) {
				err = fmt.Errorf("axwal: server closed the connection: %w", io.ErrUnexpectedEOF)
			}
			select {
			case rr.errCh <- err:
			default:
			}
			return
		}
		rr.lastRx.Store(time.Now().UnixNano())
		switch magic {
		case magicBatch:
			indices, payloads, err := decodeBatch(body)
			if err != nil {
				select {
				case rr.errCh <- err:
				default:
				}
				return
			}
			for i, idx := range indices {
				select {
				case rr.queue <- recordItem{idx: idx, payload: payloads[i]}:
				case <-rr.closed:
					return
				}
			}
		case magicPing:
			// keepalive — nothing to do
		case magicErr:
			var ep errPayload
			_ = json.Unmarshal(body, &ep)
			select {
			case rr.errCh <- fmt.Errorf("server: %s", ep.Error):
			default:
			}
			return
		default:
			select {
			case rr.errCh <- fmt.Errorf("axwal: unexpected frame %q", string(magic[:])):
			default:
			}
			return
		}
	}
}

// watchdog closes the transport when the server has been silent for longer
// than IdleTimeout. The server pings idle connections, so silence means the
// link is dead even if the OS hasn't noticed; closing it makes the blocked
// read fail and the error reach Next.
func (rr *RemoteReader) watchdog() {
	defer rr.wg.Done()
	t := time.NewTicker(rr.opts.IdleTimeout / 4)
	defer t.Stop()
	for {
		select {
		case <-rr.closed:
			return
		case <-t.C:
		}
		idle := time.Since(time.Unix(0, rr.lastRx.Load()))
		if idle < rr.opts.IdleTimeout {
			continue
		}
		select {
		case rr.errCh <- fmt.Errorf("axwal: no data or keepalive from %s for %s; connection considered dead",
			rr.opts.Addr, idle.Round(time.Millisecond)):
		default:
		}
		_ = rr.conn.Close()
		if rr.sshClient != nil {
			_ = rr.sshClient.Close()
		}
		return
	}
}
