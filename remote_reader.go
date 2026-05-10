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

	// QueueDepth bounds how many records may be buffered in memory between
	// the network read goroutine and Next/NextBatch callers. Defaults to
	// BatchSize * 4.
	QueueDepth int

	Logger *slog.Logger
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
	if opts.SSHTunnel != nil {
		sshClient, err = dialSSH(ctx, opts.SSHTunnel)
		if err != nil {
			return nil, fmt.Errorf("ssh dial: %w", err)
		}
		conn, err = sshClient.Dial("tcp", opts.Addr)
		if err != nil {
			_ = sshClient.Close()
			return nil, fmt.Errorf("tunnel dial %s: %w", opts.Addr, err)
		}
		if opts.SSHTunnel.KnownHostsFile == "" {
			log.Warn("SSH host-key verification DISABLED (no KnownHostsFile)",
				"ssh_host", opts.SSHTunnel.Host)
		}
	} else {
		d := net.Dialer{Timeout: opts.DialTimeout}
		conn, err = d.DialContext(ctx, "tcp", opts.Addr)
		if err != nil {
			return nil, fmt.Errorf("dial: %w", err)
		}
	}

	hello := helloPayload{Name: opts.Name, StartIndex: opts.StartIndex, BatchSize: opts.BatchSize}
	body, _ := json.Marshal(hello)
	if err := writeFrame(conn, magicHello, body); err != nil {
		_ = conn.Close()
		if sshClient != nil {
			_ = sshClient.Close()
		}
		return nil, fmt.Errorf("send HELLO: %w", err)
	}
	magic, body, err := readFrame(conn)
	if err != nil {
		_ = conn.Close()
		if sshClient != nil {
			_ = sshClient.Close()
		}
		return nil, fmt.Errorf("read OK: %w", err)
	}
	if magic == magicErr {
		var ep errPayload
		_ = json.Unmarshal(body, &ep)
		_ = conn.Close()
		if sshClient != nil {
			_ = sshClient.Close()
		}
		return nil, fmt.Errorf("server: %s", ep.Error)
	}
	if magic != magicOK {
		_ = conn.Close()
		if sshClient != nil {
			_ = sshClient.Close()
		}
		return nil, fmt.Errorf("expected OK, got %q", string(magic[:]))
	}
	if len(body) != 8 {
		_ = conn.Close()
		if sshClient != nil {
			_ = sshClient.Close()
		}
		return nil, fmt.Errorf("malformed OK body (%d bytes)", len(body))
	}
	head := binary.BigEndian.Uint64(body)

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
	rr.wg.Add(1)
	go rr.readLoop()
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
			if !errors.Is(err, io.EOF) {
				select {
				case rr.errCh <- err:
				default:
				}
			}
			return
		}
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
