package axwal

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/klauspost/compress/zstd"
)

// StartPosition controls where a fresh reader (no cursor file) begins.
type StartPosition int

const (
	// StartEarliest: begin at the lowest index still present in the WAL.
	StartEarliest StartPosition = iota
	// StartLatest: skip everything currently on disk; read only records
	// appended after registration.
	StartLatest
	// StartAt: begin at the index given in ReaderOptions.StartAtIndex.
	StartAt
)

// ReaderOptions configures a Reader.
type ReaderOptions struct {
	// Dir is the WAL directory.
	Dir string

	// Name is the cursor name. Must be unique per active reader process.
	// Persists at <Dir>/cursors/<Name>.cursor.
	Name string

	// StartFrom controls behavior on first registration (no existing cursor).
	StartFrom StartPosition

	// StartAtIndex is used when StartFrom == StartAt. The first record returned
	// will be at this index (or the closest available if it has been GC'd).
	StartAtIndex uint64

	// HeartbeatInterval — how often the cursor's heartbeat is refreshed.
	// Should be < writer's StaleReaderTTL/2. Defaults to 5s.
	HeartbeatInterval time.Duration

	// AckBatchInterval, when > 0, switches Ack() into batched mode: Ack()
	// updates the in-memory cursor immediately but defers the fsync to a
	// background goroutine that flushes every AckBatchInterval. This typically
	// raises read throughput by 1-3 orders of magnitude.
	//
	// Trade-off: after a reader crash or hard kill, up to AckBatchInterval's
	// worth of records may be re-delivered (at-least-once still holds, but the
	// "least" gets larger).
	//
	// On clean Close() the latest Ack is flushed so no progress is lost.
	// When 0 (default), Ack() fsyncs immediately.
	AckBatchInterval time.Duration

	// PollInterval is the fallback wakeup interval when fsnotify isn't usable
	// or just to bound latency. Defaults to 100ms.
	PollInterval time.Duration

	// Encryption, if non-nil, opens records sealed by a writer with the same
	// key. Mismatch returns ErrEncryption from Next/NextBatch.
	Encryption *EncryptionConfig

	// UseMmap, when true, memory-maps closed uncompressed segments instead of
	// reading them through bufio. Faster on huge cold-replay scans (multi-GB
	// segments) because parsing happens out of the page cache directly. Has
	// no effect on the active open segment (file size grows) or on .zst
	// segments (decoder needs a streaming reader). Cleanup is automatic on
	// segment switch / Close.
	UseMmap bool

	// Logger receives reader-side log events. Defaults to slog.Default().
	Logger *slog.Logger

	// forceStart (with StartFrom == StartAt) discards an existing cursor
	// position and starts at StartAtIndex. Used by the server for replicas.
	forceStart bool
}

// ReaderStats is a thread-safe snapshot of reader counters/gauges.
type ReaderStats struct {
	Name           string
	NextIndex      uint64
	AckedIndex     uint64
	NextCalls      uint64
	AckCalls       uint64
	ErrCorruptSegs uint64 // closed segments that returned ErrCorruptRecord
	SegmentSwitches uint64
	WaitWakeups    uint64
}

// Reader is a tail-and-follow consumer of a WAL.
type Reader struct {
	dir    string
	opts   ReaderOptions
	cur    *cursorState
	notif  *notifier
	log    *slog.Logger
	cipher *cipherAEAD

	cnt readerCounters

	// nextIdx is the index of the record to be returned by the next Next()
	// call. Atomic so Stats() can read it from another goroutine; the Reader
	// itself is contractually single-goroutine (Next not safe for concurrent use).
	nextIdx atomic.Uint64

	// segPath is the file currently open (may be the live open segment, a
	// closed segment, or a .zst). Empty if none open.
	segPath  string
	segFirst uint64
	segLast  uint64 // 0 if open segment
	segIsOpen bool
	segCompressed bool

	file    *os.File
	br      *bufio.Reader
	zstd    *zstd.Decoder
	mmap    []byte // non-nil iff segment is mmap'd
}

type readerCounters struct {
	nextCalls       atomic.Uint64
	ackCalls        atomic.Uint64
	errCorruptSegs  atomic.Uint64
	segmentSwitches atomic.Uint64
	waitWakeups     atomic.Uint64
}

// OpenReader registers a reader with the given name. Returns an error if a
// reader by that name is already active in another process.
func OpenReader(opts ReaderOptions) (*Reader, error) {
	if opts.Dir == "" {
		return nil, errors.New("axwal: ReaderOptions.Dir is required")
	}
	if opts.Name == "" {
		return nil, errors.New("axwal: ReaderOptions.Name is required")
	}
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = 5 * time.Second
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 100 * time.Millisecond
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	logger := opts.Logger.With("component", "axwal.reader", "dir", opts.Dir, "name", opts.Name)

	cipherInst, err := newCipherFromConfig(opts.Encryption)
	if err != nil {
		return nil, err
	}

	cs, err := openCursor(opts.Dir, opts.Name)
	if err != nil {
		return nil, err
	}

	// Wire up batched-ack mode and possibly tighten the heartbeat tick so the
	// cursor doesn't drift further behind than the user asked.
	if opts.AckBatchInterval > 0 {
		cs.batchAck = true
		if opts.AckBatchInterval < opts.HeartbeatInterval {
			opts.HeartbeatInterval = opts.AckBatchInterval
		}
	}

	r := &Reader{
		dir:    opts.Dir,
		opts:   opts,
		cur:    cs,
		log:    logger,
		cipher: cipherInst,
	}

	// Determine starting index.
	if opts.forceStart && opts.StartFrom == StartAt && opts.StartAtIndex > 0 {
		if err := cs.reset(opts.StartAtIndex - 1); err != nil {
			_ = cs.Close()
			return nil, err
		}
		r.nextIdx.Store(opts.StartAtIndex)
	} else if cs.rec.Index > 0 {
		// Resume after the last Ack'd record.
		r.nextIdx.Store(cs.rec.Index + 1)
	} else {
		switch opts.StartFrom {
		case StartLatest:
			latest, err := r.scanLastIndex()
			if err != nil {
				_ = cs.Close()
				return nil, err
			}
			r.nextIdx.Store(latest + 1)
		case StartAt:
			if opts.StartAtIndex == 0 {
				_ = cs.Close()
				return nil, errors.New("axwal: StartAt requires StartAtIndex > 0")
			}
			r.nextIdx.Store(opts.StartAtIndex)
		default: // StartEarliest
			earliest, err := r.scanFirstIndex()
			if err != nil {
				_ = cs.Close()
				return nil, err
			}
			if earliest == 0 {
				earliest = 1
			}
			r.nextIdx.Store(earliest)
		}
	}

	// Fast-forward: if the cursor (or StartAt) points at an index that retention
	// has already deleted, jump to the earliest available index and warn.
	if earliest, err := r.scanFirstIndex(); err == nil && earliest > 0 && r.nextIdx.Load() < earliest {
		logger.Warn("cursor below earliest available; fast-forwarding (data loss)",
			"cursor_index", r.nextIdx.Load(), "earliest", earliest)
		r.nextIdx.Store(earliest)
	}

	cs.startHeartbeat(opts.HeartbeatInterval)

	n, err := newNotifier(opts.Dir)
	if err != nil {
		_ = cs.Close()
		return nil, err
	}
	r.notif = n

	return r, nil
}

// HeadIndex returns the highest record index currently on disk in the WAL,
// regardless of whether the reader has consumed it. Cheap (one glob).
//
// For closed segments it uses the LastIndex encoded in the filename. For an
// open segment it scans the file tail with CRC validation — that read is O(N)
// in the open segment's size, so don't call this in tight loops if you have
// huge open segments. Use Lag() for the typical metric.
func (r *Reader) HeadIndex() (uint64, error) {
	return r.scanLastIndex()
}

// Lag returns how many records and bytes are between the reader's last Ack and
// the current head of the WAL.
//
// records = HeadIndex() - CurrentIndex(). When the cursor is current, returns 0.
// bytes is the on-disk size of all closed segments whose LastIndex > cursor,
// plus the full size of the open segment if any. It overestimates slightly
// (counts framing overhead) but is good enough for "consumer is falling behind"
// alerts.
func (r *Reader) Lag() (records uint64, bytes int64, err error) {
	cursor := r.cur.CurrentIndex()
	head, err := r.scanLastIndex()
	if err != nil {
		return 0, 0, err
	}
	if head > cursor {
		records = head - cursor
	}

	segs, err := r.listSegments()
	if err != nil {
		return records, 0, err
	}
	for _, s := range segs {
		path := filepath.Join(r.dir, s.Name)
		fi, statErr := os.Stat(path)
		if statErr != nil {
			continue
		}
		if s.Closed {
			// Include the segment if any of its records are unread.
			if s.LastIndex > cursor {
				bytes += fi.Size()
			}
		} else {
			// Open segment: always counted toward lag (we don't know exactly how
			// many of its bytes are past the cursor without re-walking it).
			bytes += fi.Size()
		}
	}
	return records, bytes, nil
}

// Stats returns a thread-safe snapshot of reader counters and gauges.
func (r *Reader) Stats() ReaderStats {
	return ReaderStats{
		Name:            r.opts.Name,
		NextIndex:       r.nextIdx.Load(),
		AckedIndex:      r.cur.CurrentIndex(),
		NextCalls:       r.cnt.nextCalls.Load(),
		AckCalls:        r.cnt.ackCalls.Load(),
		ErrCorruptSegs:  r.cnt.errCorruptSegs.Load(),
		SegmentSwitches: r.cnt.segmentSwitches.Load(),
		WaitWakeups:     r.cnt.waitWakeups.Load(),
	}
}

// Next blocks until the next record is available (tail-and-follow) or ctx is
// canceled. Returns (payload, index, nil) on success.
//
// IMPORTANT: Next does not advance the persisted cursor. Call Ack(index) once
// processing of the returned record is complete and durable from your side.
// On reader restart you'll resume at index+1.
func (r *Reader) Next(ctx context.Context) ([]byte, uint64, error) {
	r.cnt.nextCalls.Add(1)
	for {
		// Make sure we have a segment that contains nextIdx.
		if r.file == nil {
			if err := r.openSegmentForIndex(ctx); err != nil {
				return nil, 0, err
			}
			if r.file == nil {
				// No segment yet — wait and retry.
				if err := r.waitForChange(ctx); err != nil {
					return nil, 0, err
				}
				continue
			}
		}

		payload, err := readRecord(r.readerSource())
		switch {
		case err == nil:
			idx := r.nextIdx.Load()
			r.nextIdx.Store(idx + 1)
			if r.cipher != nil {
				pt, derr := r.cipher.open(payload)
				if derr != nil {
					return nil, idx, derr
				}
				payload = pt
			}
			return payload, idx, nil

		case errors.Is(err, io.EOF):
			// Possibly: writer just appended (open segment), writer rotated
			// (closed under us), or genuine end of stream. Try advancing first;
			// if no successor segment exists, wait.
			advanced, aerr := r.tryAdvanceSegment()
			if aerr != nil {
				return nil, 0, aerr
			}
			if advanced {
				continue
			}
			if err := r.waitForChange(ctx); err != nil {
				return nil, 0, err
			}
			continue

		case errors.Is(err, ErrTruncatedRecord):
			// Two possible causes on what was the open segment:
			//   (a) writer is mid-Append: bytes flushed into page cache but the
			//       record isn't complete yet. Wait, then re-read from the same
			//       file at the same logical position.
			//   (b) writer rotated: the file was renamed wal-F- -> wal-F-L while
			//       we were mid-frame. The renamed file is still readable via
			//       our FD, but we want to refresh from disk to pick up the
			//       new closed form.
			// Both are handled the same way: drop our state and let
			// openSegmentForIndex pick the right segment for r.nextIdx on the
			// next loop turn.
			if r.segIsOpen {
				r.closeCurrentFile()
				if err := r.waitForChange(ctx); err != nil {
					return nil, 0, err
				}
				continue
			}
			return nil, 0, fmt.Errorf("closed segment %s truncated mid-record: %w", r.segPath, err)

		default:
			return nil, 0, err
		}
	}
}

// NextBatch reads up to `max` records from the current position and returns
// them. It blocks until at least ONE record is available (or ctx is canceled),
// then drains as much as it can without further waits — so the second through
// max-th records are best-effort. Useful when callers want to amortize per-
// record processing overhead (DB inserts, S3 multipart) but don't want to
// stall waiting for a full batch on a quiet WAL.
//
// Returned slices are length-aligned: payloads[i] corresponds to indices[i].
// On error mid-batch, returns whatever was read so far + the error; the caller
// should still Ack(indices[len-1]) for the successful prefix.
func (r *Reader) NextBatch(ctx context.Context, max int) (payloads [][]byte, indices []uint64, err error) {
	if max <= 0 {
		return nil, nil, errors.New("axwal: NextBatch max must be > 0")
	}
	// First record: blocking. Reuses Next.
	first, idx, err := r.Next(ctx)
	if err != nil {
		return nil, nil, err
	}
	payloads = append(payloads, first)
	indices = append(indices, idx)

	// Subsequent records: non-blocking. We try to read directly through the
	// already-open file. Stop on the first I/O hiccup (EOF, ErrTruncatedRecord,
	// segment switch) — the caller can come back via NextBatch on the next tick.
	for len(payloads) < max && r.file != nil {
		payload, perr := readRecord(r.readerSource())
		if perr != nil {
			// Stop gracefully on EOF / truncation; surface real errors only if
			// we have nothing yet (we don't — we have at least one record).
			if errors.Is(perr, io.EOF) || errors.Is(perr, ErrTruncatedRecord) {
				if errors.Is(perr, ErrTruncatedRecord) && r.segIsOpen {
					// Mirror Next() behavior: drop the file so the next call
					// re-opens at the proper segment.
					r.closeCurrentFile()
				}
				break
			}
			return payloads, indices, perr
		}
		nextIdx := r.nextIdx.Load()
		r.nextIdx.Store(nextIdx + 1)
		r.cnt.nextCalls.Add(1)
		if r.cipher != nil {
			pt, derr := r.cipher.open(payload)
			if derr != nil {
				return payloads, indices, derr
			}
			payload = pt
		}
		payloads = append(payloads, payload)
		indices = append(indices, nextIdx)
	}
	return payloads, indices, nil
}

// Ack persists `index` as the last successfully processed record. With
// AckBatchInterval=0 (default) the call returns only after fsync. With
// AckBatchInterval>0 it's lock-free and the fsync happens in the background.
// Index must be >= the previously Ack'd index.
func (r *Reader) Ack(index uint64) error {
	r.cnt.ackCalls.Add(1)
	return r.cur.Ack(index)
}

// CurrentIndex returns the most recently Ack'd index.
func (r *Reader) CurrentIndex() uint64 {
	return r.cur.CurrentIndex()
}

// NextIndex returns the index of the next record that Next() will produce.
func (r *Reader) NextIndex() uint64 {
	return r.nextIdx.Load()
}

// Close releases resources but does NOT delete the cursor. Re-opening with the
// same name will resume at CurrentIndex+1.
func (r *Reader) Close() error {
	r.closeCurrentFile()
	if r.notif != nil {
		_ = r.notif.Close()
	}
	return r.cur.Close()
}

// readerSource returns the io.Reader used by readRecord. We always read through
// a bufio.Reader for performance; for compressed segments the decoder sits in
// front.
func (r *Reader) readerSource() io.Reader {
	return r.br
}

func (r *Reader) closeCurrentFile() {
	if r.zstd != nil {
		r.zstd.Close()
		r.zstd = nil
	}
	if r.mmap != nil {
		_ = munmap(r.mmap)
		r.mmap = nil
	}
	if r.file != nil {
		_ = r.file.Close()
		r.file = nil
	}
	r.br = nil
	r.segPath = ""
	r.segFirst = 0
	r.segLast = 0
	r.segIsOpen = false
	r.segCompressed = false
}

// openSegmentForIndex opens whichever segment contains r.nextIdx. If none is
// available yet, leaves r.file == nil and returns nil error (caller will wait).
func (r *Reader) openSegmentForIndex(ctx context.Context) error {
	target := r.nextIdx.Load()
	candidates, err := r.listSegments()
	if err != nil {
		return err
	}
	// Prefer a closed segment that exactly contains the target index.
	for _, s := range candidates {
		if s.Closed && s.FirstIndex <= target && target <= s.LastIndex {
			return r.openSegment(s)
		}
	}
	// Otherwise: the open segment if its FirstIndex <= target.
	for _, s := range candidates {
		if !s.Closed && s.FirstIndex <= target {
			return r.openSegment(s)
		}
	}
	// No suitable segment yet.
	return nil
}

func (r *Reader) openSegment(s segName) error {
	r.closeCurrentFile()
	path := filepath.Join(r.dir, s.Name)
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	var src io.Reader = f
	r.segCompressed = s.Compression == "zst"

	if r.segCompressed {
		dec, err := zstd.NewReader(f)
		if err != nil {
			f.Close()
			return fmt.Errorf("zstd reader: %w", err)
		}
		r.zstd = dec
		src = dec
	} else if r.opts.UseMmap && s.Closed {
		// mmap the closed uncompressed segment for parse-from-page-cache speed.
		fi, statErr := f.Stat()
		if statErr == nil && fi.Size() > 0 {
			data, mmapErr := mmapReadOnly(f, fi.Size())
			if mmapErr == nil {
				r.mmap = data
				src = bytes.NewReader(data)
			} else {
				r.log.Warn("mmap failed; falling back to bufio", "name", s.Name, "err", mmapErr)
			}
		}
	}

	r.file = f
	r.br = bufio.NewReaderSize(src, 64*1024)
	r.segPath = path
	r.segFirst = s.FirstIndex
	r.segLast = s.LastIndex
	r.segIsOpen = !s.Closed

	// Skip records until we land on r.nextIdx.
	target := r.nextIdx.Load()
	skip := target - r.segFirst
	for i := uint64(0); i < skip; i++ {
		if _, err := readRecord(r.br); err != nil {
			return fmt.Errorf("skipping to index %d in %s: %w", target, s.Name, err)
		}
	}
	return nil
}

// tryAdvanceSegment is called after an EOF on the current segment.
// Returns true if it switched to a new segment; false if there is none yet.
func (r *Reader) tryAdvanceSegment() (bool, error) {
	// Fast path for the common case "caught up with the writer": rotation
	// renames the open segment, so as long as it still exists under its open
	// name there is nothing to advance to — no need to list the directory on
	// every wakeup.
	if r.segIsOpen {
		if _, err := os.Stat(r.segPath); err == nil {
			return false, nil
		}
	}
	candidates, err := r.listSegments()
	if err != nil {
		return false, err
	}
	target := r.nextIdx.Load()

	// First: a closed segment containing target (could be the rotated form of
	// what was previously the open segment).
	for _, s := range candidates {
		if s.Closed && s.FirstIndex <= target && target <= s.LastIndex {
			if filepath.Join(r.dir, s.Name) == r.segPath {
				// Same on-disk file we already have open and just hit EOF on.
				continue
			}
			return true, r.openSegment(s)
		}
	}
	// Second: an open segment with FirstIndex <= target (newer than current).
	for _, s := range candidates {
		if !s.Closed && s.FirstIndex <= target {
			if filepath.Join(r.dir, s.Name) == r.segPath {
				continue
			}
			return true, r.openSegment(s)
		}
	}
	return false, nil
}

// waitForChange blocks until either a relevant filesystem event arrives, the
// poll interval ticks, or ctx is canceled.
func (r *Reader) waitForChange(ctx context.Context) error {
	t := time.NewTimer(r.opts.PollInterval)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	case <-r.notif.Events():
		return nil
	}
}

func (r *Reader) listSegments() ([]segName, error) {
	matches, err := filepath.Glob(filepath.Join(r.dir, "wal-*"))
	if err != nil {
		return nil, err
	}
	var segs []segName
	for _, p := range matches {
		base := filepath.Base(p)
		if base == writerLockFile {
			continue
		}
		s, err := parseSegName(base)
		if err != nil {
			continue
		}
		segs = append(segs, s)
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].FirstIndex < segs[j].FirstIndex })
	return segs, nil
}

func (r *Reader) scanFirstIndex() (uint64, error) {
	segs, err := r.listSegments()
	if err != nil {
		return 0, err
	}
	if len(segs) == 0 {
		return 0, nil
	}
	return segs[0].FirstIndex, nil
}

func (r *Reader) scanLastIndex() (uint64, error) {
	segs, err := r.listSegments()
	if err != nil {
		return 0, err
	}
	if len(segs) == 0 {
		return 0, nil
	}
	// Highest LastIndex among closed segments...
	var best uint64
	for _, s := range segs {
		if s.Closed && s.LastIndex > best {
			best = s.LastIndex
		}
	}
	// ...extended by the open segment, whose real tail needs a CRC walk.
	last := segs[len(segs)-1]
	if last.Closed || last.FirstIndex <= best {
		return best, nil
	}
	idx, _, err := readOpenSegmentTail(filepath.Join(r.dir, last.Name), last.FirstIndex, discardLog)
	if err != nil {
		return 0, err
	}
	if idx > best {
		best = idx
	}
	return best, nil
}

// discardLog silences helpers that log recovery-style warnings when they are
// used for read-only probing (a partial frame at the tail of a live open
// segment is normal there).
var discardLog = slog.New(slog.NewTextHandler(io.Discard, nil))

// notifier wraps fsnotify with a channel that emits at most one event per dir
// activity wave. If fsnotify can't watch the dir, we degrade to nil and rely on
// the reader's poll fallback.
type notifier struct {
	w     *fsnotify.Watcher
	out   chan struct{}
	close chan struct{}
}

func newNotifier(dir string) (*notifier, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return &notifier{out: make(chan struct{}), close: make(chan struct{})}, nil
	}
	if err := w.Add(dir); err != nil {
		w.Close()
		return &notifier{out: make(chan struct{}), close: make(chan struct{})}, nil
	}
	n := &notifier{w: w, out: make(chan struct{}, 1), close: make(chan struct{})}
	go n.loop()
	return n, nil
}

func (n *notifier) loop() {
	for {
		select {
		case <-n.close:
			return
		case ev, ok := <-n.w.Events:
			if !ok {
				return
			}
			if !strings.HasPrefix(filepath.Base(ev.Name), "wal-") {
				continue
			}
			select {
			case n.out <- struct{}{}:
			default:
			}
		case <-n.w.Errors:
			// Ignore; poll fallback covers gaps.
		}
	}
}

func (n *notifier) Events() <-chan struct{} { return n.out }

func (n *notifier) Close() error {
	select {
	case <-n.close:
		return nil
	default:
		close(n.close)
	}
	if n.w != nil {
		return n.w.Close()
	}
	return nil
}
