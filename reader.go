package axwal

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

	// PollInterval is the fallback wakeup interval when fsnotify isn't usable
	// or just to bound latency. Defaults to 100ms.
	PollInterval time.Duration
}

// Reader is a tail-and-follow consumer of a WAL.
type Reader struct {
	dir   string
	opts  ReaderOptions
	cur   *cursorState
	notif *notifier

	// nextIdx is the index of the record to be returned by the next Next() call.
	nextIdx uint64

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

	cs, err := openCursor(opts.Dir, opts.Name)
	if err != nil {
		return nil, err
	}

	r := &Reader{
		dir:  opts.Dir,
		opts: opts,
		cur:  cs,
	}

	// Determine starting index.
	if cs.rec.Index > 0 {
		// Resume after the last Ack'd record.
		r.nextIdx = cs.rec.Index + 1
	} else {
		switch opts.StartFrom {
		case StartLatest:
			latest, err := r.scanLastIndex()
			if err != nil {
				_ = cs.Close()
				return nil, err
			}
			r.nextIdx = latest + 1
		case StartAt:
			if opts.StartAtIndex == 0 {
				_ = cs.Close()
				return nil, errors.New("axwal: StartAt requires StartAtIndex > 0")
			}
			r.nextIdx = opts.StartAtIndex
		default: // StartEarliest
			earliest, err := r.scanFirstIndex()
			if err != nil {
				_ = cs.Close()
				return nil, err
			}
			if earliest == 0 {
				earliest = 1
			}
			r.nextIdx = earliest
		}
	}

	// Fast-forward: if the cursor (or StartAt) points at an index that retention
	// has already deleted, jump to the earliest available index and warn.
	if earliest, err := r.scanFirstIndex(); err == nil && earliest > 0 && r.nextIdx < earliest {
		log.Printf("axwal: reader %q: cursor index %d is below earliest available %d; fast-forwarding (data loss)", opts.Name, r.nextIdx, earliest)
		r.nextIdx = earliest
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

// Next blocks until the next record is available (tail-and-follow) or ctx is
// canceled. Returns (payload, index, nil) on success.
//
// IMPORTANT: Next does not advance the persisted cursor. Call Ack(index) once
// processing of the returned record is complete and durable from your side.
// On reader restart you'll resume at index+1.
func (r *Reader) Next(ctx context.Context) ([]byte, uint64, error) {
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
			idx := r.nextIdx
			r.nextIdx++
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

// Ack persists `index` as the last successfully processed record. Fsynced.
// Index must be >= the previously Ack'd index.
func (r *Reader) Ack(index uint64) error {
	return r.cur.Ack(index)
}

// CurrentIndex returns the most recently Ack'd index.
func (r *Reader) CurrentIndex() uint64 {
	return r.cur.CurrentIndex()
}

// NextIndex returns the index of the next record that Next() will produce.
func (r *Reader) NextIndex() uint64 {
	return r.nextIdx
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
	target := r.nextIdx
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
	if s.Compression == "zst" {
		dec, err := zstd.NewReader(f)
		if err != nil {
			f.Close()
			return fmt.Errorf("zstd reader: %w", err)
		}
		r.zstd = dec
		src = dec
		r.segCompressed = true
	}

	r.file = f
	r.br = bufio.NewReaderSize(src, 64*1024)
	r.segPath = path
	r.segFirst = s.FirstIndex
	r.segLast = s.LastIndex
	r.segIsOpen = !s.Closed

	// Skip records until we land on r.nextIdx.
	skip := r.nextIdx - r.segFirst
	for i := uint64(0); i < skip; i++ {
		if _, err := readRecord(r.br); err != nil {
			return fmt.Errorf("skipping to index %d in %s: %w", r.nextIdx, s.Name, err)
		}
	}
	return nil
}

// tryAdvanceSegment is called after an EOF on the current segment.
// Returns true if it switched to a new segment; false if there is none yet.
func (r *Reader) tryAdvanceSegment() (bool, error) {
	candidates, err := r.listSegments()
	if err != nil {
		return false, err
	}
	target := r.nextIdx

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
	// Prefer a closed segment with the highest LastIndex.
	var have bool
	var best uint64
	for _, s := range segs {
		if s.Closed && (!have || s.LastIndex > best) {
			best = s.LastIndex
			have = true
		}
	}
	if have {
		return best, nil
	}
	// Otherwise scan the open segment to find its real tail.
	last := segs[len(segs)-1]
	if last.Closed {
		return last.LastIndex, nil
	}
	idx, _, err := readOpenSegmentTail(filepath.Join(r.dir, last.Name), last.FirstIndex)
	return idx, err
}

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
