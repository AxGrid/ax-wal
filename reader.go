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

	// SkipCorruptSegments decides what happens when a CLOSED segment turns
	// out to be damaged: a record fails its CRC, the file ends before the
	// last index its name promises, or it can't be decompressed.
	//
	// false (default): Next returns the error (wrapping ErrCorruptRecord or
	// ErrTruncatedRecord) and keeps returning it — the reader does not move
	// past records it could not deliver.
	//
	// true: the rest of that segment is given up; the reader logs an error,
	// counts the lost records in ReaderStats.SkippedRecords and continues
	// with the next segment.
	SkipCorruptSegments bool

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
	// SkippedRecords counts records the reader had to jump over because they
	// no longer exist: their segment was deleted (retention, TruncatePrefix,
	// by hand) or, with SkipCorruptSegments, was damaged.
	SkippedRecords uint64
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

	// segOff is the byte offset in the current segment just past the last
	// record read; resume remembers it across a drop-and-reopen so a reader
	// sitting on the writer's tail doesn't rescan the segment from its start
	// every time it catches a record mid-write.
	segOff int64
	resume struct {
		ok    bool
		first uint64 // FirstIndex of the segment
		idx   uint64 // record index that sits at off
		off   int64
	}

	// gapSince / gapTarget: when the reader first noticed that the record it
	// needs isn't in any segment while later ones exist.
	gapSince  time.Time
	gapTarget uint64
	// warnedBehind: the "writer is behind this reader" warning was logged.
	warnedBehind bool
	// bad*: where a CRC-invalid frame was seen at the tail of the open
	// segment, and how big the file was then.
	badSeen  bool
	badFirst uint64
	badOff   int64
	badSize  int64

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
	skippedRecords  atomic.Uint64
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
		SkippedRecords:  r.cnt.skippedRecords.Load(),
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
				// Nothing holds nextIdx. Either we are at the end of the log
				// (wait for the writer) or the records are gone for good
				// (jump to the next ones that exist).
				if r.skipGap() {
					continue
				}
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
			r.segOff += int64(headerSize + len(payload))
			if r.cipher != nil {
				pt, derr := r.cipher.open(payload)
				if derr != nil {
					return nil, idx, derr
				}
				payload = pt
			}
			return payload, idx, nil

		case errors.Is(err, io.EOF) && r.segIsOpen:
			// Possibly: writer just appended, writer rotated (the file was
			// renamed under us), or genuine end of stream. Try advancing
			// first; if no successor segment exists, wait.
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

		case errors.Is(err, io.EOF):
			// End of a closed segment.
			if r.nextIdx.Load() <= r.segLast {
				// It holds fewer records than its name promises.
				if serr := r.badSegment(fmt.Errorf("%w: file ends at index %d, expected %d",
					ErrTruncatedRecord, r.nextIdx.Load()-1, r.segLast)); serr != nil {
					return nil, 0, serr
				}
				continue
			}
			// Done with it; the next loop turn finds the successor, waits
			// for one, or detects that it is missing.
			r.closeCurrentFile()
			continue

		case r.segIsOpen && errors.Is(err, ErrCorruptRecord) && r.openSegmentDamaged():
			// A complete but invalid frame, and the writer has appended past
			// it since we first saw it: not a torn tail, the segment itself
			// is damaged. Waiting won't help.
			name, at := filepath.Base(r.segPath), r.nextIdx.Load()
			r.cnt.errCorruptSegs.Add(1)
			r.dropForRetry()
			return nil, 0, fmt.Errorf("axwal: open segment %s is damaged at index %d (the writer kept appending past it): %w",
				name, at, err)

		case r.segIsOpen && (errors.Is(err, ErrTruncatedRecord) || errors.Is(err, ErrCorruptRecord)):
			// The tail of the open segment isn't a complete record:
			//   (a) the writer is mid-Append — the rest arrives in a moment;
			//   (b) the writer rotated while we were mid-frame;
			//   (c) the writer crashed mid-write — its recovery will cut the
			//       torn tail off on restart.
			// In every case: remember where this record starts, let go of
			// the file, and look again after the next change.
			r.dropForRetry()
			if err := r.waitForChange(ctx); err != nil {
				return nil, 0, err
			}
			continue

		default:
			if r.segIsOpen {
				return nil, 0, err
			}
			// A closed segment is immutable: this will not get better.
			if serr := r.badSegment(err); serr != nil {
				return nil, 0, serr
			}
			continue
		}
	}
}

// openSegmentDamaged is called when the frame at the reader's position in
// the open segment fails its CRC. The first time it just notes the position
// (a writer that crashed mid-write leaves exactly this, and its recovery
// removes it). It reports true once the file has grown beyond that point
// while the frame is still invalid.
func (r *Reader) openSegmentDamaged() bool {
	var size int64
	if fi, err := r.file.Stat(); err == nil {
		size = fi.Size()
	}
	if r.badSeen && r.badFirst == r.segFirst && r.badOff == r.segOff {
		return size > r.badSize
	}
	r.badSeen, r.badFirst, r.badOff, r.badSize = true, r.segFirst, r.segOff, size
	return false
}

// gapGrace is how long a hole in the log must persist before the reader
// jumps over it. A directory listing taken while the writer renames a
// segment can momentarily miss that segment; waiting rules that out before
// records are declared gone.
var gapGrace = time.Second

// skipGap is called when no segment holds nextIdx. If later records exist it
// is a hole — the segment was deleted by retention, TruncatePrefix or by
// hand — and once that has been true for gapGrace the reader moves to the
// first index that is still there. Returns true if it moved.
func (r *Reader) skipGap() bool {
	target := r.nextIdx.Load()
	segs, err := r.listSegments()
	if err != nil {
		return false
	}
	var next uint64
	for _, s := range segs {
		if s.FirstIndex <= target {
			if !s.Closed || target <= s.LastIndex {
				// Something does (or will) hold target: not a hole.
				r.gapSince = time.Time{}
				return false
			}
			continue
		}
		if next == 0 || s.FirstIndex < next {
			next = s.FirstIndex
		}
	}
	if next == 0 {
		r.gapSince = time.Time{} // end of the log
		return false
	}
	if r.gapSince.IsZero() || r.gapTarget != target {
		r.gapSince, r.gapTarget = time.Now(), target
		return false
	}
	if time.Since(r.gapSince) < gapGrace {
		return false
	}
	r.gapSince = time.Time{}
	r.cnt.skippedRecords.Add(next - target)
	r.log.Warn("records are gone from the WAL; skipping ahead (data loss for this reader)",
		"missing_from", target, "missing_to", next-1, "resuming_at", next)
	r.nextIdx.Store(next)
	return true
}

// badSegment handles a damaged closed segment according to
// SkipCorruptSegments. It returns the error to surface, or nil if the reader
// moved on to the next segment.
func (r *Reader) badSegment(cause error) error {
	r.cnt.errCorruptSegs.Add(1)
	name, last, at := filepath.Base(r.segPath), r.segLast, r.nextIdx.Load()
	// Either way let go of the file: the read position inside it can't be
	// trusted any more. Without skipping, the next call reopens the segment
	// and reports the same error again instead of handing out records under
	// wrong indices.
	r.closeCurrentFile()
	if !r.opts.SkipCorruptSegments {
		return fmt.Errorf("axwal: closed segment %s is damaged at index %d: %w", name, at, cause)
	}
	if last >= at {
		r.cnt.skippedRecords.Add(last - at + 1)
	}
	r.log.Error("closed segment is damaged; skipping the rest of it (data loss for this reader)",
		"name", name, "lost_from", at, "lost_to", last, "err", cause)
	r.nextIdx.Store(last + 1)
	return nil
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
			// Never fail here: we already hold at least one record. Leave the
			// reader in a state from which the next (blocking) call sorts it
			// out — a clean EOF needs nothing; after anything else the read
			// position is no longer on a record boundary, so let go of the
			// file and have Next reopen it and apply its error handling.
			if !errors.Is(perr, io.EOF) {
				if r.segIsOpen {
					r.dropForRetry()
				} else {
					r.closeCurrentFile()
				}
			}
			break
		}
		nextIdx := r.nextIdx.Load()
		r.nextIdx.Store(nextIdx + 1)
		r.segOff += int64(headerSize + len(payload))
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

// dropForRetry closes the current (open) segment after a partial frame was
// read at its tail, remembering where the next record starts.
func (r *Reader) dropForRetry() {
	first, off, idx := r.segFirst, r.segOff, r.nextIdx.Load()
	uncompressed := !r.segCompressed
	r.closeCurrentFile()
	if uncompressed {
		r.resume.ok, r.resume.first, r.resume.idx, r.resume.off = true, first, idx, off
	}
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
	r.resume.ok = false
	r.segOff = 0
	r.segPath = ""
	r.segFirst = 0
	r.segLast = 0
	r.segIsOpen = false
	r.segCompressed = false
}

// openSegmentForIndex opens whichever segment contains r.nextIdx. If none is
// available yet, leaves r.file == nil and returns nil error (caller will wait).
func (r *Reader) openSegmentForIndex(ctx context.Context) error {
	// A segment can vanish between listing and opening (compression swaps
	// wal-F-L for wal-F-L.zst, retention deletes); look again right away
	// a couple of times before falling back to waiting.
	for attempt := 0; attempt < 3; attempt++ {
		target := r.nextIdx.Load()
		candidates, err := r.listSegments()
		if err != nil {
			return err
		}
		var pick *segName
		// Prefer a closed segment that exactly contains the target index.
		for i, s := range candidates {
			if s.Closed && s.FirstIndex <= target && target <= s.LastIndex {
				pick = &candidates[i]
				break
			}
		}
		// Otherwise: the open segment if its FirstIndex <= target.
		if pick == nil {
			for i, s := range candidates {
				if !s.Closed && s.FirstIndex <= target {
					pick = &candidates[i]
					break
				}
			}
		}
		if pick == nil {
			return nil // no suitable segment yet
		}
		if err := r.openSegment(*pick); err != nil {
			return err
		}
		if r.file != nil || !pick.Closed {
			return nil
		}
	}
	return nil
}

func (r *Reader) openSegment(s segName) error {
	resume := r.resume
	r.closeCurrentFile()
	path := filepath.Join(r.dir, s.Name)
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		// Listed a moment ago, gone now: compressed (wal-F-L -> .zst),
		// rotated or deleted in between. Not an error — the caller looks at
		// the directory again.
		return nil
	}
	if err != nil {
		return err
	}
	r.cnt.segmentSwitches.Add(1)

	// Same segment we were just reading (possibly renamed to its closed form
	// by a rotation — the bytes are identical): continue at the saved offset
	// instead of walking every record from the start.
	target := r.nextIdx.Load()
	resumed := false
	if resume.ok && resume.first == s.FirstIndex && resume.idx == target && s.Compression == "" {
		if _, err := f.Seek(resume.off, io.SeekStart); err == nil {
			resumed = true
		}
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
	} else if r.opts.UseMmap && s.Closed && !resumed {
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

	if resumed {
		r.segOff = resume.off
		return nil
	}

	// Skip records until we land on r.nextIdx.
	skip := target - r.segFirst
	for i := uint64(0); i < skip; i++ {
		p, err := readRecord(r.br)
		if err == nil {
			r.segOff += int64(headerSize + len(p))
			continue
		}
		if !s.Closed {
			// The open segment doesn't reach our position (yet). Normally
			// impossible — we only ever ask for an index we were given or
			// the one after it — so the writer must have lost its tail in a
			// crash (SyncNone / SyncBarrier) and is rewriting those indices.
			// Wait for it to get here rather than fail.
			if !r.warnedBehind {
				r.warnedBehind = true
				r.log.Warn("the WAL ends before this reader's position; waiting for the writer to reach it "+
					"(records were lost on the writer side?)",
					"reader_index", target, "segment", s.Name, "segment_has", r.segFirst+i-1)
			}
			r.closeCurrentFile()
			return nil
		}
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("%w: file ends at index %d, expected %d", ErrTruncatedRecord, r.segFirst+i-1, s.LastIndex)
		}
		return r.badSegment(err)
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
	r.cnt.waitWakeups.Add(1)
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
