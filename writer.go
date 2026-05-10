package axwal

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// ErrBackpressure is returned by Append/AppendBatch when MaxPendingBytes is set
// and the unflushed-on-disk byte count would exceed it. The caller should slow
// down or shed load; retrying immediately will just hit the limit again.
var ErrBackpressure = errors.New("axwal: backpressure (pending bytes exceed MaxPendingBytes)")

// ErrCloseTimeout is returned by Close when CloseTimeout elapsed before all
// background goroutines (rotations, compressions) finished. The lock is still
// released and the writer is unusable; partially-rotated segments may remain
// on disk and will be cleaned up on the next OpenWriter() recovery pass.
var ErrCloseTimeout = errors.New("axwal: writer close timed out")

// WriterOptions configures a Writer.
type WriterOptions struct {
	// Dir is the WAL directory. Created if missing.
	Dir string

	// MaxSegmentSize is the per-segment size threshold (bytes).
	// When the next AppendBatch would push the segment past this, we rotate first.
	// Defaults to 64 MiB if zero.
	MaxSegmentSize int64

	// MaxSegmentAge: rotate the active segment if it has been open for longer
	// than this, regardless of size. Zero disables.
	MaxSegmentAge time.Duration

	// MaxSegmentRecords: rotate when the active segment has accumulated this
	// many records. Zero disables.
	MaxSegmentRecords uint64

	// PreallocateSegments, when true, reserves MaxSegmentSize bytes on disk at
	// segment-create time (Linux fallocate KEEP_SIZE, macOS F_PREALLOCATE).
	// Disk-full surfaces immediately on segment creation rather than mid-Append,
	// and segments are typically more contiguous on the underlying storage.
	// Costs an extra fcntl per rotation; safe to leave on for production.
	PreallocateSegments bool

	// CompressOnRotate toggles async zstd compression of rotated segments.
	CompressOnRotate bool

	// OnRotate, if non-nil, is invoked from a background goroutine after a
	// segment is successfully renamed to its closed form (and compressed, if
	// enabled). Use it for in-process archiving — uploading to S3, triggering
	// replication, computing checksums. Panics inside the callback are
	// recovered and logged so they can't take down the writer.
	//
	// The callback runs serially per segment; do your own concurrency if you
	// want parallel uploads. Slow callbacks back-pressure rotateWG.Wait at
	// Close time, so consider running the heavy work on your own goroutine
	// and just enqueuing here.
	OnRotate func(SegmentInfo)

	// WriteMetaSidecar toggles writing wal-F-L.meta after rotation. The meta
	// JSON includes record counts, byte size, and a sha256 of the uncompressed
	// content — useful for backup verification and time-based retention.
	WriteMetaSidecar bool

	// SyncMode selects the durability strategy on each commit window. See
	// SyncMode constants for the trade-offs. Defaults to SyncFull.
	SyncMode SyncMode

	// MaxPendingBytes, if > 0, makes Append return ErrBackpressure once the
	// number of bytes written to the bufio buffer but not yet fsynced exceeds
	// this threshold. Useful with GroupCommit on slow disks: prevents the
	// caller from queueing unbounded work while the commit goroutine catches up.
	// 0 = unlimited.
	MaxPendingBytes int

	// BufferSize is the bufio.Writer size for the active segment. Defaults to 64 KiB.
	BufferSize int

	// Logger receives all package-internal log events. Defaults to
	// slog.Default(). Use this to route axwal logs through your central
	// structured-logging pipeline.
	Logger *slog.Logger

	// CloseTimeout caps how long Close() waits for in-flight rotations /
	// compressions to finish. 0 means wait indefinitely (current behavior).
	CloseTimeout time.Duration

	// VerifyOnOpen performs a full CRC walk of every closed segment at
	// OpenWriter time. Reports gaps in the index chain and removes one of any
	// duplicate (wal-F-L vs wal-F-L.zst) pair. Costly — proportional to total
	// WAL size — but catches data damage from out-of-band actors (rsync, FS
	// corruption, manual file shuffling).
	VerifyOnOpen bool

	// GroupCommit, when enabled, batches multiple concurrent Append/AppendBatch
	// calls into a single fsync. Each Append still BLOCKS until its batch has
	// been fsync'd, so durability semantics are unchanged — only throughput
	// improves under concurrent load.
	GroupCommit GroupCommitConfig

	// Retention controls cursor-aware automatic deletion of old segments.
	Retention RetentionConfig
}

// SegmentInfo describes a closed segment delivered to the OnRotate callback
// and used by helper APIs (axwal-cli, archive workflows).
type SegmentInfo struct {
	// Path is the absolute path of the segment file as it exists right now.
	// For a freshly-rotated uncompressed segment this is /…/wal-F-L; if
	// compression is enabled and finished, it's /…/wal-F-L.zst.
	Path string

	// FirstIndex / LastIndex are the closed-form bounds (inclusive).
	FirstIndex uint64
	LastIndex  uint64

	// SizeBytes is the on-disk size at the moment the callback fires.
	SizeBytes int64

	// Compressed reports whether Path ends in ".zst".
	Compressed bool
}

// SyncMode controls how aggressively the writer pushes data to stable storage.
type SyncMode int

const (
	// SyncFull (default) — call *os.File.Sync() after every commit window.
	// On macOS this internally invokes F_FULLFSYNC, which waits for the SSD
	// firmware to flush its DRAM cache; safest, but ~5-15ms per call.
	// On Linux this is fsync(2): waits for data + size metadata to reach disk.
	// Choose for: financial / regulatory systems, anything that must survive
	// hard power loss without losing a single Acked record.
	SyncFull SyncMode = iota

	// SyncBarrier — barrier-only durability.
	//   macOS: F_BARRIERFSYNC (orders writes; doesn't wait for flush-to-platter)
	//   Linux: fdatasync(2)  (skips metadata that doesn't affect read consistency)
	// Records reach the OS page cache and are ordered, but a sudden power loss
	// can lose the last hundreds-of-ms of writes. Throughput often 5-30× higher
	// than SyncFull. Good for: queues backed by UPS / cloud SSDs / replicated
	// storage where the OS surviving the failure is enough.
	SyncBarrier

	// SyncNone — never call any flush; let the kernel decide. The write is
	// visible to other processes on this host immediately, but a hard crash or
	// power loss can lose seconds of data.
	// Good for: metric pipelines, observability event streams, any case where
	// the producer is the source of truth and downstream is reconstructable.
	SyncNone
)

// String returns the SyncMode in human-readable form.
func (m SyncMode) String() string {
	switch m {
	case SyncFull:
		return "full"
	case SyncBarrier:
		return "barrier"
	case SyncNone:
		return "none"
	default:
		return "unknown"
	}
}

// GroupCommitConfig controls batched fsync.
type GroupCommitConfig struct {
	Enabled bool

	// MaxLatency caps how long an Append waits for its batch to flush.
	// Defaults to 1ms.
	MaxLatency time.Duration

	// MaxBatchBytes triggers an early flush when this many unflushed bytes
	// have accumulated. Defaults to 1 MiB.
	MaxBatchBytes int
}

// RetentionConfig controls the janitor goroutine.
type RetentionConfig struct {
	// Enabled gates the janitor. When false, no segments are ever auto-deleted.
	Enabled bool

	// Interval between janitor scans. Defaults to 30s.
	Interval time.Duration

	// MinRetainRecords forces the WAL to keep at least this many of the most recent
	// records on disk regardless of cursor positions. Zero means no extra floor.
	MinRetainRecords uint64

	// StaleReaderTTL: a cursor whose heartbeat is older than this is treated as dead
	// and excluded from the retention computation. Defaults to 60s.
	StaleReaderTTL time.Duration

	// RequireArchivedMark, when true, forbids deleting a segment unless a
	// sidecar file `wal-F-L.archived` exists in the directory. Use this when an
	// external uploader (S3/B2/etc.) is responsible for archiving rotated
	// segments before they can be discarded locally.
	//
	// Mark a segment as archived from the writer process via Writer.MarkArchived,
	// or externally with `axwal-cli mark-archived <segment>`.
	RequireArchivedMark bool

	// MaxRetainAge, when > 0, force-deletes closed segments whose ModTime is
	// older than this duration — independent of cursor positions and
	// MinRetainRecords. Use for compliance-style "purge after N days".
	//
	// CAUTION: this can delete data that no reader has consumed. Pair with a
	// matching consumer SLO (e.g. "all consumers must drain within 6 days" if
	// MaxRetainAge=7d), or readers with lagging cursors will get fast-forwarded
	// and lose records.
	//
	// RequireArchivedMark still applies — even time-aged segments wait for the
	// sidecar before deletion.
	MaxRetainAge time.Duration
}

// WriterStats is a thread-safe snapshot of writer counters/gauges. Cheap to
// call in a metrics-scrape loop (no I/O, brief mutex acquisition).
type WriterStats struct {
	// Gauges (current state).
	LastIndex    uint64
	CommitIndex  uint64
	FirstIndex   uint64
	SegmentBytes int64
	PendingBytes int64

	// Counters (monotonic; reset only on process restart).
	AppendCalls         uint64
	AppendRecords       uint64
	AppendBytes         uint64
	Rotations           uint64
	FsyncCalls          uint64
	FsyncErrors         uint64
	BackpressureRejects uint64
	RetentionDeletes    uint64
}

type writerCounters struct {
	appendCalls       atomic.Uint64
	appendRecords     atomic.Uint64
	appendBytes       atomic.Uint64
	rotations         atomic.Uint64
	fsyncCalls        atomic.Uint64
	fsyncErrors       atomic.Uint64
	backpressure      atomic.Uint64
	retentionDeletes  atomic.Uint64
}

// Writer is the single-process append side of the WAL.
type Writer struct {
	opts WriterOptions
	dir  string
	lock *writerLock
	log  *slog.Logger

	// ctx is canceled by Close so that infinite retry loops in the rotation /
	// compression goroutines exit cleanly. Background work that observes
	// ctx.Err() != nil should give up and return.
	ctx       context.Context
	ctxCancel context.CancelFunc

	cnt writerCounters

	mu         sync.Mutex
	file       *os.File
	buf        *bufio.Writer
	curPath    string
	curSize    int64
	firstIndex uint64
	lastIndex  uint64
	openedAt   time.Time
	curRecords uint64
	closed     bool

	// rotateWG tracks async post-rotation work (rename + compress).
	rotateWG sync.WaitGroup

	janitorStop chan struct{}
	janitorWG   sync.WaitGroup

	// Group-commit state. Used iff opts.GroupCommit.Enabled.
	gcCond         *sync.Cond // signals on commitIndex advance; tied to mu
	commitIndex    uint64     // highest index whose batch has been fsync'd
	pendingBytes   int        // bytes written to bufio but not yet fsync'd
	gcKick         chan struct{}
	gcStop         chan struct{}
	gcWG           sync.WaitGroup
	rotateAgeStop  chan struct{}
	rotateAgeWG    sync.WaitGroup
}

// OpenWriter opens (or creates) a WAL for writing. Runs recovery first.
// Returns an error if another process holds the writer lock.
func OpenWriter(opts WriterOptions) (*Writer, error) {
	if opts.Dir == "" {
		return nil, errors.New("axwal: WriterOptions.Dir is required")
	}
	if opts.MaxSegmentSize <= 0 {
		opts.MaxSegmentSize = 64 * 1024 * 1024
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = 64 * 1024
	}
	if opts.Retention.Enabled {
		if opts.Retention.Interval <= 0 {
			opts.Retention.Interval = 30 * time.Second
		}
		if opts.Retention.StaleReaderTTL <= 0 {
			opts.Retention.StaleReaderTTL = 60 * time.Second
		}
	}
	if opts.GroupCommit.Enabled {
		if opts.GroupCommit.MaxLatency <= 0 {
			opts.GroupCommit.MaxLatency = time.Millisecond
		}
		if opts.GroupCommit.MaxBatchBytes <= 0 {
			opts.GroupCommit.MaxBatchBytes = 1 << 20
		}
	}

	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	logger := opts.Logger.With("component", "axwal.writer", "dir", opts.Dir)

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir wal dir: %w", err)
	}

	lock, err := acquireWriterLock(opts.Dir)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	w := &Writer{
		opts:      opts,
		dir:       opts.Dir,
		lock:      lock,
		log:       logger,
		ctx:       ctx,
		ctxCancel: cancel,
	}

	last, err := recover_(opts.Dir, opts.CompressOnRotate, logger)
	if err != nil {
		_ = lock.Release()
		return nil, fmt.Errorf("recovery: %w", err)
	}
	w.lastIndex = last

	if opts.VerifyOnOpen {
		if err := verifyChain(opts.Dir, logger); err != nil {
			_ = lock.Release()
			return nil, fmt.Errorf("verify chain: %w", err)
		}
	}

	if err := w.openNewSegment(); err != nil {
		_ = lock.Release()
		return nil, fmt.Errorf("open initial segment: %w", err)
	}

	if opts.Retention.Enabled {
		w.janitorStop = make(chan struct{})
		w.janitorWG.Add(1)
		go w.janitorLoop()
	}

	if opts.GroupCommit.Enabled {
		w.gcCond = sync.NewCond(&w.mu)
		w.gcKick = make(chan struct{}, 1)
		w.gcStop = make(chan struct{})
		w.gcWG.Add(1)
		go w.commitLoop()
	}

	if opts.MaxSegmentAge > 0 {
		w.rotateAgeStop = make(chan struct{})
		w.rotateAgeWG.Add(1)
		go w.rotateAgeLoop()
	}

	return w, nil
}

// LastIndex returns the highest index that has been Appended (and fsynced) so far.
func (w *Writer) LastIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastIndex
}

// Stats returns a thread-safe snapshot of writer counters and gauges suitable
// for periodic scraping by a metrics system.
func (w *Writer) Stats() WriterStats {
	w.mu.Lock()
	s := WriterStats{
		LastIndex:    w.lastIndex,
		CommitIndex:  w.commitIndex,
		FirstIndex:   w.firstIndex,
		SegmentBytes: w.curSize,
		PendingBytes: int64(w.pendingBytes),
	}
	w.mu.Unlock()
	s.AppendCalls = w.cnt.appendCalls.Load()
	s.AppendRecords = w.cnt.appendRecords.Load()
	s.AppendBytes = w.cnt.appendBytes.Load()
	s.Rotations = w.cnt.rotations.Load()
	s.FsyncCalls = w.cnt.fsyncCalls.Load()
	s.FsyncErrors = w.cnt.fsyncErrors.Load()
	s.BackpressureRejects = w.cnt.backpressure.Load()
	s.RetentionDeletes = w.cnt.retentionDeletes.Load()
	return s
}

// NextIndex returns the index that the next Append will assign.
func (w *Writer) NextIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastIndex + 1
}

// Append writes a single record and fsyncs. Equivalent to
// AppendContext(context.Background(), payload).
func (w *Writer) Append(payload []byte) (uint64, error) {
	return w.AppendContext(context.Background(), payload)
}

// AppendContext writes a single record and waits for durability, respecting
// ctx cancellation while waiting on the group-commit window. Returns
// ctx.Err() if the deadline elapses before the batch is fsynced.
func (w *Writer) AppendContext(ctx context.Context, payload []byte) (uint64, error) {
	idxs, err := w.AppendBatchContext(ctx, [][]byte{payload})
	if err != nil {
		return 0, err
	}
	return idxs[0], nil
}

// AppendBatch writes multiple records atomically. Equivalent to
// AppendBatchContext(context.Background(), records).
func (w *Writer) AppendBatch(records [][]byte) ([]uint64, error) {
	return w.AppendBatchContext(context.Background(), records)
}

// AppendBatchContext writes multiple records atomically and returns the
// assigned indices. By default each call ends with an fsync; with GroupCommit
// enabled the fsync is shared across concurrent appenders, but each call still
// blocks until its records are durable. ctx cancellation aborts the group-
// commit wait (the records may still be on disk after the next commit window
// — but the caller is told nothing about whether they were committed).
func (w *Writer) AppendBatchContext(ctx context.Context, records [][]byte) ([]uint64, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, errors.New("axwal: writer closed")
	}

	frames := make([][]byte, len(records))
	var totalSize int64
	for i, r := range records {
		f, err := encodeRecord(r)
		if err != nil {
			return nil, err
		}
		frames[i] = f
		totalSize += int64(len(f))
	}

	// Backpressure: refuse the write before we've appended anything if it
	// would push pendingBytes past the configured ceiling. Returning an error
	// lets producers shed load instead of growing unbounded buffers.
	if w.opts.MaxPendingBytes > 0 && w.pendingBytes+int(totalSize) > w.opts.MaxPendingBytes {
		w.cnt.backpressure.Add(1)
		return nil, fmt.Errorf("%w: pending=%d would-add=%d limit=%d",
			ErrBackpressure, w.pendingBytes, totalSize, w.opts.MaxPendingBytes)
	}

	if w.shouldRotateBeforeWriteLocked(totalSize, uint64(len(records))) {
		if err := w.rotateLocked(); err != nil {
			return nil, fmt.Errorf("rotate: %w", err)
		}
	}

	idxs := make([]uint64, len(records))
	for i, f := range frames {
		if _, err := w.buf.Write(f); err != nil {
			return nil, fmt.Errorf("buffer write: %w", err)
		}
		w.lastIndex++
		w.curSize += int64(len(f))
		w.curRecords++
		w.pendingBytes += len(f)
		idxs[i] = w.lastIndex
	}
	myMaxIdx := idxs[len(idxs)-1]
	w.cnt.appendCalls.Add(1)
	w.cnt.appendRecords.Add(uint64(len(records)))
	w.cnt.appendBytes.Add(uint64(totalSize))

	if w.opts.GroupCommit.Enabled {
		// Kick the commit goroutine if our batch alone exceeds the bytes
		// threshold; otherwise the timer will pick it up.
		if w.pendingBytes >= w.opts.GroupCommit.MaxBatchBytes {
			w.kickCommit()
		}

		// ctx-aware wait: if the caller's ctx is cancellable, spawn a watcher
		// that broadcasts on the cond so the Wait() unblocks.
		var stopWatch chan struct{}
		if ctx.Done() != nil {
			stopWatch = make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
					w.mu.Lock()
					w.gcCond.Broadcast()
					w.mu.Unlock()
				case <-stopWatch:
				}
			}()
		}

		for {
			if w.closed {
				if stopWatch != nil {
					close(stopWatch)
				}
				return nil, errors.New("axwal: writer closed during commit")
			}
			if err := ctx.Err(); err != nil {
				if stopWatch != nil {
					close(stopWatch)
				}
				return nil, err
			}
			if w.commitIndex >= myMaxIdx {
				break
			}
			w.gcCond.Wait()
		}
		if stopWatch != nil {
			close(stopWatch)
		}
		return idxs, nil
	}

	// Default path: synchronous fsync.
	if err := w.flushAndSyncLocked(); err != nil {
		return nil, err
	}
	w.commitIndex = w.lastIndex
	return idxs, nil
}

// shouldRotateBeforeWriteLocked returns true if appending `addBytes` more bytes
// (or `addRecords` more records) would push the active segment past any
// configured rotation trigger.
func (w *Writer) shouldRotateBeforeWriteLocked(addBytes int64, addRecords uint64) bool {
	if w.curSize == 0 {
		return false
	}
	if w.opts.MaxSegmentSize > 0 && w.curSize+addBytes > w.opts.MaxSegmentSize {
		return true
	}
	if w.opts.MaxSegmentRecords > 0 && w.curRecords+addRecords > w.opts.MaxSegmentRecords {
		return true
	}
	return false
}

// flushAndSyncLocked performs Flush+Sync and resets pendingBytes. Caller holds w.mu.
func (w *Writer) flushAndSyncLocked() error {
	if w.buf == nil || w.file == nil {
		return nil
	}
	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	if err := w.syncFile(w.file); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	w.pendingBytes = 0
	return nil
}

// syncFile applies the configured SyncMode and bumps fsync counters.
func (w *Writer) syncFile(f *os.File) error {
	if w.opts.SyncMode == SyncNone {
		return nil
	}
	w.cnt.fsyncCalls.Add(1)
	var err error
	switch w.opts.SyncMode {
	case SyncBarrier:
		err = syncBarrier(f)
	default: // SyncFull
		err = f.Sync()
	}
	if err != nil {
		w.cnt.fsyncErrors.Add(1)
	}
	return err
}

func (w *Writer) kickCommit() {
	select {
	case w.gcKick <- struct{}{}:
	default:
	}
}

// Rotate forces a rotation (close current segment, open a new one).
func (w *Writer) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("axwal: writer closed")
	}
	return w.rotateLocked()
}

func (w *Writer) rotateLocked() error {
	if w.file == nil {
		return w.openNewSegment()
	}

	// Rotating an empty segment is a no-op. If we did rotate it, we'd spawn an
	// async rename of "wal-F-" -> "wal-F-(F-1)" (the empty marker) while
	// openNewSegment immediately recreates a new file at "wal-F-" — same path
	// because firstIndex = lastIndex+1 = F. The two operations race for the
	// directory entry and can corrupt subsequent appends. Skipping the rotate
	// here is the only safe behavior; callers calling Rotate() on an empty
	// segment expressed an intent that is satisfied by "the segment will roll
	// once you actually write to it".
	if w.curRecords == 0 {
		return nil
	}

	if err := w.flushAndSyncLocked(); err != nil {
		return err
	}
	w.cnt.rotations.Add(1)

	// Rotation implicitly commits everything written so far.
	w.commitIndex = w.lastIndex
	if w.gcCond != nil {
		w.gcCond.Broadcast()
	}

	rotatedPath := w.curPath
	rotatedFirst := w.firstIndex
	rotatedLast := w.lastIndex

	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil
	w.buf = nil

	w.rotateWG.Add(1)
	go w.finalizeRotated(rotatedPath, rotatedFirst, rotatedLast)

	return w.openNewSegment()
}

func (w *Writer) openNewSegment() error {
	first := w.lastIndex + 1
	name := formatOpenName(first)
	path := filepath.Join(w.dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	if w.opts.PreallocateSegments && w.opts.MaxSegmentSize > 0 {
		if err := preallocate(f, w.opts.MaxSegmentSize); err != nil {
			// Best-effort: log and continue without pre-alloc rather than fail
			// the whole open. Disk-full will surface on the first Append's
			// fsync if it's a real problem.
			w.log.Warn("preallocate failed (continuing without)", "name", name, "err", err)
		}
	}
	// fsync the directory so the new file entry is durable.
	if err := fsyncDir(w.dir); err != nil {
		f.Close()
		return err
	}
	w.file = f
	w.curPath = path
	w.firstIndex = first
	w.curSize = 0
	w.curRecords = 0
	w.openedAt = time.Now()
	w.buf = bufio.NewWriterSize(f, w.opts.BufferSize)
	return nil
}

// Close finalizes the current segment (rename to wal-F-L), waits for any
// in-flight rotations, stops the janitor and group-commit goroutines, and
// releases the lock.
func (w *Writer) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true

	var firstErr error
	if w.file != nil {
		if err := w.buf.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
		// On clean close we ALWAYS do a full Sync, regardless of SyncMode.
		// The user opted into SyncBarrier/SyncNone for steady-state throughput;
		// on shutdown we want the segment durably committed.
		if err := w.file.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		w.commitIndex = w.lastIndex
		if err := w.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}

		// If the current segment has any records, rename it to closed form;
		// otherwise remove the empty placeholder.
		if w.lastIndex >= w.firstIndex {
			final := filepath.Join(w.dir, formatClosedName(w.firstIndex, w.lastIndex))
			if err := os.Rename(w.curPath, final); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("rename on close: %w", err)
			}
		} else {
			if err := os.Remove(w.curPath); err != nil && !os.IsNotExist(err) && firstErr == nil {
				firstErr = err
			}
		}
		w.file = nil
	}

	// Wake any group-commit waiters so they bail out with "writer closed".
	if w.gcCond != nil {
		w.gcCond.Broadcast()
	}

	janitorStop := w.janitorStop
	gcStop := w.gcStop
	rotAgeStop := w.rotateAgeStop
	w.mu.Unlock()

	if janitorStop != nil {
		close(janitorStop)
		w.janitorWG.Wait()
	}
	if gcStop != nil {
		close(gcStop)
		w.gcWG.Wait()
	}
	if rotAgeStop != nil {
		close(rotAgeStop)
		w.rotateAgeWG.Wait()
	}

	// Cancel the writer ctx so any retry loops in finalizeRotated /
	// robustCompressZstd give up. Without this they'd loop forever on a stuck
	// disk and rotateWG.Wait() would hang.
	w.ctxCancel()

	timedOut := false
	if w.opts.CloseTimeout > 0 {
		done := make(chan struct{})
		go func() { w.rotateWG.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(w.opts.CloseTimeout):
			timedOut = true
			w.log.Warn("close timeout: abandoning in-flight rotations",
				"timeout", w.opts.CloseTimeout)
		}
	} else {
		w.rotateWG.Wait()
	}

	if err := w.lock.Release(); err != nil && firstErr == nil {
		firstErr = err
	}
	if timedOut && firstErr == nil {
		firstErr = ErrCloseTimeout
	}
	return firstErr
}

// TruncatePrefix synchronously deletes all closed segments whose LastIndex is
// strictly below uptoIndex, plus their .archived/.meta sidecars. The active
// open segment is never touched.
//
// Use this for explicit checkpoint-style truncation: "I've persisted everything
// up to N elsewhere; drop the local prefix." It bypasses cursor protection and
// MinRetainRecords, so call it only when you know no consumer will need the
// truncated range again. Live readers whose cursors fall below the new
// earliest-available will be fast-forwarded on their next OpenReader.
//
// Returns the number of segments removed.
func (w *Writer) TruncatePrefix(uptoIndex uint64) (int, error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return 0, errors.New("axwal: writer closed")
	}
	curPath := w.curPath
	w.mu.Unlock()

	matches, err := filepath.Glob(filepath.Join(w.dir, "wal-*"))
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, p := range matches {
		base := filepath.Base(p)
		if base == writerLockFile {
			continue
		}
		s, err := parseSegName(base)
		if err != nil || !s.Closed {
			continue
		}
		if p == curPath {
			continue
		}
		if s.LastIndex >= uptoIndex {
			continue
		}
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			w.log.Error("truncate-prefix: remove failed", "name", base, "err", err)
			continue
		}
		_ = removeArchivedSidecar(w.dir, s)
		_ = removeMetaSidecar(w.dir, s.FirstIndex, s.LastIndex)
		w.cnt.retentionDeletes.Add(1)
		w.log.Info("truncate-prefix: deleted segment",
			"name", base, "last_index", s.LastIndex, "upto", uptoIndex)
		removed++
	}
	if err := fsyncDir(w.dir); err != nil {
		return removed, err
	}
	return removed, nil
}

// Sync forces a full fsync of the current segment file. Use this as an
// explicit checkpoint when SyncMode is Barrier or None — it bypasses the
// configured mode and guarantees a hard flush.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	if w.buf != nil {
		if err := w.buf.Flush(); err != nil {
			return err
		}
	}
	return w.file.Sync()
}

// fsyncDir fsyncs a directory so that newly created/renamed entries are durable.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
