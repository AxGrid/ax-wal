package axwal

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

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

	// CompressOnRotate toggles async zstd compression of rotated segments.
	CompressOnRotate bool

	// SyncMode selects the durability strategy on each commit window. See
	// SyncMode constants for the trade-offs. Defaults to SyncFull.
	SyncMode SyncMode

	// BufferSize is the bufio.Writer size for the active segment. Defaults to 64 KiB.
	BufferSize int

	// GroupCommit, when enabled, batches multiple concurrent Append/AppendBatch
	// calls into a single fsync. Each Append still BLOCKS until its batch has
	// been fsync'd, so durability semantics are unchanged — only throughput
	// improves under concurrent load.
	GroupCommit GroupCommitConfig

	// Retention controls cursor-aware automatic deletion of old segments.
	Retention RetentionConfig
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
}

// Writer is the single-process append side of the WAL.
type Writer struct {
	opts WriterOptions
	dir  string
	lock *writerLock

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

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir wal dir: %w", err)
	}

	lock, err := acquireWriterLock(opts.Dir)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		opts: opts,
		dir:  opts.Dir,
		lock: lock,
	}

	last, err := recover_(opts.Dir, opts.CompressOnRotate)
	if err != nil {
		_ = lock.Release()
		return nil, fmt.Errorf("recovery: %w", err)
	}
	w.lastIndex = last

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

// NextIndex returns the index that the next Append will assign.
func (w *Writer) NextIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastIndex + 1
}

// Append writes a single record and fsyncs.
func (w *Writer) Append(payload []byte) (uint64, error) {
	idxs, err := w.AppendBatch([][]byte{payload})
	if err != nil {
		return 0, err
	}
	return idxs[0], nil
}

// AppendBatch writes multiple records atomically and returns the assigned
// indices in order. By default each call ends with an fsync; with GroupCommit
// enabled the fsync is shared across concurrent appenders, but each call still
// blocks until its records are durable.
func (w *Writer) AppendBatch(records [][]byte) ([]uint64, error) {
	if len(records) == 0 {
		return nil, nil
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

	if w.opts.GroupCommit.Enabled {
		// Kick the commit goroutine if our batch alone exceeds the bytes
		// threshold; otherwise the timer will pick it up.
		if w.pendingBytes >= w.opts.GroupCommit.MaxBatchBytes {
			w.kickCommit()
		}
		// Wait until our batch is durable.
		for w.commitIndex < myMaxIdx && !w.closed {
			w.gcCond.Wait()
		}
		if w.closed {
			return nil, errors.New("axwal: writer closed during commit")
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

// syncFile applies the configured SyncMode.
func (w *Writer) syncFile(f *os.File) error {
	switch w.opts.SyncMode {
	case SyncNone:
		return nil
	case SyncBarrier:
		return syncBarrier(f)
	default: // SyncFull
		return f.Sync()
	}
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

	if err := w.flushAndSyncLocked(); err != nil {
		return err
	}

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

	w.rotateWG.Wait()

	if err := w.lock.Release(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
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
