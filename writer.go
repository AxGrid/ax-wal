package axwal

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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

// ErrDiskFull is returned by Append/AppendBatch when the WAL directory is over
// Retention.MaxTotalBytes (or the filesystem is under Retention.MinFreeBytes),
// the janitor found nothing it is allowed to delete, and
// Retention.OnDiskFull is DiskFullReject.
var ErrDiskFull = errors.New("axwal: disk limit reached and no segment can be deleted")

// ErrQueueFull is returned by AppendAsync when the async queue is at capacity;
// the record was dropped (and counted in WriterStats.Dropped).
var ErrQueueFull = errors.New("axwal: async queue full, record dropped")

// ErrAsyncDisabled is returned by AppendAsync when WriterOptions.AsyncQueueSize
// is zero.
var ErrAsyncDisabled = errors.New("axwal: AppendAsync requires WriterOptions.AsyncQueueSize > 0")

// ErrWriterClosed is returned by operations on a closed Writer.
var ErrWriterClosed = errors.New("axwal: writer closed")

// closeFinalizeGrace bounds how long Close lets the rotation pipeline of the
// last segment run (compression, .meta, OnRotate) when CloseTimeout is unset.
const closeFinalizeGrace = 30 * time.Second

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

	// RotateAligned, when > 0, rotates on wall-clock boundaries that are
	// multiples of this duration: time.Hour = every hour at :00,
	// 24*time.Hour = once a day at 00:00. A segment then only ever holds
	// records appended inside one such window, so an archive laid out by day
	// matches the content rather than the moment the file happened to close.
	//
	// Durations that divide 24h evenly are aligned to midnight in
	// RotateLocation; anything else is aligned to the Unix epoch. Independent
	// of MaxSegmentAge (which counts from segment open) — both may be set.
	RotateAligned time.Duration

	// RotateLocation is the time zone for RotateAligned. Defaults to UTC.
	RotateLocation *time.Location

	// RotateOnClose makes Close() treat the last segment like a rotation:
	// it is compressed (if CompressOnRotate), gets its .meta sidecar and is
	// handed to OnRotate / the archiver before Close returns — bounded by
	// CloseTimeout, or 30s when CloseTimeout is zero. Without it Close only
	// renames the segment to its closed form. Implied by Archive.
	RotateOnClose bool

	// Archive, if non-nil, enables the built-in archiver: every closed
	// segment is uploaded to Archive.Store and marked .archived once the
	// upload is verified. See ArchiveConfig. Implies WriteMetaSidecar,
	// RotateOnClose and Retention.RequireArchivedMark.
	Archive *ArchiveConfig

	// SyncReplication, if non-nil, makes appends wait for a replica's
	// acknowledgement (semi-synchronous replication) with automatic
	// fallback to asynchronous mode. See SyncReplicationConfig.
	SyncReplication *SyncReplicationConfig

	// AsyncQueueSize, when > 0, enables AppendAsync: a bounded in-memory
	// queue of this many records drained by a background goroutine.
	AsyncQueueSize int

	// AsyncQueueBytes optionally also bounds the async queue by total payload
	// bytes. 0 = no byte limit.
	AsyncQueueBytes int64

	// AsyncBatchSize is the maximum number of queued records written per
	// AppendBatch by the async drainer. Defaults to 256.
	AsyncBatchSize int

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

	// Encryption, if non-nil, seals every record's payload with AES-256-GCM
	// before it hits disk. Readers must use the same key. See EncryptionConfig.
	Encryption *EncryptionConfig

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

	// nowFn overrides the clock in tests.
	nowFn func() time.Time

	// replica is set by RunReplica: the only writer allowed into a directory
	// that carries the replica marker.
	replica bool
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

	// MinRetainAge, when > 0, keeps every closed segment on local disk for at
	// least this long (by file ModTime), even when no reader needs it. The
	// full rule: a segment is deleted when it is archived (if an archive mark
	// is required) AND older than MinRetainAge AND not needed by live readers
	// or MinRetainRecords. MaxRetainAge stays the hard ceiling. Disk-pressure
	// eviction (MaxTotalBytes / MinFreeBytes) overrides MinRetainAge.
	MinRetainAge time.Duration

	// MaxTotalBytes, when > 0, caps the total size of segment files in the
	// WAL directory. When exceeded the janitor deletes the oldest closed
	// segments first — ignoring MinRetainAge and MinRetainRecords, but never
	// a segment that is not archived yet (when an archive mark is required)
	// or that a live reader still needs. The limit is enforced per janitor
	// pass and on every rotation, so leave headroom of about one segment.
	MaxTotalBytes int64

	// MinFreeBytes, when > 0, triggers the same eviction when the filesystem
	// holding the WAL has less free space than this (Unix only).
	MinFreeBytes int64

	// OnDiskFull selects what Append does while the directory is over the
	// limit and nothing can be deleted (archive unreachable, disk full).
	OnDiskFull DiskFullPolicy
}

// DiskFullPolicy is the writer's behavior while a disk limit is exceeded and
// the janitor has nothing it may delete.
type DiskFullPolicy int

const (
	// DiskFullReject (default): Append returns ErrDiskFull; counted in
	// WriterStats.DiskFullRejects.
	DiskFullReject DiskFullPolicy = iota
	// DiskFullDrop: Append discards the records and returns (nil indices,
	// nil error); counted in WriterStats.Dropped. For telemetry-style logs
	// where losing records beats failing the caller.
	DiskFullDrop
	// DiskFullIgnore: keep writing past the limit (eviction still runs).
	DiskFullIgnore
)

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

	// Dropped counts records that were discarded instead of written: async
	// queue overflow, async write failures and DiskFullDrop.
	Dropped uint64
	// AsyncErrors counts failed background writes of AppendAsync batches.
	AsyncErrors uint64
	// AsyncQueued is the number of records currently waiting in the async queue.
	AsyncQueued int
	// DiskFullRejects counts Append calls refused with ErrDiskFull.
	DiskFullRejects uint64
	// DiskFull reports whether a disk limit is currently exceeded with
	// nothing left to delete.
	DiskFull bool

	// Replication, as reported by a Serve running with ServerOptions.Writer.
	//
	// ReplicasConnected: replica sessions currently attached.
	// ReplicaAckedIndex: highest index a replica has confirmed on its disk;
	// LastIndex - ReplicaAckedIndex is the replication lag in records.
	// SyncReplicationActive: appends are currently waiting for the replica
	// (false = asynchronous, either by configuration or after a fallback).
	// SyncReplicationFallbacks: how many times sync mode was dropped.
	ReplicasConnected        int
	ReplicaAckedIndex        uint64
	SyncReplicationActive    bool
	SyncReplicationFallbacks uint64
	SyncReplicationWaits     uint64

	// Archive gauges/counters (zero unless WriterOptions.Archive is set).
	//
	// ArchivePending / ArchivePendingBytes: closed segments not yet uploaded.
	// ArchiveOldestPendingAge: age of the oldest of them (0 if none).
	// ArchiveLastSuccess: time of the last verified upload (zero if none yet).
	ArchivePending          int
	ArchivePendingBytes     int64
	ArchiveOldestPendingAge time.Duration
	ArchiveLastSuccess      time.Time
	ArchiveUploads          uint64
	ArchiveErrors           uint64
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
	dropped           atomic.Uint64
	asyncErrors       atomic.Uint64
	diskFullRejects   atomic.Uint64
}

// Writer is the single-process append side of the WAL.
type Writer struct {
	opts   WriterOptions
	dir    string
	lock   *writerLock
	log    *slog.Logger
	cipher *cipherAEAD // non-nil iff opts.Encryption is set

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
	// segFirstAt / segLastAt: append time of the first / last record of the
	// active segment. Valid iff curRecords > 0.
	segFirstAt time.Time
	segLastAt  time.Time

	// finalizing is the set of closed segments still inside the rotation
	// pipeline (rename → compress → meta → OnRotate).
	finMu      sync.Mutex
	finalizing map[segRange]struct{}

	arch *archiver // non-nil iff opts.Archive is set
	repl *replState

	// diskFull is set by the janitor while a disk limit is exceeded and
	// nothing can be deleted.
	diskFull    atomic.Bool
	janitorKick chan struct{}

	// Async append state. Used iff opts.AsyncQueueSize > 0.
	asyncMu     sync.RWMutex // guards asyncClosed vs. sends on asyncCh
	asyncCh     chan []byte
	asyncClosed bool
	asyncBytes  atomic.Int64
	asyncWG     sync.WaitGroup
	asyncOnce   sync.Once

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

	if opts.RotateAligned < 0 {
		return nil, errors.New("axwal: WriterOptions.RotateAligned must be >= 0")
	}
	if opts.RotateLocation == nil {
		opts.RotateLocation = time.UTC
	}
	if opts.nowFn == nil {
		opts.nowFn = time.Now
	}
	if opts.AsyncQueueSize > 0 && opts.AsyncBatchSize <= 0 {
		opts.AsyncBatchSize = 256
	}
	if opts.Archive != nil {
		cfg, err := opts.Archive.withDefaults()
		if err != nil {
			return nil, err
		}
		opts.Archive = &cfg
		// The archiver needs the sidecar for record times / key fingerprint,
		// a properly finished last segment, and retention must wait for it.
		opts.WriteMetaSidecar = true
		opts.RotateOnClose = true
		opts.Retention.RequireArchivedMark = true
	}

	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	logger := opts.Logger.With("component", "axwal.writer", "dir", opts.Dir)

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir wal dir: %w", err)
	}

	if IsReplica(opts.Dir) && !opts.replica {
		return nil, fmt.Errorf("%w: %s", ErrReplicaDir, opts.Dir)
	}

	lock, err := acquireWriterLock(opts.Dir)
	if err != nil {
		return nil, err
	}

	var cipherInst *cipherAEAD
	if opts.Encryption != nil {
		c, err := newCipher(opts.Encryption.Key)
		if err != nil {
			_ = lock.Release()
			return nil, err
		}
		cipherInst = c
	}

	ctx, cancel := context.WithCancel(context.Background())
	w := &Writer{
		opts:      opts,
		dir:       opts.Dir,
		lock:      lock,
		log:       logger,
		cipher:    cipherInst,
		ctx:       ctx,
		ctxCancel: cancel,
		repl:      newReplState(opts.SyncReplication, logger),
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
		w.janitorKick = make(chan struct{}, 1)
		if opts.Retention.MaxTotalBytes > 0 || opts.Retention.MinFreeBytes > 0 {
			// Know right away whether we start over the limit.
			w.kickJanitor()
		}
		w.janitorWG.Add(1)
		go w.janitorLoop()
	}

	if opts.Archive != nil {
		w.arch = newArchiver(w, *opts.Archive)
		w.arch.start()
	}

	if opts.AsyncQueueSize > 0 {
		w.asyncCh = make(chan []byte, opts.AsyncQueueSize)
		w.asyncWG.Add(1)
		go w.asyncLoop()
	}

	if opts.GroupCommit.Enabled {
		w.gcCond = sync.NewCond(&w.mu)
		w.gcKick = make(chan struct{}, 1)
		w.gcStop = make(chan struct{})
		w.gcWG.Add(1)
		go w.commitLoop()
	}

	if opts.MaxSegmentAge > 0 || opts.RotateAligned > 0 {
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
	s.Dropped = w.cnt.dropped.Load()
	s.AsyncErrors = w.cnt.asyncErrors.Load()
	s.AsyncQueued = len(w.asyncCh)
	s.DiskFullRejects = w.cnt.diskFullRejects.Load()
	s.DiskFull = w.diskFull.Load()
	if w.arch != nil {
		w.arch.fillStats(&s)
	}
	w.repl.fillStats(&s)
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
	if len(idxs) == 0 {
		return 0, nil // dropped by DiskFullDrop
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
//
// With SyncReplication the call additionally waits — after local durability,
// without holding the writer lock — until a replica has the records, unless
// the writer is in (or falls back to) asynchronous mode.
func (w *Writer) AppendBatchContext(ctx context.Context, records [][]byte) ([]uint64, error) {
	idxs, err := w.appendBatchLocal(ctx, records)
	if err != nil || len(idxs) == 0 {
		return idxs, err
	}
	last := idxs[len(idxs)-1]
	for {
		cur := w.repl.last.Load()
		if cur >= last || w.repl.last.CompareAndSwap(cur, last) {
			break
		}
	}
	w.repl.wait(ctx, last)
	return idxs, nil
}

// appendBatchLocal writes records and makes them durable on local disk.
func (w *Writer) appendBatchLocal(ctx context.Context, records [][]byte) ([]uint64, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, ErrWriterClosed
	}

	if w.diskFull.Load() {
		switch w.opts.Retention.OnDiskFull {
		case DiskFullDrop:
			w.cnt.dropped.Add(uint64(len(records)))
			return nil, nil
		case DiskFullReject:
			w.cnt.diskFullRejects.Add(1)
			return nil, ErrDiskFull
		}
	}

	// If encryption is enabled, seal every payload now. We materialize a new
	// slice because the original `records` are user-owned and we don't want
	// to mutate them; the sealed slice goes through the rest of the path
	// transparently.
	if w.cipher != nil {
		sealed := make([][]byte, len(records))
		for i, r := range records {
			ct, err := w.cipher.seal(r)
			if err != nil {
				return nil, err
			}
			sealed[i] = ct
		}
		records = sealed
	}

	// Compute total frame bytes without allocating per-record buffers; we'll
	// stream each record into bufio directly below.
	var totalSize int64
	for _, r := range records {
		if len(r) > maxRecordSize {
			return nil, fmt.Errorf("%w: record %d exceeds %d", ErrRecordTooLarge, len(r), maxRecordSize)
		}
		totalSize += int64(headerSize + len(r))
	}

	// Backpressure: refuse the write before we've appended anything if it
	// would push pendingBytes past the configured ceiling. Returning an error
	// lets producers shed load instead of growing unbounded buffers.
	if w.opts.MaxPendingBytes > 0 && w.pendingBytes+int(totalSize) > w.opts.MaxPendingBytes {
		w.cnt.backpressure.Add(1)
		return nil, fmt.Errorf("%w: pending=%d would-add=%d limit=%d",
			ErrBackpressure, w.pendingBytes, totalSize, w.opts.MaxPendingBytes)
	}

	now := w.opts.nowFn()
	if w.shouldRotateBeforeWriteLocked(totalSize, uint64(len(records))) || w.crossedBoundaryLocked(now) {
		if err := w.rotateLocked(); err != nil {
			return nil, fmt.Errorf("rotate: %w", err)
		}
	}
	if w.curRecords == 0 {
		w.segFirstAt = now
	}
	w.segLastAt = now

	idxs := make([]uint64, len(records))
	for i, r := range records {
		if err := w.writeFrameLocked(r); err != nil {
			return nil, err
		}
		w.lastIndex++
		frameSize := int64(headerSize + len(r))
		w.curSize += frameSize
		w.curRecords++
		w.pendingBytes += int(frameSize)
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

// writeFrameLocked emits a complete frame [u32 len | u32 crc32c | payload]
// directly to the bufio buffer without allocating any intermediate slice.
// Caller holds w.mu.
func (w *Writer) writeFrameLocked(payload []byte) error {
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, crcTable))
	if _, err := w.buf.Write(hdr[:]); err != nil {
		return fmt.Errorf("buffer write header: %w", err)
	}
	if _, err := w.buf.Write(payload); err != nil {
		return fmt.Errorf("buffer write payload: %w", err)
	}
	return nil
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
		return ErrWriterClosed
	}
	return w.rotateLocked()
}

// RotateKey switches the writer to a new 32-byte encryption key without
// stopping it. The active segment is rotated first, so every segment is
// sealed with exactly one key: records appended before the call use the old
// key, records after it the new one. Readers need the new key as
// EncryptionConfig.Key and the previous ones in OldKeys.
//
// Returns an error if the writer was opened without Encryption.
func (w *Writer) RotateKey(newKey []byte) error {
	c, err := newCipher(newKey)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWriterClosed
	}
	if w.cipher == nil {
		return fmt.Errorf("%w: RotateKey on a writer opened without Encryption", ErrEncryption)
	}
	if err := w.rotateLocked(); err != nil {
		return fmt.Errorf("rotate: %w", err)
	}
	w.cipher = c
	return nil
}

// keyFingerprintLocked returns the fingerprint of the key sealing the active
// segment ("" when encryption is off). Caller holds w.mu.
func (w *Writer) keyFingerprintLocked() string {
	if w.cipher == nil {
		return ""
	}
	return w.cipher.fp
}

// crossedBoundaryLocked reports whether `now` falls into a later RotateAligned
// window than the first record of the active segment. Caller holds w.mu.
func (w *Writer) crossedBoundaryLocked(now time.Time) bool {
	if w.opts.RotateAligned <= 0 || w.curRecords == 0 {
		return false
	}
	d, loc := w.opts.RotateAligned, w.opts.RotateLocation
	return alignedWindowStart(now, d, loc).After(alignedWindowStart(w.segFirstAt, d, loc))
}

// alignedWindowStart returns the start of the RotateAligned window containing t.
func alignedWindowStart(t time.Time, d time.Duration, loc *time.Location) time.Time {
	const day = 24 * time.Hour
	if d > day || day%d != 0 {
		return t.Truncate(d) // aligned to the Unix epoch
	}
	t = t.In(loc)
	midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
	return midnight.Add(t.Sub(midnight).Truncate(d))
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
	rs := rotatedSeg{
		first:   w.firstIndex,
		last:    w.lastIndex,
		firstAt: w.segFirstAt,
		lastAt:  w.segLastAt,
		keyFP:   w.keyFingerprintLocked(),
	}

	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil
	w.buf = nil

	w.markFinalizing(rs)
	w.rotateWG.Add(1)
	go w.finalizeRotated(rotatedPath, rs)

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
	w.openedAt = w.opts.nowFn()
	w.buf = bufio.NewWriterSize(f, w.opts.BufferSize)
	return nil
}

// Close finalizes the current segment (rename to wal-F-L), waits for any
// in-flight rotations, stops the janitor and group-commit goroutines, and
// releases the lock.
//
// Records still sitting in the AppendAsync queue are written first. With
// RotateOnClose the last segment also goes through the full rotation pipeline,
// and with Archive.DrainOnClose the archiver gets that long to upload what is
// still pending. CloseTimeout caps the whole sequence.
func (w *Writer) Close() error {
	// Queued async records still need a live writer to land in.
	w.stopAsync()

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true

	var (
		firstErr error
		tail     *rotatedSeg
		tailPath string
	)
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
			if err := os.Rename(w.curPath, final); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("rename on close: %w", err)
				}
			} else if w.opts.RotateOnClose {
				tail = &rotatedSeg{
					first:   w.firstIndex,
					last:    w.lastIndex,
					firstAt: w.segFirstAt,
					lastAt:  w.segLastAt,
					keyFP:   w.keyFingerprintLocked(),
				}
				tailPath = final
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

	// left returns how long a shutdown phase may still take: `limit` capped by
	// what remains of CloseTimeout. 0 means unbounded.
	start := time.Now()
	left := func(limit time.Duration) time.Duration {
		if w.opts.CloseTimeout <= 0 {
			return limit
		}
		rem := w.opts.CloseTimeout - time.Since(start)
		if rem < 50*time.Millisecond {
			rem = 50 * time.Millisecond
		}
		if limit <= 0 || rem < limit {
			return rem
		}
		return limit
	}

	if tail != nil {
		w.markFinalizing(*tail)
		w.rotateWG.Add(1)
		go func() {
			defer w.rotateWG.Done()
			w.finishClosed(tailPath, *tail)
		}()
	}
	if tail != nil || w.arch != nil {
		// Let the pipeline actually finish its segments (canceling ctx below
		// would make it bail out before compression / .meta / OnRotate).
		waitTimeout(&w.rotateWG, left(closeFinalizeGrace))
	}
	if w.arch != nil {
		if d := w.arch.cfg.DrainOnClose; d > 0 {
			w.arch.drain(left(d))
		}
	}

	// Cancel the writer ctx so any retry loops in finalizeRotated /
	// robustCompressZstd give up. Without this they'd loop forever on a stuck
	// disk and rotateWG.Wait() would hang.
	w.ctxCancel()
	if w.arch != nil {
		w.arch.stop()
	}

	timedOut := false
	if w.opts.CloseTimeout > 0 {
		if !waitTimeout(&w.rotateWG, left(0)) {
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

// waitTimeout waits for wg for at most d (d <= 0: no limit). Reports whether
// wg finished.
func waitTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	if d <= 0 {
		wg.Wait()
		return true
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

func (w *Writer) kickJanitor() {
	if w.janitorKick == nil {
		return
	}
	select {
	case w.janitorKick <- struct{}{}:
	default:
	}
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
		return 0, ErrWriterClosed
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
