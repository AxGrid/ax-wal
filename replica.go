package axwal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// replicaMarkerFile marks a WAL directory as a read-only replica.
const replicaMarkerFile = "REPLICA"

// ErrReplicaDir is returned by OpenWriter for a directory that is a replica:
// only RunReplica may write there, so the copy can't drift from its source.
// Call PromoteReplica to turn it into a normal, writable WAL.
var ErrReplicaDir = errors.New("axwal: directory is a read-only replica (see PromoteReplica)")

// ErrReplicaDiverged is returned by RunReplica when the source can no longer
// continue the replica's log exactly where it ends: the source has already
// deleted the next records, or the replica holds records the source doesn't.
// The replica is left untouched; re-seed it (empty directory) or promote it.
var ErrReplicaDiverged = errors.New("axwal: replica diverged from its source")

// ReplicaOptions configures RunReplica.
type ReplicaOptions struct {
	// Dir is the local replica directory. It must be empty (or not exist) on
	// the first run; afterwards it is recognized by its marker file.
	Dir string

	// Source is the primary's axwal-serve endpoint (Addr, optional
	// SSHTunnel). Source.Name is the cursor registered on the primary for
	// this replica: while the replica is connected or was seen within the
	// primary's StaleReaderTTL, the primary's retention keeps what the
	// replica hasn't received yet. StartIndex is ignored.
	Source RemoteReaderOptions

	// Writer tunes the local WAL (SyncMode, segment size, compression,
	// retention, archive…). Dir is overridden. Leave Encryption unset:
	// records are copied as stored, so an encrypted primary yields a replica
	// encrypted with the same keys.
	Writer WriterOptions

	// Reconnect is the backoff between connection attempts while the
	// primary is unreachable. Default {Min: 1s, Max: 30s}.
	Reconnect Backoff

	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

type replicaMarker struct {
	Source        string `json:"source"`
	Name          string `json:"name"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

// IsReplica reports whether dir carries the replica marker.
func IsReplica(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, replicaMarkerFile))
	return err == nil
}

// PromoteReplica turns a replica directory into a normal WAL that OpenWriter
// accepts — the failover step once the primary is gone. It fails while a
// RunReplica (or any writer) still holds the directory. Indices continue
// after the last replicated record.
func PromoteReplica(dir string) error {
	if !IsReplica(dir) {
		return fmt.Errorf("axwal: %s is not a replica", dir)
	}
	lock, err := acquireWriterLock(dir)
	if err != nil {
		return fmt.Errorf("promote: replica is still running: %w", err)
	}
	defer lock.Release()
	if err := os.Remove(filepath.Join(dir, replicaMarkerFile)); err != nil {
		return err
	}
	return fsyncDir(dir)
}

// RunReplica maintains a read-only copy of a remote WAL until ctx is
// canceled (then it returns nil).
//
// Unlike Mirror, the copy is exact: record N on the replica is record N on
// the primary. The replica always asks the primary for "my last index + 1",
// so a restart or a dropped connection neither skips nor duplicates records,
// and it refuses to continue (ErrReplicaDiverged) rather than paper over a
// hole. Local readers use the directory like any WAL; OpenWriter is rejected
// with ErrReplicaDir until PromoteReplica.
//
// Replication is asynchronous: a record is acknowledged to the producer once
// it is durable on the primary, and reaches the replica a moment later. If
// the primary's disk is lost, records it had not shipped yet are lost too.
// Segment boundaries on the replica are its own; indices and payloads match.
func RunReplica(ctx context.Context, opts ReplicaOptions) error {
	if opts.Dir == "" {
		return errors.New("axwal: ReplicaOptions.Dir required")
	}
	if opts.Source.Addr == "" || opts.Source.Name == "" {
		return errors.New("axwal: ReplicaOptions.Source.Addr and Source.Name required")
	}
	if opts.Reconnect.Min <= 0 {
		opts.Reconnect.Min = time.Second
	}
	if opts.Reconnect.Max < opts.Reconnect.Min {
		opts.Reconnect.Max = max(30*time.Second, opts.Reconnect.Min)
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	log := opts.Logger.With("component", "axwal.replica", "dir", opts.Dir, "source", opts.Source.Addr)

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return err
	}
	if !IsReplica(opts.Dir) {
		// Never adopt an existing WAL: it may be a (former) primary whose
		// records have nothing to do with this source.
		if segs, _ := filepath.Glob(filepath.Join(opts.Dir, "wal-*")); len(segs) > 0 {
			return fmt.Errorf("axwal: %s already holds a WAL that is not a replica; start from an empty directory", opts.Dir)
		}
		body, _ := json.MarshalIndent(replicaMarker{
			Source: opts.Source.Addr, Name: opts.Source.Name, CreatedAtUnix: time.Now().Unix(),
		}, "", "  ")
		if err := os.WriteFile(filepath.Join(opts.Dir, replicaMarkerFile), body, 0644); err != nil {
			return err
		}
	}

	wOpts := opts.Writer
	wOpts.Dir = opts.Dir
	wOpts.replica = true
	if wOpts.Logger == nil {
		wOpts.Logger = opts.Logger
	}
	w, err := OpenWriter(wOpts)
	if err != nil {
		return fmt.Errorf("replica: open local WAL: %w", err)
	}
	defer w.Close()

	batch := opts.Source.BatchSize
	if batch <= 0 {
		batch = 256
	}
	for attempt := 0; ; {
		progressed, err := replicateOnce(ctx, w, opts.Source, batch, log)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, ErrReplicaDiverged) || errors.Is(err, ErrTransportAuth) {
			// Neither gets better by retrying: the logs don't line up, or
			// the two sides don't share a transport key.
			return err
		}
		if progressed {
			attempt = 0
		}
		attempt++
		delay := opts.Reconnect.delay(attempt)
		log.Warn("source connection lost; reconnecting", "retry_in", delay, "last_index", w.LastIndex(), "err", err)
		if sleepCtx(ctx, delay) != nil {
			return nil
		}
	}
}

// replicateOnce runs one connection: dial, then copy batches until an error.
// progressed reports whether at least one batch was written.
func replicateOnce(ctx context.Context, w *Writer, src RemoteReaderOptions, batch int, log *slog.Logger) (progressed bool, err error) {
	last := w.LastIndex()
	src.StartIndex = last + 1
	src.exact = true
	src.BatchSize = batch
	// Acknowledge every batch as soon as it is on disk: the primary may be
	// holding appends until it hears from us (SyncReplication).
	src.AckEvery = 1
	rr, err := DialRemoteReader(ctx, src)
	if err != nil {
		return false, err
	}
	defer rr.Close()

	if head := rr.HeadIndex(); head < last {
		return false, fmt.Errorf("%w: replica is at index %d but the source ends at %d", ErrReplicaDiverged, last, head)
	}
	log.Info("replicating", "from_index", last+1, "source_head", rr.HeadIndex())

	for {
		payloads, indices, err := rr.NextBatch(ctx, batch)
		if err != nil {
			return progressed, err
		}
		want := w.LastIndex() + 1
		if indices[0] != want {
			if w.LastIndex() != 0 || indices[0] < want {
				return progressed, fmt.Errorf("%w: need index %d, source continues at %d (already deleted there?)",
					ErrReplicaDiverged, want, indices[0])
			}
			// Brand-new replica of a source whose history starts later than
			// index 1: start the local log at the same index.
			if err := w.rebaseEmpty(indices[0]); err != nil {
				return progressed, err
			}
			log.Info("source history starts past index 1; replica starts there too", "first_index", indices[0])
		}
		for i := 1; i < len(indices); i++ {
			if indices[i] != indices[i-1]+1 {
				return progressed, fmt.Errorf("%w: source sent index %d after %d", ErrReplicaDiverged, indices[i], indices[i-1])
			}
		}
		if _, err := w.AppendBatchContext(ctx, payloads); err != nil {
			return progressed, err
		}
		lastIdx := indices[len(indices)-1]
		if got := w.LastIndex(); got != lastIdx {
			// Can't happen while we are the only writer; never ack past it.
			return progressed, fmt.Errorf("%w: local index %d after writing source index %d", ErrReplicaDiverged, got, lastIdx)
		}
		progressed = true
		// Acked only after the batch is on local disk: the primary's cursor
		// for this replica never runs ahead of what the replica really has.
		if err := rr.Ack(lastIdx); err != nil {
			return progressed, err
		}
	}
}

// rebaseEmpty makes a writer that has never held a record continue from
// index `first` instead of 1.
func (w *Writer) rebaseEmpty(first uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWriterClosed
	}
	if w.lastIndex != 0 || w.curRecords != 0 || first == 0 {
		return fmt.Errorf("axwal: cannot rebase a non-empty WAL (last index %d) to %d", w.lastIndex, first)
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
		if err := os.Remove(w.curPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		w.file, w.buf = nil, nil
	}
	w.lastIndex = first - 1
	w.commitIndex = w.lastIndex
	return w.openNewSegment()
}
