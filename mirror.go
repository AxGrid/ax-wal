package axwal

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const defaultMirrorAckBatch = 100 * time.Millisecond

// MirrorOptions configures a Mirror session: read from a source WAL, append to
// a destination WAL.
type MirrorOptions struct {
	// SrcDir / DstDir are the source and destination WAL directories. They
	// must NOT be the same directory.
	SrcDir string
	DstDir string

	// Name is the cursor name registered in the SOURCE WAL. Use a stable name
	// so the mirror resumes after restart from the last fully-mirrored index.
	Name string

	// StartFrom controls where the mirror begins on first registration. After
	// the first mirror cycle the cursor takes over.
	StartFrom    StartPosition
	StartAtIndex uint64

	// BatchSize is the number of records the mirror reads/writes per round
	// trip. Defaults to 256.
	BatchSize int

	// DstWriterOptions is applied to the destination Writer. Dir is overridden
	// with DstDir; everything else (SyncMode, GroupCommit, Retention, etc.) is
	// honored.
	DstWriterOptions WriterOptions

	// SrcReaderOptions is applied to the source Reader. Dir/Name/StartFrom are
	// overridden; the rest (Logger, UseMmap, Encryption…) is honored. In
	// particular set Encryption here if the source is encrypted.
	SrcReaderOptions ReaderOptions
}

// Mirror copies records from SrcDir to DstDir until ctx is canceled. Indices
// align iff DstDir starts fresh and no other writer touches it. Returns the
// total number of records mirrored when ctx ends.
//
// Designed for follower setups: run under systemd / k8s, point at a primary
// WAL on shared storage, get a hot standby that's queryable as a normal WAL.
func Mirror(ctx context.Context, opts MirrorOptions) (int64, error) {
	if opts.SrcDir == "" || opts.DstDir == "" {
		return 0, errors.New("axwal: Mirror requires SrcDir and DstDir")
	}
	if opts.SrcDir == opts.DstDir {
		return 0, errors.New("axwal: Mirror SrcDir and DstDir must differ")
	}
	if opts.Name == "" {
		return 0, errors.New("axwal: Mirror.Name is required")
	}
	batch := opts.BatchSize
	if batch <= 0 {
		batch = 256
	}

	rOpts := opts.SrcReaderOptions
	rOpts.Dir = opts.SrcDir
	rOpts.Name = opts.Name
	rOpts.StartFrom = opts.StartFrom
	rOpts.StartAtIndex = opts.StartAtIndex
	if rOpts.AckBatchInterval == 0 {
		// Sensible default: cursor fsync once per 100ms keeps mirror progress
		// up-to-date without bottlenecking on per-record fsyncs.
		rOpts.AckBatchInterval = defaultMirrorAckBatch
	}

	r, err := OpenReader(rOpts)
	if err != nil {
		return 0, fmt.Errorf("mirror: open source reader: %w", err)
	}
	defer r.Close()

	wOpts := opts.DstWriterOptions
	wOpts.Dir = opts.DstDir
	w, err := OpenWriter(wOpts)
	if err != nil {
		return 0, fmt.Errorf("mirror: open dst writer: %w", err)
	}
	defer w.Close()

	var mirrored int64
	for {
		if err := ctx.Err(); err != nil {
			return mirrored, nil
		}
		payloads, indices, err := r.NextBatch(ctx, batch)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return mirrored, nil
			}
			return mirrored, fmt.Errorf("mirror: read: %w", err)
		}
		if len(payloads) == 0 {
			continue
		}
		if _, err := w.AppendBatchContext(ctx, payloads); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return mirrored, nil
			}
			return mirrored, fmt.Errorf("mirror: write: %w", err)
		}
		// Ack after the destination has fsynced (AppendBatchContext returns
		// only after durability — see GroupCommit/SyncMode).
		if err := r.Ack(indices[len(indices)-1]); err != nil {
			return mirrored, fmt.Errorf("mirror: ack: %w", err)
		}
		mirrored += int64(len(payloads))
	}
}
