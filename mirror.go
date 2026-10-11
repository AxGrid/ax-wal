package axwal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// archiveMirrorPosFile holds the last source index an archive mirror has
// written to its destination.
const archiveMirrorPosFile = "archive-mirror.pos"

// archiveMirrorSource is an ArchiveReader whose Ack persists the position
// locally (an archive has no server-side cursor).
type archiveMirrorSource struct {
	*ArchiveReader
	posPath string
}

func (s *archiveMirrorSource) Ack(idx uint64) error {
	tmp := s.posPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(idx, 10)+"\n"), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.posPath)
}

func readArchiveMirrorPos(path string) (uint64, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	pos, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", path, err)
	}
	return pos, nil
}

const defaultMirrorAckBatch = 100 * time.Millisecond

// MirrorOptions configures a Mirror session: read from a source WAL, append to
// a destination WAL.
//
// Source can be local (SrcDir set) or remote (SrcRemote set, in which case
// the mirror dials axwal-serve via TCP, optionally through an in-process SSH
// tunnel — see RemoteReaderOptions and SSHTunnelConfig).
type MirrorOptions struct {
	// SrcDir is the local source WAL directory. Mutually exclusive with
	// SrcRemote.
	SrcDir string

	// SrcRemote, when non-nil, makes the mirror pull records from a remote
	// axwal-serve over the wire protocol. SrcRemote.Name overrides Name below.
	SrcRemote *RemoteReaderOptions

	// SrcArchive, when non-nil, makes the mirror follow an archive (object
	// storage) instead of a live WAL: it replays what is there and then keeps
	// picking up segments as the archiver uploads them. Follow is forced on.
	// The position is kept in <DstDir>/archive-mirror.pos, so a restart
	// resumes where it stopped; FromIndex only applies to the first run.
	// Mutually exclusive with SrcDir and SrcRemote.
	SrcArchive *ArchiveReaderOptions

	// DstDir is the destination WAL directory. Required.
	DstDir string

	// Name is the cursor name registered in the local SOURCE WAL. Ignored when
	// SrcRemote is set (server tracks the cursor under SrcRemote.Name).
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

	// SrcReaderOptions is applied to the local source Reader (when SrcDir is
	// used). Dir/Name/StartFrom are overridden; the rest (Logger, UseMmap,
	// Encryption…) is honored.
	SrcReaderOptions ReaderOptions
}

// recordSource is the small surface Mirror needs from its source. Both Reader
// and RemoteReader satisfy it.
type recordSource interface {
	NextBatch(ctx context.Context, max int) ([][]byte, []uint64, error)
	Ack(idx uint64) error
	Close() error
}

// Mirror copies records from SrcDir to DstDir until ctx is canceled. Indices
// align iff DstDir starts fresh and no other writer touches it. Returns the
// total number of records mirrored when ctx ends.
//
// Designed for follower setups: run under systemd / k8s, point at a primary
// WAL on shared storage, get a hot standby that's queryable as a normal WAL.
func Mirror(ctx context.Context, opts MirrorOptions) (int64, error) {
	if opts.DstDir == "" {
		return 0, errors.New("axwal: Mirror.DstDir required")
	}
	sources := 0
	if opts.SrcDir != "" {
		sources++
	}
	if opts.SrcRemote != nil {
		sources++
	}
	if opts.SrcArchive != nil {
		sources++
	}
	if sources != 1 {
		return 0, errors.New("axwal: Mirror requires exactly one of SrcDir, SrcRemote or SrcArchive")
	}
	if opts.SrcDir != "" && opts.SrcDir == opts.DstDir {
		return 0, errors.New("axwal: Mirror SrcDir and DstDir must differ")
	}
	batch := opts.BatchSize
	if batch <= 0 {
		batch = 256
	}

	var src recordSource
	if opts.SrcArchive != nil {
		ao := *opts.SrcArchive
		ao.Follow = true
		posPath := filepath.Join(opts.DstDir, archiveMirrorPosFile)
		pos, err := readArchiveMirrorPos(posPath)
		if err != nil {
			return 0, fmt.Errorf("mirror: %w", err)
		}
		if pos > 0 {
			ao.FromIndex = pos + 1
		}
		ar, err := OpenArchiveReader(ctx, ao)
		if err != nil {
			return 0, fmt.Errorf("mirror: open archive: %w", err)
		}
		src = &archiveMirrorSource{ArchiveReader: ar, posPath: posPath}
	} else if opts.SrcRemote != nil {
		ro := *opts.SrcRemote
		if ro.BatchSize <= 0 {
			ro.BatchSize = batch
		}
		rr, err := DialRemoteReader(ctx, ro)
		if err != nil {
			return 0, fmt.Errorf("mirror: dial remote: %w", err)
		}
		src = rr
	} else {
		if opts.Name == "" {
			return 0, errors.New("axwal: Mirror.Name required for local source")
		}
		rOpts := opts.SrcReaderOptions
		rOpts.Dir = opts.SrcDir
		rOpts.Name = opts.Name
		rOpts.StartFrom = opts.StartFrom
		rOpts.StartAtIndex = opts.StartAtIndex
		if rOpts.AckBatchInterval == 0 {
			rOpts.AckBatchInterval = defaultMirrorAckBatch
		}
		r, err := OpenReader(rOpts)
		if err != nil {
			return 0, fmt.Errorf("mirror: open source reader: %w", err)
		}
		src = r
	}
	defer src.Close()

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
		payloads, indices, err := src.NextBatch(ctx, batch)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return mirrored, nil
			}
			if errors.Is(err, io.EOF) {
				// Bounded archive source (ToIndex / ToTime) fully replayed.
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
		if err := src.Ack(indices[len(indices)-1]); err != nil {
			return mirrored, fmt.Errorf("mirror: ack: %w", err)
		}
		mirrored += int64(len(payloads))
	}
}
