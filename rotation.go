package axwal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	renameRetryInterval      = 500 * time.Millisecond
	compressionRetryInterval = 500 * time.Millisecond

	// compressTmpSuffix marks an in-progress compression output. Leftovers
	// from a crash are removed by recovery.
	compressTmpSuffix = ".tmp"
)

// rotatedSeg carries what the writer knew about a segment at the moment it
// stopped appending to it.
type rotatedSeg struct {
	first, last uint64
	// firstAt / lastAt are the append times of the first and last record.
	firstAt, lastAt time.Time
	// keyFP is the fingerprint of the encryption key in effect ("" = none).
	keyFP string
}

type segRange struct{ first, last uint64 }

// finalizeRotated runs in its own goroutine after rotation. It renames the
// open segment to wal-F-L and hands it to finishClosed, retrying on transient
// I/O errors. The writer's ctx provides cancellation: when Close() fires,
// in-flight retries give up cleanly and the segment is left in whatever
// intermediate state it reached (recovery on next OpenWriter handles that —
// see recover_).
func (w *Writer) finalizeRotated(openPath string, rs rotatedSeg) {
	defer w.rotateWG.Done()

	finalPath := filepath.Join(w.dir, formatClosedName(rs.first, rs.last))
	if err := robustRename(w.ctx, w.log, openPath, finalPath); err != nil {
		w.doneFinalizing(rs)
		if errors.Is(err, context.Canceled) {
			w.log.Warn("rotation rename abandoned (writer closing)",
				"old", filepath.Base(openPath), "new", filepath.Base(finalPath))
			return
		}
		w.log.Error("rotation rename failed", "old", filepath.Base(openPath),
			"new", filepath.Base(finalPath), "err", err)
		return
	}
	w.finishClosed(finalPath, rs)
}

// finishClosed runs the post-rename pipeline for a closed, uncompressed
// segment: hash for .meta, compress, write .meta, call OnRotate, then release
// the segment to the archiver and the janitor.
func (w *Writer) finishClosed(finalPath string, rs rotatedSeg) {
	defer w.doneFinalizing(rs)

	// The meta hash covers the UNCOMPRESSED bytes, so it has to be taken
	// before compression removes that file.
	var meta *SegmentMeta
	if w.opts.WriteMetaSidecar {
		m, err := computeSegmentMeta(finalPath, rs.first, rs.last)
		if err != nil {
			w.log.Error("meta sidecar: hashing failed", "name", filepath.Base(finalPath), "err", err)
		} else {
			if !rs.firstAt.IsZero() {
				m.FirstRecordUnix = rs.firstAt.Unix()
				m.LastRecordUnix = rs.lastAt.Unix()
			}
			m.KeyFingerprint = rs.keyFP
			meta = &m
		}
	}

	finalLivePath := finalPath
	compressedFlag := false

	if w.opts.CompressOnRotate {
		zstPath := finalPath + ".zst"
		if err := robustCompressZstd(w.ctx, w.log, finalPath, zstPath); err != nil {
			if errors.Is(err, context.Canceled) {
				w.log.Warn("compression abandoned (writer closing)",
					"src", filepath.Base(finalPath))
				return
			}
			w.log.Error("zstd compress failed (leaving uncompressed)",
				"src", filepath.Base(finalPath), "dst", filepath.Base(zstPath), "err", err)
		} else {
			finalLivePath = zstPath
			compressedFlag = true
		}
	}

	// Optional .meta sidecar — written before the user callback so the callback
	// can read it (e.g., to upload metadata alongside the segment).
	if meta != nil {
		if compressedFlag {
			meta.Compression = "zst"
		}
		if err := writeMetaSidecar(w.dir, *meta); err != nil {
			w.log.Error("meta sidecar write failed", "name", filepath.Base(finalPath), "err", err)
		}
	}

	if w.opts.OnRotate != nil {
		fi, statErr := os.Stat(finalLivePath)
		var size int64
		if statErr == nil {
			size = fi.Size()
		}
		info := SegmentInfo{
			Path:       finalLivePath,
			FirstIndex: rs.first,
			LastIndex:  rs.last,
			SizeBytes:  size,
			Compressed: compressedFlag,
		}
		// Recover from panics so a buggy user callback can't tear down the
		// writer. We DON'T recover from cancellation — a hung callback at
		// shutdown is the user's problem (we already log via CloseTimeout).
		func() {
			defer func() {
				if r := recover(); r != nil {
					w.log.Error("OnRotate callback panicked",
						"segment", filepath.Base(finalLivePath), "panic", fmt.Sprintf("%v", r))
				}
			}()
			w.opts.OnRotate(info)
		}()
	}
}

// markFinalizing records that a segment is still going through the rotation
// pipeline, so the archiver doesn't pick up a half-compressed file.
func (w *Writer) markFinalizing(rs rotatedSeg) {
	w.finMu.Lock()
	if w.finalizing == nil {
		w.finalizing = map[segRange]struct{}{}
	}
	w.finalizing[segRange{rs.first, rs.last}] = struct{}{}
	w.finMu.Unlock()
}

func (w *Writer) doneFinalizing(rs rotatedSeg) {
	w.finMu.Lock()
	delete(w.finalizing, segRange{rs.first, rs.last})
	w.finMu.Unlock()
	if w.arch != nil {
		w.arch.wake()
	}
	w.kickJanitor()
}

func (w *Writer) isFinalizing(first, last uint64) bool {
	w.finMu.Lock()
	defer w.finMu.Unlock()
	_, ok := w.finalizing[segRange{first, last}]
	return ok
}

// sleepCtx waits for d or until ctx is done. Returns the ctx error on cancel.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func robustRename(ctx context.Context, log *slog.Logger, oldPath, newPath string) error {
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := os.Rename(oldPath, newPath)
		if err == nil {
			if attempt > 2 {
				log.Info("rename succeeded after retries",
					"old", filepath.Base(oldPath), "new", filepath.Base(newPath), "attempts", attempt)
			}
			return nil
		}
		if attempt > 2 {
			log.Warn("rename retrying", "attempts", attempt, "err", err,
				"old", filepath.Base(oldPath), "new", filepath.Base(newPath))
		}
		if err := sleepCtx(ctx, renameRetryInterval); err != nil {
			return err
		}
	}
}

func robustCompressZstd(ctx context.Context, log *slog.Logger, src, dst string) error {
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := compressOnceZstd(src, dst)
		if err == nil {
			if rmErr := os.Remove(src); rmErr != nil {
				log.Warn("compress: remove source failed", "src", filepath.Base(src), "err", rmErr)
			}
			return nil
		}
		log.Warn("compress retrying", "src", filepath.Base(src), "attempts", attempt, "err", err)
		_ = os.Remove(dst)
		if err := sleepCtx(ctx, compressionRetryInterval); err != nil {
			return err
		}
	}
}

func compressOnceZstd(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	// Compress into a temp name and rename: wal-F-L.zst must never be visible
	// half-written, or a reader / the archiver could pick up a truncated file.
	tmp := dst + compressTmpSuffix
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer os.Remove(tmp) // no-op after a successful rename

	enc, err := zstd.NewWriter(out,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(runtime.NumCPU()),
	)
	if err != nil {
		out.Close()
		return err
	}

	if _, err := io.Copy(enc, in); err != nil {
		enc.Close()
		out.Close()
		return err
	}
	if err := enc.Close(); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return err
	}
	// Make sure the compressed name is durable in the directory.
	return fsyncDir(filepath.Dir(dst))
}

// awaitRotations blocks the caller until all in-flight rotation goroutines finish.
// Used by tests.
func (w *Writer) awaitRotations() {
	w.rotateWG.Wait()
}

// _ used to keep fmt referenced when some helpers are commented out.
var _ = fmt.Sprintf
