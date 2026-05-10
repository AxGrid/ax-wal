package axwal

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	renameRetryInterval      = 500 * time.Millisecond
	compressionRetryInterval = 500 * time.Millisecond
)

// finalizeRotated runs in its own goroutine after rotation. It renames the open
// segment to wal-F-L and (optionally) compresses it to .zst, retrying on transient
// I/O errors. It must not panic — the writer's correctness depends only on the
// rename eventually succeeding.
func (w *Writer) finalizeRotated(openPath string, first, last uint64) {
	defer w.rotateWG.Done()

	finalPath := filepath.Join(w.dir, formatClosedName(first, last))
	if err := robustRename(openPath, finalPath); err != nil {
		log.Printf("axwal: finalize rename %s -> %s: %v", openPath, finalPath, err)
		return
	}

	if !w.opts.CompressOnRotate {
		return
	}
	zstPath := finalPath + ".zst"
	if err := robustCompressZstd(finalPath, zstPath); err != nil {
		log.Printf("axwal: zstd compress %s -> %s: %v (leaving uncompressed)", finalPath, zstPath, err)
	}
}

func robustRename(oldPath, newPath string) error {
	for attempt := 1; ; attempt++ {
		if err := os.Rename(oldPath, newPath); err == nil {
			if attempt > 2 {
				log.Printf("axwal: rename %s -> %s succeeded after %d attempts", oldPath, newPath, attempt)
			}
			return nil
		} else if attempt > 2 {
			log.Printf("axwal: rename attempt %d failed: %v; retry in %s", attempt, err, renameRetryInterval)
		}
		time.Sleep(renameRetryInterval)
	}
}

func robustCompressZstd(src, dst string) error {
	for attempt := 1; ; attempt++ {
		err := compressOnceZstd(src, dst)
		if err == nil {
			if rmErr := os.Remove(src); rmErr != nil {
				log.Printf("axwal: compress: failed to remove %s after success: %v", src, rmErr)
			}
			return nil
		}
		log.Printf("axwal: compress attempt %d for %s failed: %v; retry in %s", attempt, src, err, compressionRetryInterval)
		_ = os.Remove(dst)
		time.Sleep(compressionRetryInterval)
	}
}

func compressOnceZstd(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}

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
