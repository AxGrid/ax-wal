//go:build linux

package axwal

import (
	"os"
	"syscall"
)

// On Linux fdatasync skips metadata that doesn't affect later read consistency
// (mtime, etc.) so it's typically 2-5× faster than fsync for append-only logs.
// File size IS metadata, so it still ends up flushing inode entries; this is
// fine for our purposes — readers never depend on stat().
func syncBarrier(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

// preallocate uses fallocate(2) to reserve space. Mode 0 actually allocates
// (not FALLOC_FL_KEEP_SIZE) so the file size grows; subsequent O_APPEND writes
// land at the actual data tail thanks to ftruncate-on-Close-if-needed elsewhere.
// Actually we DO want KEEP_SIZE here because we want disk reservation without
// changing the logical EOF (so bufio's append still works correctly).
func preallocate(f *os.File, length int64) error {
	if length <= 0 {
		return nil
	}
	const fallocKeepSize = 0x01 // FALLOC_FL_KEEP_SIZE
	return syscall.Fallocate(int(f.Fd()), fallocKeepSize, 0, length)
}
