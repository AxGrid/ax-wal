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
