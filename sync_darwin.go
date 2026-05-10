//go:build darwin

package axwal

import (
	"os"
	"syscall"
	"unsafe"
)

// fBarrierFSync is the fcntl op that requests an ordering barrier without a
// full hardware flush. ~30× cheaper than F_FULLFSYNC on Apple SSDs while still
// guaranteeing that earlier writes hit stable storage before later ones.
const fBarrierFSync = 0x55

func syncBarrier(f *os.File) error {
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), fBarrierFSync, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

// F_PREALLOCATE constants from <sys/fcntl.h>.
const (
	fPreallocate    = 42
	fAllocateContig = 0x00000002
	fAllocateAll    = 0x00000004
	fPEOFPosMode    = 3 // offset is relative to physical EOF
)

// fstore_t mirrors XNU's fstore_t exactly. Layout: u_int32 flags, int posmode,
// off_t offset, off_t length, off_t bytesalloc — with 4 bytes of natural
// padding after posmode to align off_t. Go's int32+int64 follows the same rule.
type fstore struct {
	flags      uint32
	posmode    int32
	offset     int64
	length     int64
	bytesAlloc int64
}

// preallocate reserves length bytes past current EOF using F_PREALLOCATE.
// Tries contiguous allocation first; falls back to non-contiguous if the FS
// can't satisfy contiguity (common on fragmented APFS).
func preallocate(f *os.File, length int64) error {
	if length <= 0 {
		return nil
	}
	store := fstore{
		flags:   fAllocateContig | fAllocateAll,
		posmode: fPEOFPosMode,
		offset:  0,
		length:  length,
	}
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), fPreallocate, uintptr(unsafe.Pointer(&store)))
	if errno != 0 {
		store.flags = fAllocateAll
		_, _, errno = syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), fPreallocate, uintptr(unsafe.Pointer(&store)))
		if errno != 0 {
			return errno
		}
	}
	// F_PREALLOCATE doesn't extend the logical file size, only block allocation.
	// That's exactly what we want — readers don't see phantom bytes.
	return nil
}
