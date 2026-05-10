//go:build unix

package axwal

import (
	"os"
	"syscall"
)

// mmapReadOnly maps the first `size` bytes of f for reading. The caller owns
// the returned slice and must call munmap on it before letting it go out of
// scope (otherwise the mapping leaks until process exit).
func mmapReadOnly(f *os.File, size int64) ([]byte, error) {
	if size <= 0 {
		return nil, nil
	}
	return syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmap(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return syscall.Munmap(b)
}
