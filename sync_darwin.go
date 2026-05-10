//go:build darwin

package axwal

import (
	"os"
	"syscall"
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
