//go:build unix

package axwal

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const writerLockFile = ".writer.lock"

type writerLock struct {
	f    *os.File
	path string
}

// acquireWriterLock takes an exclusive non-blocking flock on dir/.writer.lock.
// On contention returns an error containing the holder PID (best-effort, may be 0).
func acquireWriterLock(dir string) (*writerLock, error) {
	path := filepath.Join(dir, writerLockFile)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK {
			pid := readPID(path)
			if pid > 0 {
				return nil, fmt.Errorf("axwal: another writer (PID %d) holds %s", pid, dir)
			}
			return nil, fmt.Errorf("axwal: another writer holds %s", dir)
		}
		return nil, fmt.Errorf("flock: %w", err)
	}
	if err := writePID(f); err != nil {
		// non-fatal; we already hold the lock
		_ = err
	}
	return &writerLock{f: f, path: path}, nil
}

func (l *writerLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	closeErr := l.f.Close()
	l.f = nil
	rmErr := os.Remove(l.path)
	if closeErr != nil {
		return closeErr
	}
	if rmErr != nil && !os.IsNotExist(rmErr) {
		return rmErr
	}
	return nil
}

func writePID(f *os.File) error {
	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "%d\n", os.Getpid()); err != nil {
		return err
	}
	return f.Sync()
}

func readPID(path string) int {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	var pid int
	_, _ = fmt.Sscanf(string(b), "%d", &pid)
	return pid
}
