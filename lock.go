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
				// flock guarantees the holder process is alive (kernel releases
				// the lock on process exit). Phrase the error to reflect that.
				if pidAlive(pid) {
					return nil, fmt.Errorf("axwal: another writer (PID %d, alive) holds %s", pid, dir)
				}
				// Shouldn't happen on healthy POSIX; surface the oddity.
				return nil, fmt.Errorf("axwal: lock contention for %s (recorded PID %d not alive — flock state is inconsistent; consider removing %s)",
					dir, pid, filepath.Join(dir, writerLockFile))
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

// pidAlive returns true when the given PID exists and signals can be sent to
// it. POSIX kill(pid, 0) doesn't actually signal — it just returns ESRCH if
// the process is gone. False positive: if the PID has been recycled by an
// unrelated process, we'll say "alive" but the lock is functionally stale.
// flock's kernel-level holder bookkeeping is the real source of truth; this
// is purely a friendlier error message.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}
