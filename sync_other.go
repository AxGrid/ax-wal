//go:build unix && !darwin && !linux

package axwal

import "os"

// Fallback for less common Unixes: full Sync() is the safest default.
func syncBarrier(f *os.File) error { return f.Sync() }

// preallocate is a no-op on platforms without a portable equivalent.
func preallocate(f *os.File, length int64) error { return nil }
