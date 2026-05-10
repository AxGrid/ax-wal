package axqcompat

import "os"

// openOrCreate is a tiny helper used by tests to fabricate placeholder
// segment files without dragging in additional packages.
func openOrCreate(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
}
