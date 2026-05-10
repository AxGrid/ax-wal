package axwal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// archivedSidecarSuffix is the suffix appended to a segment's BASE name (without
// any compression extension) to mark it as successfully uploaded by an external
// archiver.
const archivedSidecarSuffix = ".archived"

// archivedSidecarFor returns the path of the .archived sidecar that would mark
// the given segment. The sidecar's name is based on wal-F-L (no compression
// suffix), so that wal-1-100 and wal-1-100.zst share the same marker.
func archivedSidecarFor(dir string, s segName) string {
	base := formatClosedName(s.FirstIndex, s.LastIndex)
	return filepath.Join(dir, base+archivedSidecarSuffix)
}

// isArchived reports whether a segment has its .archived sidecar present.
func isArchived(dir string, s segName) bool {
	if !s.Closed {
		return false
	}
	_, err := os.Stat(archivedSidecarFor(dir, s))
	return err == nil
}

// MarkArchived creates the .archived sidecar for the segment covering the
// given range. Returns an error if no matching closed segment exists.
//
// Typical use: an external uploader, after successfully writing a segment to
// S3/B2/cold storage, calls this so that retention can delete the local copy.
func (w *Writer) MarkArchived(firstIndex, lastIndex uint64) error {
	return MarkArchived(w.dir, firstIndex, lastIndex)
}

// MarkArchived is the package-level form usable by external tools that don't
// hold a Writer (e.g., the admin CLI).
func MarkArchived(dir string, firstIndex, lastIndex uint64) error {
	if firstIndex == 0 || lastIndex+1 < firstIndex {
		return fmt.Errorf("axwal: invalid index range %d..%d", firstIndex, lastIndex)
	}
	// Verify the segment actually exists in some closed form.
	candidates, err := filepath.Glob(filepath.Join(dir, formatClosedName(firstIndex, lastIndex)+"*"))
	if err != nil {
		return err
	}
	found := false
	for _, c := range candidates {
		base := filepath.Base(c)
		if !strings.HasPrefix(base, formatClosedName(firstIndex, lastIndex)) {
			continue
		}
		// Skip the sidecar itself.
		if strings.HasSuffix(base, archivedSidecarSuffix) {
			continue
		}
		// Match either wal-F-L exactly or wal-F-L.<compression>.
		rest := strings.TrimPrefix(base, formatClosedName(firstIndex, lastIndex))
		if rest == "" || (strings.HasPrefix(rest, ".") && !strings.Contains(rest[1:], ".")) {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("axwal: no segment matching wal-%d-%d* in %s", firstIndex, lastIndex, dir)
	}

	path := filepath.Join(dir, formatClosedName(firstIndex, lastIndex)+archivedSidecarSuffix)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return fsyncDir(dir)
}

// removeArchivedSidecar removes the sidecar (best-effort; ignores not-found).
func removeArchivedSidecar(dir string, s segName) error {
	if err := os.Remove(archivedSidecarFor(dir, s)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
