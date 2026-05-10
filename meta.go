package axwal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// metaSidecarSuffix is the extension of the segment metadata file.
const metaSidecarSuffix = ".meta"

// SegmentMeta is the JSON payload of a segment's .meta sidecar. Written
// optionally on rotation when WriterOptions.WriteMetaSidecar is set; consumed
// by axwal-cli for verification and by external archivers that want a quick
// summary without parsing the segment file.
type SegmentMeta struct {
	FirstIndex          uint64 `json:"first_index"`
	LastIndex           uint64 `json:"last_index"`
	NumRecords          uint64 `json:"num_records"`
	NumBytesUncompressed int64  `json:"num_bytes_uncompressed"`
	SHA256Uncompressed  string `json:"sha256_uncompressed"`
	CreatedAtUnix       int64  `json:"created_at_unix"` // mtime when written
	Compression         string `json:"compression"`     // "" or "zst"
}

// metaSidecarFor returns the path of the .meta sidecar for the given segment
// base path (e.g. "/wal/wal-1-100" -> "/wal/wal-1-100.meta"). The sidecar
// uses the closed form regardless of compression — wal-1-100 and wal-1-100.zst
// share the same meta file, just like the .archived sidecar.
func metaSidecarFor(dir string, first, last uint64) string {
	return filepath.Join(dir, formatClosedName(first, last)+metaSidecarSuffix)
}

// writeMetaSidecar computes the meta for a freshly-rotated, still-uncompressed
// segment at `uncompressedPath` and writes the sidecar atomically.
func writeMetaSidecar(dir, uncompressedPath string, first, last uint64) error {
	f, err := os.Open(uncompressedPath)
	if err != nil {
		return fmt.Errorf("open %s for meta: %w", uncompressedPath, err)
	}
	defer f.Close()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return fmt.Errorf("hash %s: %w", uncompressedPath, err)
	}
	meta := SegmentMeta{
		FirstIndex:           first,
		LastIndex:            last,
		NumRecords:           last - first + 1,
		NumBytesUncompressed: n,
		SHA256Uncompressed:   hex.EncodeToString(h.Sum(nil)),
		CreatedAtUnix:        time.Now().Unix(),
	}
	body, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}

	path := metaSidecarFor(dir, first, last)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return fsyncDir(dir)
}

// readMetaSidecar loads the SegmentMeta for the given range, or (nil, nil) if
// the sidecar doesn't exist.
func readMetaSidecar(dir string, first, last uint64) (*SegmentMeta, error) {
	body, err := os.ReadFile(metaSidecarFor(dir, first, last))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var m SegmentMeta
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("parse meta sidecar: %w", err)
	}
	return &m, nil
}

// removeMetaSidecar best-effort removes the meta file. Used by retention.
func removeMetaSidecar(dir string, first, last uint64) error {
	if err := os.Remove(metaSidecarFor(dir, first, last)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
