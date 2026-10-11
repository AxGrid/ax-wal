package axwal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// recover_ scans the WAL directory and brings it to a clean state suitable for a
// fresh openNewSegment(). It returns the highest record index found.
//
// Steps:
//   1. List all wal-* files. Drop empty open segments.
//   2. Re-read every open segment with CRC validation, truncating its tail at the
//      first corrupt/truncated record. Then rename it to wal-F-L (closed form).
//   3. Sort closed segments by FirstIndex; pick the one with the highest LastIndex.
//   4. Return that LastIndex (or 0 if no segments at all).
//
// This treats CRC failure or trailing zeros at the tail of an open segment as
// "torn write at crash time" and discards them.
func recover_(dir string, compressOnRotate bool, log *slog.Logger) (uint64, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "wal-*"))
	if err != nil {
		return 0, fmt.Errorf("glob: %w", err)
	}

	var (
		open   []segName
		closed []segName
	)
	for _, p := range matches {
		base := filepath.Base(p)
		if base == writerLockFile {
			continue
		}
		if isSegmentSidecar(base) {
			continue
		}
		if strings.HasSuffix(base, ".zst"+compressTmpSuffix) {
			// Compression interrupted by a crash; the source segment is intact.
			_ = os.Remove(p)
			continue
		}
		s, err := parseSegName(base)
		if err != nil {
			log.Warn("recovery: skipping unrecognized file", "name", base, "err", err)
			continue
		}
		if s.Closed {
			closed = append(closed, s)
		} else {
			open = append(open, s)
		}
	}

	// A crash between "wal-F-L.zst renamed into place" and "wal-F-L removed"
	// leaves both forms. The .zst is complete (it is renamed atomically), so
	// the uncompressed twin is redundant.
	zst := map[segRange]bool{}
	for _, s := range closed {
		if s.Compression != "" {
			zst[segRange{s.FirstIndex, s.LastIndex}] = true
		}
	}
	for _, s := range closed {
		if s.Compression == "" && zst[segRange{s.FirstIndex, s.LastIndex}] {
			if err := os.Remove(filepath.Join(dir, s.Name)); err == nil {
				log.Info("recovery: removed uncompressed twin of compressed segment", "name", s.Name)
			}
		}
	}

	// Step 1+2: finalize open segments.
	for _, s := range open {
		path := filepath.Join(dir, s.Name)
		st, err := os.Stat(path)
		if err != nil {
			return 0, fmt.Errorf("stat %s: %w", path, err)
		}
		if st.Size() == 0 {
			if err := os.Remove(path); err != nil {
				return 0, fmt.Errorf("remove empty %s: %w", path, err)
			}
			log.Info("recovery: removed empty open segment", "name", base(path))
			continue
		}

		actualLast, validBytes, err := readOpenSegmentTail(path, s.FirstIndex, log)
		if err != nil {
			return 0, fmt.Errorf("scan open segment %s: %w", path, err)
		}

		// Truncate the file to the last good byte if there's torn data at the end.
		if err := truncateIfNeeded(path, validBytes); err != nil {
			return 0, fmt.Errorf("truncate %s: %w", path, err)
		}

		if actualLast < s.FirstIndex {
			// Empty after truncation: drop it.
			if err := os.Remove(path); err != nil {
				return 0, fmt.Errorf("remove empty after trunc %s: %w", path, err)
			}
			log.Info("recovery: open segment held no valid records, removed", "name", base(path))
			continue
		}

		finalName := formatClosedName(s.FirstIndex, actualLast)
		finalPath := filepath.Join(dir, finalName)
		if err := os.Rename(path, finalPath); err != nil {
			return 0, fmt.Errorf("rename %s -> %s: %w", path, finalPath, err)
		}
		closed = append(closed, segName{
			Name:       finalName,
			FirstIndex: s.FirstIndex,
			LastIndex:  actualLast,
			Closed:     true,
		})
		log.Info("recovery: finalized open segment",
			"old", base(path), "new", finalName, "records", actualLast-s.FirstIndex+1)

		if compressOnRotate {
			zstPath := finalPath + ".zst"
			if err := compressOnceZstd(finalPath, zstPath); err == nil {
				_ = os.Remove(finalPath)
			} else {
				log.Warn("recovery: compress failed (leaving uncompressed)",
					"name", finalName, "err", err)
				_ = os.Remove(zstPath)
			}
		}
	}

	if err := fsyncDir(dir); err != nil {
		return 0, fmt.Errorf("fsync dir: %w", err)
	}

	// Step 3: pick the highest LastIndex among closed segments.
	if len(closed) == 0 {
		return 0, nil
	}
	sort.Slice(closed, func(i, j int) bool { return closed[i].LastIndex > closed[j].LastIndex })
	return closed[0].LastIndex, nil
}

// readOpenSegmentTail walks an open segment frame by frame to find the index of
// the last valid record. Returns:
//   - lastIndex (firstIndex-1 if zero records were valid)
//   - validBytes: the file offset just past the last valid record (where to truncate)
func readOpenSegmentTail(path string, firstIndex uint64, log *slog.Logger) (uint64, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	br := bufio.NewReaderSize(f, 64*1024)
	last := firstIndex - 1
	var pos int64

	for {
		// Track the offset before the next record so we can truncate accurately.
		startPos := pos
		// Use a counting reader so we know how far we got into a corrupt frame.
		cr := &countingReader{r: br}
		_, err := readRecord(cr)
		if err == io.EOF {
			return last, pos, nil
		}
		if err != nil {
			if errors.Is(err, ErrCorruptRecord) || errors.Is(err, ErrTruncatedRecord) {
				log.Warn("recovery: truncating segment at corrupt/short frame",
					"name", filepath.Base(path), "offset", startPos, "err", err)
				return last, startPos, nil
			}
			return 0, 0, err
		}
		pos = startPos + cr.n
		last++
	}
}

func truncateIfNeeded(path string, validBytes int64) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if st.Size() == validBytes {
		return nil
	}
	if validBytes > st.Size() {
		return fmt.Errorf("validBytes %d > file size %d", validBytes, st.Size())
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(validBytes); err != nil {
		return err
	}
	return f.Sync()
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func base(p string) string { return filepath.Base(p) }

// isSegmentSidecar reports whether name is a per-segment sidecar
// (.archived / .meta) rather than a segment file.
func isSegmentSidecar(name string) bool {
	return strings.HasSuffix(name, archivedSidecarSuffix) ||
		strings.HasSuffix(name, metaSidecarSuffix) ||
		strings.HasSuffix(name, metaSidecarSuffix+".tmp")
}

// _ keeps zstd referenced so go vet doesn't complain on the unused-import path
// when CompressOnRotate is false. Import is needed by recovery's compress branch.
var _ = zstd.SpeedDefault

// verifyChain CRC-walks every closed segment, dedupes (wal-F-L vs
// wal-F-L.zst) pairs, and reports gaps in the index chain. Called at
// OpenWriter time when WriterOptions.VerifyOnOpen is set.
//
// Side effects: when both compressed and uncompressed forms of the same
// segment exist, removes the uncompressed twin. Also removes any segment
// that fails CRC (callers should rely on cluster-level redundancy or backups
// to recover from CRC corruption — keeping a known-bad segment in the chain
// would silently break readers).
//
// Returns nil even when gaps are detected — the caller already lost that data
// so failing the open would just make recovery harder. Gaps are surfaced via
// log warnings.
func verifyChain(dir string, log *slog.Logger) error {
	matches, err := filepath.Glob(filepath.Join(dir, "wal-*"))
	if err != nil {
		return err
	}

	// Group by (FirstIndex, LastIndex). When two entries share the range, one
	// is uncompressed and one is .zst — we keep the .zst.
	type key struct{ first, last uint64 }
	type cand struct {
		path    string
		seg     segName
	}
	groups := map[key][]cand{}
	for _, p := range matches {
		base := filepath.Base(p)
		s, err := parseSegName(base)
		if err != nil || !s.Closed {
			continue
		}
		k := key{s.FirstIndex, s.LastIndex}
		groups[k] = append(groups[k], cand{p, s})
	}

	// Resolve duplicates.
	for k, cs := range groups {
		if len(cs) <= 1 {
			continue
		}
		// Prefer .zst. If both are .zst (impossible by parser) or both raw,
		// keep the larger one.
		var keepIdx int = -1
		for i, c := range cs {
			if c.seg.Compression == "zst" {
				keepIdx = i
				break
			}
		}
		if keepIdx == -1 {
			keepIdx = 0
		}
		for i, c := range cs {
			if i == keepIdx {
				continue
			}
			log.Warn("verify: duplicate range; removing twin",
				"keeping", filepath.Base(cs[keepIdx].path),
				"removing", filepath.Base(c.path),
				"first", k.first, "last", k.last)
			if err := os.Remove(c.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Error("verify: remove twin failed", "path", c.path, "err", err)
			}
		}
	}

	// Re-scan after dedup for chain analysis.
	matches, err = filepath.Glob(filepath.Join(dir, "wal-*"))
	if err != nil {
		return err
	}
	var closed []segName
	for _, p := range matches {
		base := filepath.Base(p)
		s, err := parseSegName(base)
		if err != nil || !s.Closed {
			continue
		}
		closed = append(closed, s)
	}
	sort.Slice(closed, func(i, j int) bool { return closed[i].FirstIndex < closed[j].FirstIndex })

	// CRC walk.
	for _, s := range closed {
		path := filepath.Join(dir, s.Name)
		got, err := walkAndCount(path)
		if err != nil {
			log.Error("verify: segment failed CRC walk; removing",
				"name", s.Name, "records_seen", got, "err", err)
			if rmErr := os.Remove(path); rmErr != nil {
				log.Error("verify: remove failed segment", "name", s.Name, "err", rmErr)
			}
			continue
		}
		expected := s.LastIndex - s.FirstIndex + 1
		if uint64(got) != expected {
			log.Warn("verify: record count mismatch",
				"name", s.Name, "expected", expected, "got", got)
		}
	}

	// Gap detection.
	var prev uint64 // last LastIndex seen
	first := true
	for _, s := range closed {
		// Skip the segment if it was removed by CRC walk.
		if _, statErr := os.Stat(filepath.Join(dir, s.Name)); statErr != nil {
			continue
		}
		if first {
			if s.FirstIndex != 1 {
				log.Warn("verify: chain doesn't start at 1", "first_segment", s.Name, "first_index", s.FirstIndex)
			}
			first = false
		} else if s.FirstIndex != prev+1 {
			log.Warn("verify: gap detected",
				"missing_range", fmt.Sprintf("%d..%d", prev+1, s.FirstIndex-1),
				"prev_segment_last", prev, "next_segment_first", s.FirstIndex)
		}
		prev = s.LastIndex
	}
	return nil
}

// walkAndCount opens path (transparent .zst) and counts CRC-valid records.
// Returns count, with error on any frame failure.
func walkAndCount(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var src io.Reader = f
	if filepath.Ext(path) == ".zst" {
		dec, derr := zstd.NewReader(f)
		if derr != nil {
			return 0, derr
		}
		defer dec.Close()
		src = dec
	}
	br := bufio.NewReaderSize(src, 64*1024)
	count := 0
	for {
		_, err := readRecord(br)
		if err == io.EOF {
			return count, nil
		}
		if err != nil {
			return count, err
		}
		count++
	}
}
