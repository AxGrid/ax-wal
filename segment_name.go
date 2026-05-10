package axwal

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Segment naming:
//   open    : wal-{F}-           (writer is currently appending)
//   closed  : wal-{F}-{L}        (rotated, F..L inclusive)
//   compress: wal-{F}-{L}.zst    (after async zstd)
//
// Empty closed segment uses L = F-1 (i.e. zero records). It's allowed but
// shouldn't normally occur.

var (
	// Compression suffix excludes ".archived" so segment_name treats sidecars as
	// non-segment files (errBadName).
	closedRe = regexp.MustCompile(`^wal-(\d+)-(\d+)(\.(?:zst|gz))?$`)
	openRe   = regexp.MustCompile(`^wal-(\d+)-(\.(?:zst|gz))?$`)
)

var errBadName = errors.New("not a wal segment filename")

type segName struct {
	Name        string // basename
	FirstIndex  uint64
	LastIndex   uint64 // valid iff Closed
	Closed      bool
	Compression string // "zst" or ""
}

func parseSegName(name string) (segName, error) {
	out := segName{Name: name}

	if m := closedRe.FindStringSubmatch(name); m != nil {
		first, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil || first == 0 {
			return out, fmt.Errorf("%w: bad first index %q", errBadName, m[1])
		}
		last, err := strconv.ParseUint(m[2], 10, 64)
		if err != nil {
			return out, fmt.Errorf("%w: bad last index %q", errBadName, m[2])
		}
		if last+1 < first {
			return out, fmt.Errorf("%w: last %d < first-1 %d", errBadName, last, first-1)
		}
		out.FirstIndex = first
		out.LastIndex = last
		out.Closed = true
		if m[3] != "" {
			out.Compression = strings.TrimPrefix(m[3], ".")
		}
		return out, nil
	}

	if m := openRe.FindStringSubmatch(name); m != nil {
		first, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil || first == 0 {
			return out, fmt.Errorf("%w: bad first index %q", errBadName, m[1])
		}
		out.FirstIndex = first
		out.Closed = false
		if m[2] != "" {
			out.Compression = strings.TrimPrefix(m[2], ".")
		}
		return out, nil
	}

	return out, errBadName
}

func formatOpenName(first uint64) string {
	return fmt.Sprintf("wal-%d-", first)
}

func formatClosedName(first, last uint64) string {
	return fmt.Sprintf("wal-%d-%d", first, last)
}
