package axqcompat

import (
	"os"
	"regexp"
	"sort"
	"strconv"
)

// ax-wal segments are named one of:
//
//   wal-{F}-          (open / currently-being-appended)
//   wal-{F}-{L}       (closed)
//   wal-{F}-{L}.zst   (closed, post-compression)
//
// F is the first record index in the segment. listSegmentFIDs returns the
// distinct F values found in `dir`, sorted ascending. The set is small
// (handful to a few hundred segments under normal retention) so a fresh
// os.ReadDir per call is cheap and avoids stale-cache footguns.
var segNameRe = regexp.MustCompile(`^wal-(\d+)-(\d+)?(?:\.(?:zst|gz))?$`)

func listSegmentFIDs(dir string) ([]uint64, error) {
	if dir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	seen := make(map[uint64]struct{}, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := segNameRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		v, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			continue
		}
		seen[v] = struct{}{}
	}
	out := make([]uint64, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// minMaxSegmentFID returns the smallest and largest F seen in `dir`. (0, 0)
// when no segments exist (fresh empty WAL dir).
func minMaxSegmentFID(dir string) (uint64, uint64, error) {
	ids, err := listSegmentFIDs(dir)
	if err != nil || len(ids) == 0 {
		return 0, 0, err
	}
	return ids[0], ids[len(ids)-1], nil
}
