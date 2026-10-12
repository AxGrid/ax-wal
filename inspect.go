package axwal

import (
	"os"
	"path/filepath"
	"sort"
	"time"
)

// CursorInfo is a reader's registration as found on disk.
type CursorInfo struct {
	// Name is the reader's cursor name.
	Name string
	// Index is the last record the reader acknowledged (0: none yet). With
	// AckBatchInterval it trails the reader's real position by up to that
	// interval; a reader that never calls Ack stays at its starting point.
	Index uint64
	// PID of the process that last opened the cursor (on its own host).
	PID int
	// StartedAt is when that process opened it.
	StartedAt time.Time
	// LastHeartbeat is the last sign of life. A reader whose heartbeat is
	// older than the writer's Retention.StaleReaderTTL no longer holds back
	// retention.
	LastHeartbeat time.Time
}

// ListCursors returns every reader registered in a WAL directory, sorted by
// name. It only reads the cursor files: it takes no locks, registers nothing
// and is safe to call next to a live writer and live readers.
func ListCursors(dir string) ([]CursorInfo, error) {
	recs, err := loadAllCursors(dir)
	if err != nil {
		return nil, err
	}
	out := make([]CursorInfo, 0, len(recs))
	for _, r := range recs {
		out = append(out, CursorInfo{
			Name:          r.Name,
			Index:         r.Index,
			PID:           r.PID,
			StartedAt:     time.Unix(r.StartedAtUnix, 0),
			LastHeartbeat: time.Unix(r.LastHeartbeatUnix, 0),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// LogInfo is a snapshot of what a WAL directory holds.
type LogInfo struct {
	// FirstIndex / LastIndex: the oldest and newest record present (both 0
	// for an empty log). LastIndex includes the open segment.
	FirstIndex uint64
	LastIndex  uint64
	// Segments counts segment files (closed and open); Bytes is their size.
	Segments int
	Bytes    int64
}

// InspectLog reports the index range and size of a WAL directory without
// registering a reader. Reading the tail of the open segment is the only
// non-trivial cost.
func InspectLog(dir string) (LogInfo, error) {
	var info LogInfo
	segs, err := listSegmentsIn(dir)
	if err != nil || len(segs) == 0 {
		return info, err
	}
	info.FirstIndex = segs[0].FirstIndex
	for _, s := range segs {
		if fi, err := os.Stat(filepath.Join(dir, s.Name)); err == nil {
			info.Segments++
			info.Bytes += fi.Size()
		}
	}
	info.LastIndex, err = lastIndexOf(dir, segs)
	if info.LastIndex < info.FirstIndex {
		// Only an empty open segment: nothing written yet.
		info.FirstIndex = 0
		info.LastIndex = 0
	}
	return info, err
}
