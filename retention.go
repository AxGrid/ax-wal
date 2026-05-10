package axwal

import (
	"os"
	"path/filepath"
	"sort"
	"time"
)

// janitorLoop periodically attempts to delete closed segments whose data is no
// longer needed by any active reader cursor and that fall outside MinRetainRecords.
func (w *Writer) janitorLoop() {
	defer w.janitorWG.Done()
	t := time.NewTicker(w.opts.Retention.Interval)
	defer t.Stop()
	for {
		select {
		case <-w.janitorStop:
			return
		case <-t.C:
			if err := w.runRetention(); err != nil {
				w.log.Error("retention pass", "err", err)
			}
		}
	}
}

// runRetention executes a single retention pass.
//
// Algorithm:
//   1. Take a snapshot of the current lastIndex (under writer mu).
//   2. Read all live cursors; exclude those whose heartbeat is older than StaleReaderTTL.
//   3. minActive = min(cursor.index) over live cursors. If no live cursors, set
//      minActive = lastIndex (nothing held back by readers).
//   4. retainFloor = lastIndex - MinRetainRecords (saturating). Records with
//      index <= retainFloor MAY be deleted; records above must be kept.
//   5. cutoff = min(minActive, retainFloor). For each closed segment with
//      LastIndex < cutoff, delete it.
//
// We never touch the active open segment.
func (w *Writer) runRetention() error {
	w.mu.Lock()
	last := w.lastIndex
	curPath := w.curPath
	w.mu.Unlock()

	// Step 2: live cursors.
	cursors, err := loadAllCursors(w.dir)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	ttl := int64(w.opts.Retention.StaleReaderTTL / time.Second)
	var minActive uint64
	haveActive := false
	for _, c := range cursors {
		if now-c.LastHeartbeatUnix > ttl {
			continue
		}
		if !haveActive || c.Index < minActive {
			minActive = c.Index
			haveActive = true
		}
	}
	if !haveActive {
		minActive = last
	}

	// Step 4: floor from MinRetainRecords.
	var retainFloor uint64
	if last > w.opts.Retention.MinRetainRecords {
		retainFloor = last - w.opts.Retention.MinRetainRecords
	}

	cutoff := minActive
	if retainFloor < cutoff {
		cutoff = retainFloor
	}
	if cutoff == 0 {
		return nil
	}

	// Step 5: scan and delete eligible closed segments.
	matches, err := filepath.Glob(filepath.Join(w.dir, "wal-*"))
	if err != nil {
		return err
	}
	type item struct {
		path string
		seg  segName
	}
	var items []item
	for _, p := range matches {
		base := filepath.Base(p)
		if base == writerLockFile {
			continue
		}
		s, err := parseSegName(base)
		if err != nil || !s.Closed {
			continue
		}
		if p == curPath {
			continue
		}
		items = append(items, item{p, s})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].seg.LastIndex < items[j].seg.LastIndex })

	for _, it := range items {
		// We treat "<= cutoff-1" as deletable: a segment with LastIndex == cutoff
		// still contains the record at index `cutoff` itself, which a reader at
		// position `cutoff` (next-to-read) might want.
		if it.seg.LastIndex >= cutoff {
			break
		}
		// Archive gate: if configured, only delete once an external uploader
		// has marked this segment as archived.
		if w.opts.Retention.RequireArchivedMark && !isArchived(w.dir, it.seg) {
			continue
		}
		if err := os.Remove(it.path); err != nil && !os.IsNotExist(err) {
			w.log.Error("retention: remove failed", "name", it.seg.Name, "err", err)
			continue
		}
		// Clean up the sidecar if it existed.
		_ = removeArchivedSidecar(w.dir, it.seg)
		w.cnt.retentionDeletes.Add(1)
		w.log.Info("retention: deleted segment",
			"name", it.seg.Name, "last_index", it.seg.LastIndex, "cutoff", cutoff)
	}
	return fsyncDir(w.dir)
}
