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

	// MaxRetainAge: any segment older than this is also eligible for deletion,
	// regardless of cursors. Computed once per pass.
	var ageDeadline time.Time
	if w.opts.Retention.MaxRetainAge > 0 {
		ageDeadline = time.Now().Add(-w.opts.Retention.MaxRetainAge)
	}

	if cutoff == 0 && ageDeadline.IsZero() {
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
		mod  time.Time
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
		fi, statErr := os.Stat(p)
		if statErr != nil {
			continue
		}
		items = append(items, item{p, s, fi.ModTime()})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].seg.LastIndex < items[j].seg.LastIndex })

	for _, it := range items {
		// Two independent reasons to delete this segment:
		//   1. cursor/MinRetainRecords cutoff has moved past it
		//   2. MaxRetainAge fires (compliance-style hard purge)
		eligibleByCursor := cutoff > 0 && it.seg.LastIndex < cutoff
		eligibleByAge := !ageDeadline.IsZero() && it.mod.Before(ageDeadline)
		if !eligibleByCursor && !eligibleByAge {
			// Cursor list is sorted ascending, so once we hit a non-cursor-
			// eligible segment we can stop — but only if AGE retention isn't
			// also active, since the age check is independent of order.
			if ageDeadline.IsZero() {
				break
			}
			continue
		}
		// Archive gate: if configured, only delete once an external uploader
		// has marked this segment as archived. Applies regardless of WHY the
		// segment is eligible.
		if w.opts.Retention.RequireArchivedMark && !isArchived(w.dir, it.seg) {
			continue
		}
		if err := os.Remove(it.path); err != nil && !os.IsNotExist(err) {
			w.log.Error("retention: remove failed", "name", it.seg.Name, "err", err)
			continue
		}
		_ = removeArchivedSidecar(w.dir, it.seg)
		_ = removeMetaSidecar(w.dir, it.seg.FirstIndex, it.seg.LastIndex)
		w.cnt.retentionDeletes.Add(1)
		reason := "cursor"
		if eligibleByAge && !eligibleByCursor {
			reason = "age"
		} else if eligibleByAge {
			reason = "cursor+age"
		}
		w.log.Info("retention: deleted segment",
			"name", it.seg.Name, "last_index", it.seg.LastIndex,
			"cutoff", cutoff, "age", time.Since(it.mod), "reason", reason)
	}
	return fsyncDir(w.dir)
}
