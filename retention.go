package axwal

import (
	"os"
	"path/filepath"
	"sort"
	"time"
)

// janitorLoop periodically attempts to delete closed segments whose data is no
// longer needed by any active reader cursor and that fall outside MinRetainRecords.
// Besides the ticker it runs whenever a segment finishes rotating or gets
// archived, so disk limits react to growth without waiting for the interval.
func (w *Writer) janitorLoop() {
	defer w.janitorWG.Done()
	t := time.NewTicker(w.opts.Retention.Interval)
	defer t.Stop()
	for {
		select {
		case <-w.janitorStop:
			return
		case <-t.C:
		case <-w.janitorKick:
		}
		if err := w.runRetention(); err != nil {
			w.log.Error("retention pass", "err", err)
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
//   5. cutoff = min(minActive, retainFloor). A closed segment with
//      LastIndex < cutoff is deleted once it is also older than MinRetainAge.
//      Independently, MaxRetainAge deletes by age alone.
//   6. If the directory is over MaxTotalBytes / under MinFreeBytes, evict the
//      oldest segments that no live reader needs, ignoring MinRetainAge and
//      MinRetainRecords.
//
// Steps 5 and 6 never delete a segment that still waits for its .archived
// mark when one is required. We never touch the active open segment.
func (w *Writer) runRetention() error {
	rc := w.opts.Retention

	w.mu.Lock()
	last := w.lastIndex
	curPath := w.curPath
	w.mu.Unlock()

	// Step 2: live cursors.
	cursors, err := loadAllCursors(w.dir)
	if err != nil {
		return err
	}
	now := time.Now()
	ttl := int64(rc.StaleReaderTTL / time.Second)
	var minActive uint64
	haveActive := false
	for _, c := range cursors {
		if now.Unix()-c.LastHeartbeatUnix > ttl {
			continue
		}
		if !haveActive || c.Index < minActive {
			minActive = c.Index
			haveActive = true
		}
	}
	readerFloor := minActive
	if !haveActive {
		minActive = last
	}

	// Step 4: floor from MinRetainRecords.
	var retainFloor uint64
	if last > rc.MinRetainRecords {
		retainFloor = last - rc.MinRetainRecords
	}

	cutoff := minActive
	if retainFloor < cutoff {
		cutoff = retainFloor
	}

	// MaxRetainAge: any segment older than this is also eligible for deletion,
	// regardless of cursors. Computed once per pass.
	var ageDeadline, minAgeDeadline time.Time
	if rc.MaxRetainAge > 0 {
		ageDeadline = now.Add(-rc.MaxRetainAge)
	}
	if rc.MinRetainAge > 0 {
		minAgeDeadline = now.Add(-rc.MinRetainAge)
	}
	diskLimits := rc.MaxTotalBytes > 0 || rc.MinFreeBytes > 0

	if cutoff == 0 && ageDeadline.IsZero() && !diskLimits {
		return nil
	}

	// Scan closed segments (deletion candidates) and add up everything the
	// WAL occupies, including the open segment.
	matches, err := filepath.Glob(filepath.Join(w.dir, "wal-*"))
	if err != nil {
		return err
	}
	type item struct {
		path string
		seg  segName
		mod  time.Time
		size int64
	}
	var (
		items      []item
		totalBytes int64
	)
	for _, p := range matches {
		base := filepath.Base(p)
		if base == writerLockFile {
			continue
		}
		s, err := parseSegName(base)
		if err != nil {
			continue
		}
		fi, statErr := os.Stat(p)
		if statErr != nil {
			continue
		}
		totalBytes += fi.Size()
		if !s.Closed || p == curPath {
			continue
		}
		items = append(items, item{p, s, fi.ModTime(), fi.Size()})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].seg.LastIndex < items[j].seg.LastIndex })

	remove := func(it item, reason string) bool {
		if err := os.Remove(it.path); err != nil && !os.IsNotExist(err) {
			w.log.Error("retention: remove failed", "name", it.seg.Name, "err", err)
			return false
		}
		_ = removeArchivedSidecar(w.dir, it.seg)
		_ = removeMetaSidecar(w.dir, it.seg.FirstIndex, it.seg.LastIndex)
		totalBytes -= it.size
		w.cnt.retentionDeletes.Add(1)
		w.log.Info("retention: deleted segment",
			"name", it.seg.Name, "last_index", it.seg.LastIndex,
			"cutoff", cutoff, "age", now.Sub(it.mod), "reason", reason)
		return true
	}
	// Archive gate: if configured, only delete once the segment has been
	// marked as archived. Applies regardless of WHY the segment is eligible.
	archiveOK := func(it item) bool {
		return !rc.RequireArchivedMark || isArchived(w.dir, it.seg)
	}

	// Step 5: regular retention.
	kept := items[:0]
	for _, it := range items {
		// Two independent reasons to delete this segment:
		//   1. cursor/MinRetainRecords cutoff has moved past it and it has
		//      been kept for MinRetainAge
		//   2. MaxRetainAge fires (compliance-style hard purge)
		eligibleByCursor := cutoff > 0 && it.seg.LastIndex < cutoff &&
			(minAgeDeadline.IsZero() || it.mod.Before(minAgeDeadline))
		eligibleByAge := !ageDeadline.IsZero() && it.mod.Before(ageDeadline)
		if (eligibleByCursor || eligibleByAge) && archiveOK(it) {
			reason := "cursor"
			if eligibleByAge && !eligibleByCursor {
				reason = "age"
			} else if eligibleByAge {
				reason = "cursor+age"
			}
			if remove(it, reason) {
				continue
			}
		}
		kept = append(kept, it)
	}
	items = kept

	// Step 6: disk pressure.
	if diskLimits {
		over := func() bool {
			if rc.MaxTotalBytes > 0 && totalBytes > rc.MaxTotalBytes {
				return true
			}
			if rc.MinFreeBytes > 0 {
				if free, err := diskFree(w.dir); err == nil && free < rc.MinFreeBytes {
					return true
				}
			}
			return false
		}
		for _, it := range items {
			if !over() {
				break
			}
			// Oldest first. Everything from the first segment a live reader
			// still needs onwards stays.
			if haveActive && it.seg.LastIndex >= readerFloor {
				break
			}
			if !archiveOK(it) {
				continue
			}
			remove(it, "disk-pressure")
		}
		full := over()
		if full && !w.diskFull.Load() {
			w.log.Error("retention: disk limit exceeded and nothing can be deleted",
				"total_bytes", totalBytes, "max_total_bytes", rc.MaxTotalBytes,
				"min_free_bytes", rc.MinFreeBytes)
		}
		w.diskFull.Store(full)
	}
	return fsyncDir(w.dir)
}
