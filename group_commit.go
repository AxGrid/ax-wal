package axwal

import (
	"time"
)

// commitLoop drives group-commit. It waits up to MaxLatency for a kick (or a
// timer tick) and then performs a single Flush+Sync covering all data that
// arrived during the window. All Append callers waiting on commitIndex are
// then woken up.
func (w *Writer) commitLoop() {
	defer w.gcWG.Done()
	t := time.NewTicker(w.opts.GroupCommit.MaxLatency)
	defer t.Stop()
	for {
		select {
		case <-w.gcStop:
			return
		case <-w.gcKick:
		case <-t.C:
		}

		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return
		}
		if w.pendingBytes == 0 {
			w.mu.Unlock()
			continue
		}
		want := w.lastIndex
		if err := w.flushAndSyncLocked(); err != nil {
			w.log.Error("group-commit fsync failed", "err", err)
			// Don't advance commitIndex; waiters will keep waiting and the
			// next tick will retry.
			w.mu.Unlock()
			continue
		}
		w.commitIndex = want
		w.gcCond.Broadcast()
		w.mu.Unlock()
	}
}

// rotateAgeLoop fires Rotate() when the active segment exceeds MaxSegmentAge.
// Cheap polling (every MaxSegmentAge/4, capped 1s..5s) is fine here: rotation
// is rare relative to writes.
func (w *Writer) rotateAgeLoop() {
	defer w.rotateAgeWG.Done()
	period := w.opts.MaxSegmentAge / 4
	switch {
	case period < time.Second:
		period = time.Second
	case period > 5*time.Second:
		period = 5 * time.Second
	}
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-w.rotateAgeStop:
			return
		case <-t.C:
			w.mu.Lock()
			if w.closed || w.file == nil || w.curRecords == 0 {
				w.mu.Unlock()
				continue
			}
			if time.Since(w.openedAt) >= w.opts.MaxSegmentAge {
				if err := w.rotateLocked(); err != nil {
					w.log.Error("age-based rotation failed", "err", err)
				}
			}
			w.mu.Unlock()
		}
	}
}
