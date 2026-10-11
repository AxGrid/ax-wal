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

// rotateAgeLoop fires rotations that depend on the clock rather than on
// writes: MaxSegmentAge (segment open for too long) and RotateAligned (a
// wall-clock boundary passed, so an idle segment doesn't stay open into the
// next window). Cheap polling is fine here: rotation is rare relative to
// writes.
func (w *Writer) rotateAgeLoop() {
	defer w.rotateAgeWG.Done()
	period := 5 * time.Second
	if age := w.opts.MaxSegmentAge; age > 0 && age/4 < period {
		period = age / 4
	}
	if period < time.Second || w.opts.RotateAligned > 0 {
		period = time.Second
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
			now := w.opts.nowFn()
			byAge := w.opts.MaxSegmentAge > 0 && now.Sub(w.openedAt) >= w.opts.MaxSegmentAge
			if byAge || w.crossedBoundaryLocked(now) {
				if err := w.rotateLocked(); err != nil {
					w.log.Error("time-based rotation failed", "err", err)
				}
			}
			w.mu.Unlock()
		}
	}
}
