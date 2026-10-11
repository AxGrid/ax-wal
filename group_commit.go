package axwal

import (
	"time"
)

// commitLoop drives group-commit. Whenever records are pending it flushes the
// buffer and fsyncs, then wakes every Append whose records that fsync covered.
//
// The fsync runs WITHOUT the writer lock, so appenders keep adding records to
// the buffer while the disk is busy; they are picked up by the next cycle.
// The batch size therefore adapts by itself: on a slow disk many appends pile
// up per fsync, on a fast one the loop simply spins faster. There is no
// artificial wait — an append is committed as soon as the disk allows. The
// MaxLatency ticker only remains as a safety net for a missed kick.
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
		if w.pendingBytes == 0 || w.file == nil {
			w.mu.Unlock()
			continue
		}
		if err := w.buf.Flush(); err != nil {
			w.log.Error("group-commit flush failed", "err", err)
			w.mu.Unlock()
			continue
		}
		// Everything up to `want` is now in the file; `syncing` bytes of the
		// pending count are what this fsync will make durable.
		want, syncing, file := w.lastIndex, w.pendingBytes, w.file
		w.mu.Unlock()

		err := w.syncFile(file)

		w.mu.Lock()
		switch {
		case w.file != file:
			// The segment was rotated (or the writer closed) meanwhile. That
			// path does its own flush+sync and commit; our fsync may even
			// have hit the closed file. Nothing left to do for this batch.
		case err != nil:
			w.log.Error("group-commit fsync failed", "err", err)
			// Don't advance commitIndex; waiters keep waiting and the next
			// tick retries.
		default:
			w.pendingBytes -= syncing
			if want > w.commitIndex {
				w.commitIndex = want
			}
			w.gcCond.Broadcast()
		}
		more := !w.closed && w.pendingBytes > 0
		w.mu.Unlock()
		if more {
			w.kickCommit()
		}
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
