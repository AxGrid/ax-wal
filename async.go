package axwal

import (
	"time"
)

// AppendAsync queues payload for a background write and returns immediately —
// it never waits for disk. Use it for telemetry-style logs where the caller
// (an API handler) must not be delayed by fsync.
//
// The queue is bounded by WriterOptions.AsyncQueueSize (and AsyncQueueBytes).
// When it is full the record is DROPPED: AppendAsync returns ErrQueueFull and
// WriterStats.Dropped is incremented. A nil return means "queued", not
// "durable": records still in the queue are lost on a crash, and a failed
// background write drops its batch (counted in Dropped and AsyncErrors).
// Close() writes out everything that is queued.
//
// The writer keeps the slice: the caller must not modify payload afterwards.
// Order is preserved among AppendAsync calls from one goroutine; relative to
// concurrent synchronous Append calls it is not defined.
func (w *Writer) AppendAsync(payload []byte) error {
	if w.asyncCh == nil {
		return ErrAsyncDisabled
	}
	if len(payload) > maxRecordSize {
		return ErrRecordTooLarge
	}
	w.asyncMu.RLock()
	defer w.asyncMu.RUnlock()
	if w.asyncClosed {
		return ErrWriterClosed
	}
	size := int64(len(payload))
	if limit := w.opts.AsyncQueueBytes; limit > 0 && w.asyncBytes.Load()+size > limit {
		w.cnt.dropped.Add(1)
		return ErrQueueFull
	}
	select {
	case w.asyncCh <- payload:
		w.asyncBytes.Add(size)
		return nil
	default:
		w.cnt.dropped.Add(1)
		return ErrQueueFull
	}
}

// asyncLoop drains the async queue into AppendBatch calls. It exits when
// stopAsync closes the channel and everything queued has been written.
func (w *Writer) asyncLoop() {
	defer w.asyncWG.Done()
	var lastLog time.Time
	batch := make([][]byte, 0, w.opts.AsyncBatchSize)
	for first := range w.asyncCh {
		batch = append(batch[:0], first)
		size := int64(len(first))
	fill:
		for len(batch) < w.opts.AsyncBatchSize {
			select {
			case p, ok := <-w.asyncCh:
				if !ok {
					break fill
				}
				batch = append(batch, p)
				size += int64(len(p))
			default:
				break fill
			}
		}
		w.asyncBytes.Add(-size)

		// A nil-error return with no indices is DiskFullDrop, which has
		// already counted the records as dropped.
		if _, err := w.AppendBatch(batch); err != nil {
			w.cnt.asyncErrors.Add(1)
			w.cnt.dropped.Add(uint64(len(batch)))
			if time.Since(lastLog) > 5*time.Second {
				lastLog = time.Now()
				w.log.Error("async append failed; records dropped",
					"records", len(batch), "err", err)
			}
		}
		for i := range batch {
			batch[i] = nil // don't pin payloads until the next batch
		}
	}
}

// stopAsync stops accepting AppendAsync calls and waits for the queue to be
// written out. Idempotent.
func (w *Writer) stopAsync() {
	if w.asyncCh == nil {
		return
	}
	w.asyncOnce.Do(func() {
		w.asyncMu.Lock()
		w.asyncClosed = true
		close(w.asyncCh)
		w.asyncMu.Unlock()
	})
	w.asyncWG.Wait()
}
