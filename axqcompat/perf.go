package axqcompat

import (
	"sync/atomic"
	"time"
)

// rateCounter is a tiny rolling rec/s counter. A background goroutine
// snapshots the running total every second and records the delta as the
// "current rate". Cheap (one atomic per Inc, one ticker per counter) so we
// can attach one to every Writer / Reader without measurable overhead.
type rateCounter struct {
	total atomic.Uint64
	rate  atomic.Uint64

	closing chan struct{}
	done    chan struct{}
}

func newRateCounter() *rateCounter {
	rc := &rateCounter{
		closing: make(chan struct{}),
		done:    make(chan struct{}),
	}
	go rc.loop()
	return rc
}

func (rc *rateCounter) loop() {
	defer close(rc.done)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	var prev uint64
	for {
		select {
		case <-rc.closing:
			return
		case <-t.C:
			cur := rc.total.Load()
			rc.rate.Store(cur - prev)
			prev = cur
		}
	}
}

// Inc records one event.
func (rc *rateCounter) Inc() { rc.total.Add(1) }

// Add records n events at once (useful for batch operations).
func (rc *rateCounter) Add(n uint64) { rc.total.Add(n) }

// Rate returns the rec/s observed in the last completed 1-second window.
// Zero is returned during the first second after construction.
func (rc *rateCounter) Rate() uint64 { return rc.rate.Load() }

// Stop tears down the background goroutine. Idempotent — safe to call
// multiple times only if you implement that yourself; rateCounter does
// NOT guard against double-close (Writer/Reader.Close() already do).
func (rc *rateCounter) Stop() {
	close(rc.closing)
	<-rc.done
}
