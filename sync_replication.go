package axwal

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// SyncReplicationConfig turns on semi-synchronous replication for a Writer.
//
// With it, Append / AppendBatch return only after the record is durable
// locally AND a replica has acknowledged writing it to its own disk — so an
// acknowledged record survives the loss of the primary's disk.
//
// It degrades instead of stalling. Whenever a replica doesn't acknowledge
// within Timeout, or the last replica disconnects, the writer falls back to
// asynchronous replication: appends stop waiting (they are still durable
// locally) and the replica keeps receiving the stream as fast as the link
// allows. Once it has caught up again the writer returns to synchronous
// mode by itself. WriterStats shows the current mode.
//
// Requirements: the writer's own process must run the replication endpoint —
// Serve with ServerOptions.Writer set to this Writer — and the other side
// must be a replica (RunReplica / axwal-mirror -replica). A separate
// axwal-serve process can't report acknowledgements to the writer.
type SyncReplicationConfig struct {
	// Timeout is the longest an append waits for the replica before the
	// writer falls back to asynchronous mode. That one append returns
	// successfully after Timeout; the following ones don't wait at all.
	// Default 1s.
	Timeout time.Duration

	// RecoverLag: synchronous mode resumes once the replica is at most this
	// many records behind. Default 1000.
	RecoverLag uint64

	// Cooldown is the minimum time spent in asynchronous mode after a
	// Timeout fallback, so a link that is merely slow doesn't make the
	// writer flap (and pay Timeout over and over). Default 5s.
	Cooldown time.Duration
}

func (c SyncReplicationConfig) withDefaults() SyncReplicationConfig {
	if c.Timeout <= 0 {
		c.Timeout = time.Second
	}
	if c.RecoverLag == 0 {
		c.RecoverLag = 1000
	}
	if c.Cooldown <= 0 {
		c.Cooldown = 5 * time.Second
	}
	return c
}

// replState is the writer's view of its replicas, fed by Serve. It exists on
// every Writer (for stats); syncing only happens when cfg is set.
type replState struct {
	cfg *SyncReplicationConfig
	log *slog.Logger

	last atomic.Uint64 // writer's last locally durable index

	mu         sync.Mutex
	replicas   int       // connected replica sessions
	acked      uint64    // highest index a replica has on disk
	active     bool      // appends currently wait for acks
	coolUntil  time.Time // no return to sync mode before this
	changed    chan struct{}
	degrades   atomic.Uint64
	syncWaits  atomic.Uint64
	activeFlag atomic.Bool // mirror of active for the lock-free fast path
}

func newReplState(cfg *SyncReplicationConfig, log *slog.Logger) *replState {
	s := &replState{log: log, changed: make(chan struct{})}
	if cfg != nil {
		c := cfg.withDefaults()
		s.cfg = &c
	}
	return s
}

// broadcastLocked wakes every waiter. Caller holds s.mu.
func (s *replState) broadcastLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *replState) setActiveLocked(on bool, why string) {
	if s.active == on {
		return
	}
	s.active = on
	s.activeFlag.Store(on)
	if on {
		s.log.Info("replication: synchronous mode ON", "reason", why, "replica_index", s.acked)
	} else {
		s.degrades.Add(1)
		s.log.Warn("replication: falling back to ASYNCHRONOUS mode", "reason", why,
			"replica_index", s.acked, "last_index", s.last.Load())
	}
	s.broadcastLocked()
}

// maybeRecoverLocked switches sync mode on when a replica is connected and
// close enough to the writer.
func (s *replState) maybeRecoverLocked() {
	if s.cfg == nil || s.active || s.replicas == 0 {
		return
	}
	if time.Now().Before(s.coolUntil) {
		return
	}
	last := s.last.Load()
	if last > s.acked && last-s.acked > s.cfg.RecoverLag {
		return
	}
	s.setActiveLocked(true, "replica caught up")
}

// replicaConnected is called by the server when a replica session starts;
// have is the last index the replica already holds.
func (s *replState) replicaConnected(have uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicas++
	if s.replicas == 1 || have > s.acked {
		// With a single replica its position IS the replicated position,
		// even if it is lower than what a previous session had acked.
		s.acked = have
	}
	s.maybeRecoverLocked()
	s.broadcastLocked()
}

func (s *replState) replicaDisconnected() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicas--
	if s.replicas == 0 {
		s.setActiveLocked(false, "replica disconnected")
	}
}

// replicaAcked is called by the server for every ACK from a replica.
func (s *replState) replicaAcked(idx uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if idx > s.acked {
		s.acked = idx
		s.broadcastLocked()
	}
	s.maybeRecoverLocked()
}

// wait blocks until a replica has acknowledged idx, the writer is (or
// becomes) asynchronous, or ctx ends. It never fails: by now the record is
// durable locally, replication is the extra guarantee.
func (s *replState) wait(ctx context.Context, idx uint64) {
	if !s.activeFlag.Load() {
		return
	}
	s.syncWaits.Add(1)
	t := time.NewTimer(s.cfg.Timeout)
	defer t.Stop()
	for {
		s.mu.Lock()
		if !s.active || s.acked >= idx {
			s.mu.Unlock()
			return
		}
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return
		case <-t.C:
			s.mu.Lock()
			if s.active && s.acked < idx {
				s.coolUntil = time.Now().Add(s.cfg.Cooldown)
				s.setActiveLocked(false, "replica did not acknowledge within "+s.cfg.Timeout.String())
			}
			s.mu.Unlock()
			return
		}
	}
}

func (s *replState) fillStats(st *WriterStats) {
	s.mu.Lock()
	st.ReplicasConnected = s.replicas
	st.ReplicaAckedIndex = s.acked
	st.SyncReplicationActive = s.active
	s.mu.Unlock()
	st.SyncReplicationFallbacks = s.degrades.Load()
	st.SyncReplicationWaits = s.syncWaits.Load()
}
