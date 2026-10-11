package axwal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Cursor on-disk format:
//   [4B "AXC1" magic][4B BE crc32c of payload][N bytes JSON payload]
// Legacy (pre-CRC) cursors stored a raw JSON object; readCursorFile detects
// missing magic and accepts those for backward compatibility, with a warning.
var cursorMagic = [4]byte{'A', 'X', 'C', '1'}

// ErrCorruptCursor indicates a cursor file failed CRC validation. Reader
// callers can decide what to do: bail out (default) or fast-forward.
var ErrCorruptCursor = errors.New("axwal: cursor CRC mismatch")

// cursorsSubdir holds one file per registered reader.
const cursorsSubdir = "cursors"

// cursorRecord is the persisted on-disk format of a reader cursor.
// Index is the index of the LAST acknowledged record (i.e. processing has
// completed up to and including this index). On reader restart it resumes at
// Index+1.
type cursorRecord struct {
	Name              string `json:"name"`
	Index             uint64 `json:"index"`
	PID               int    `json:"pid"`
	StartedAtUnix     int64  `json:"started_at_unix"`
	LastHeartbeatUnix int64  `json:"last_heartbeat_unix"`
}

// cursorState manages a reader's persisted position.
//
// The cursor file lives at <dir>/cursors/<name>.cursor and is updated atomically
// via tmp+rename+fsync. A heartbeat goroutine refreshes LastHeartbeatUnix on a
// timer so that the writer's janitor can tell live readers from dead ones.
type cursorState struct {
	dir       string // WAL root
	name      string
	path      string
	tmpPath   string
	lockFile  *os.File // exclusive flock so two processes can't share the same cursor name
	startedAt int64

	mu  sync.Mutex
	rec cursorRecord

	// hbIndex is the most recent index seen by Ack(). The heartbeat / batch-ack
	// goroutine periodically reads this and persists it.
	hbIndex atomic.Uint64

	// batchAck, when true, makes Ack() update hbIndex only (no fsync). The
	// heartbeat goroutine fsyncs at its own cadence. Trade-off: up to
	// HeartbeatInterval of records may be re-delivered after a reader crash.
	batchAck bool

	stop chan struct{}
	wg   sync.WaitGroup
}

func openCursor(dir, name string) (*cursorState, error) {
	if name == "" || strings.ContainsAny(name, "/\\") {
		return nil, errors.New("axwal: cursor name must be non-empty and have no path separators")
	}
	cdir := filepath.Join(dir, cursorsSubdir)
	if err := os.MkdirAll(cdir, 0755); err != nil {
		return nil, err
	}
	path := filepath.Join(cdir, name+".cursor")
	tmp := path + ".tmp"

	// flock on a sibling .lock file to prevent two readers of the same name.
	lockPath := filepath.Join(cdir, name+".lock")
	lf, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open cursor lock: %w", err)
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lf.Close()
		if err == syscall.EWOULDBLOCK {
			return nil, fmt.Errorf("axwal: cursor %q is held by another reader", name)
		}
		return nil, fmt.Errorf("flock cursor: %w", err)
	}

	cs := &cursorState{
		dir:       dir,
		name:      name,
		path:      path,
		tmpPath:   tmp,
		lockFile:  lf,
		startedAt: time.Now().Unix(),
	}

	// Load existing cursor record if present.
	rec, ok, err := readCursorFile(path)
	if err != nil {
		_ = cs.releaseLock()
		return nil, err
	}
	if ok {
		cs.rec = rec
	} else {
		cs.rec = cursorRecord{Name: name}
	}
	cs.rec.PID = os.Getpid()
	cs.rec.StartedAtUnix = cs.startedAt
	cs.rec.LastHeartbeatUnix = cs.startedAt
	cs.hbIndex.Store(cs.rec.Index)

	// Persist the initial heartbeat so the janitor sees us as alive immediately.
	if err := cs.persist(cs.rec); err != nil {
		_ = cs.releaseLock()
		return nil, err
	}
	return cs, nil
}

// startHeartbeat launches the background goroutine that refreshes
// LastHeartbeatUnix every interval/2 (or 1s, whichever is smaller).
// staleTTL is informational only: callers wanting freshness < ttl should pick
// interval accordingly.
func (cs *cursorState) startHeartbeat(interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	cs.stop = make(chan struct{})
	cs.wg.Add(1)
	go cs.heartbeatLoop(interval)
}

func (cs *cursorState) heartbeatLoop(interval time.Duration) {
	defer cs.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-cs.stop:
			return
		case <-t.C:
			cs.mu.Lock()
			rec := cs.rec
			rec.Index = cs.hbIndex.Load()
			rec.LastHeartbeatUnix = time.Now().Unix()
			err := cs.persist(rec)
			if err == nil {
				cs.rec = rec
			}
			cs.mu.Unlock()
		}
	}
}

// Ack persists index as the new acknowledged position. When batchAck is set,
// the persist+fsync is deferred to the heartbeat goroutine; otherwise this
// call returns only after the index is durably saved.
func (cs *cursorState) Ack(index uint64) error {
	if cs.batchAck {
		// Lock-free fast path: update hbIndex monotonically.
		for {
			cur := cs.hbIndex.Load()
			if index < cur {
				return fmt.Errorf("axwal: cursor %s ack %d < current %d", cs.name, index, cur)
			}
			if cs.hbIndex.CompareAndSwap(cur, index) {
				return nil
			}
		}
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	rec := cs.rec
	if index < rec.Index {
		return fmt.Errorf("axwal: cursor %s ack %d < current %d", cs.name, index, rec.Index)
	}
	rec.Index = index
	rec.LastHeartbeatUnix = time.Now().Unix()
	if err := cs.persist(rec); err != nil {
		return err
	}
	cs.rec = rec
	cs.hbIndex.Store(index)
	return nil
}

// reset moves the cursor to index unconditionally (it may go backwards).
// Only valid before the heartbeat goroutine is started.
func (cs *cursorState) reset(index uint64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	rec := cs.rec
	rec.Index = index
	if err := cs.persist(rec); err != nil {
		return err
	}
	cs.rec = rec
	cs.hbIndex.Store(index)
	return nil
}

// CurrentIndex returns the most recently Ack'd index.
func (cs *cursorState) CurrentIndex() uint64 {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.rec.Index
}

// persist writes rec atomically: tmp -> fsync -> rename -> fsync(dir).
// Format: [magic][crc32c(body)][JSON body].
func (cs *cursorState) persist(rec cursorRecord) error {
	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	frame := make([]byte, 8+len(body))
	copy(frame[0:4], cursorMagic[:])
	binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(body, crcTable))
	copy(frame[8:], body)

	f, err := os.OpenFile(cs.tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(frame); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(cs.tmpPath, cs.path); err != nil {
		return err
	}
	return fsyncDir(filepath.Dir(cs.path))
}

func (cs *cursorState) Close() error {
	if cs.stop != nil {
		close(cs.stop)
		cs.wg.Wait()
		cs.stop = nil
	}
	// Final flush so a batched-ack reader doesn't lose the most recently Ack'd
	// index on a clean shutdown.
	cs.mu.Lock()
	if cs.hbIndex.Load() > cs.rec.Index {
		rec := cs.rec
		rec.Index = cs.hbIndex.Load()
		rec.LastHeartbeatUnix = time.Now().Unix()
		if err := cs.persist(rec); err == nil {
			cs.rec = rec
		}
	}
	cs.mu.Unlock()
	return cs.releaseLock()
}

func (cs *cursorState) releaseLock() error {
	if cs.lockFile == nil {
		return nil
	}
	_ = syscall.Flock(int(cs.lockFile.Fd()), syscall.LOCK_UN)
	closeErr := cs.lockFile.Close()
	cs.lockFile = nil
	// We deliberately leave .lock file on disk; recreating it is cheap and
	// removing it racily during restart would risk losing exclusivity.
	return closeErr
}

// readCursorFile parses either the new framed format (magic+crc+body) or a
// legacy raw-JSON cursor. Returns (rec, exists, error). Returns ErrCorruptCursor
// only when the framed format's CRC fails — JSON parse errors fall through to a
// generic error so callers can distinguish "cursor was corrupted by torn write"
// from "cursor file is total garbage".
func readCursorFile(path string) (cursorRecord, bool, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cursorRecord{}, false, nil
	}
	if err != nil {
		return cursorRecord{}, false, err
	}

	// Framed format starts with our magic.
	if len(b) >= 8 && b[0] == cursorMagic[0] && b[1] == cursorMagic[1] && b[2] == cursorMagic[2] && b[3] == cursorMagic[3] {
		want := binary.BigEndian.Uint32(b[4:8])
		body := b[8:]
		got := crc32.Checksum(body, crcTable)
		if got != want {
			return cursorRecord{}, false, fmt.Errorf("%w (%s): want %08x got %08x", ErrCorruptCursor, path, want, got)
		}
		var rec cursorRecord
		if err := json.Unmarshal(body, &rec); err != nil {
			return cursorRecord{}, false, fmt.Errorf("parse framed cursor %s: %w", path, err)
		}
		return rec, true, nil
	}

	// Legacy cursors written before CRC framing was introduced: raw JSON.
	var rec cursorRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return cursorRecord{}, false, fmt.Errorf("parse cursor %s: %w", path, err)
	}
	return rec, true, nil
}

// loadAllCursors reads every *.cursor file in <dir>/cursors and returns the
// parsed records. Lock files and tmp files are ignored.
func loadAllCursors(dir string) ([]cursorRecord, error) {
	cdir := filepath.Join(dir, cursorsSubdir)
	entries, err := os.ReadDir(cdir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []cursorRecord
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".cursor") {
			continue
		}
		rec, ok, err := readCursorFile(filepath.Join(cdir, name))
		if err != nil {
			// Skip unreadable cursors: treat as non-existent for retention purposes.
			continue
		}
		if ok {
			out = append(out, rec)
		}
	}
	return out, nil
}
