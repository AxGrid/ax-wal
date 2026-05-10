package axqcompat

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	axwal "github.com/axgrid/ax-wal"
)

// TestPushPopThroughAxqSurface drives one end-to-end flow against the
// file-system WAL: push, pop, Done, and verify the inspector methods.
// Forces a rotation (MaxSegmentRecords=20) so we end up with multiple
// segments and FID-related methods are exercised against real data.
func TestPushPopThroughAxqSurface(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	rawW, err := axwal.OpenWriter(axwal.WriterOptions{
		Dir:               dir,
		MaxSegmentRecords: 20, // rotate every 20 records
		GroupCommit:       axwal.GroupCommitConfig{Enabled: true, MaxLatency: 5 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	w, err := NewWriter(WriterOptions{
		Writer: rawW, Dir: dir, Name: "tx-producer", CloseUnderlying: true,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	const N = 50
	for i := 0; i < N; i++ {
		body := make([]byte, 16)
		binary.BigEndian.PutUint64(body[0:8], uint64(i))
		if err := w.Push(body); err != nil {
			t.Fatalf("Push %d: %v", i, err)
		}
	}

	if cnt, err := w.Counter(); err != nil || cnt != N {
		t.Errorf("Counter() = %d err=%v, want %d", cnt, err, N)
	}
	if lid, err := w.LastID(); err != nil || lid != N {
		t.Errorf("LastID() = %d err=%v, want %d", lid, err, N)
	}
	// MaxSegmentRecords=20 → after 50 records we expect 3 segments
	// (closed: F=1,21 ; open: F=41). MinimalFID = 1, LastFID = 41.
	if minFid, err := w.MinimalFID(); err != nil || minFid != 1 {
		t.Errorf("MinimalFID() = %d err=%v, want 1", minFid, err)
	}
	if lastFid, err := w.LastFID(); err != nil || lastFid != 41 {
		t.Errorf("LastFID() = %d err=%v, want 41", lastFid, err)
	}
	if minID, err := w.MinimalID(); err != nil || minID != 1 {
		t.Errorf("MinimalID() = %d err=%v, want 1", minID, err)
	}
	if name := w.GetName(); name != "tx-producer" {
		t.Errorf("GetName() = %q", name)
	}
	if so := w.GetOpts(); so.GetType() != "Writer" || so.GetName() != "tx-producer" {
		t.Errorf("GetOpts: type=%q name=%q", so.GetType(), so.GetName())
	}

	w.Close()

	// ----- reader through axq adapter -----
	rawR, err := axwal.OpenReader(axwal.ReaderOptions{
		Dir:              dir,
		Name:             "consumer-1",
		StartFrom:        axwal.StartEarliest,
		AckBatchInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	rdrCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	r, err := NewReader(ReaderOptions{
		Reader: rawR, Dir: dir,
		Name: "consumer-1", WriterName: "tx-producer",
		Ctx: rdrCtx, CloseUnderlying: true,
	})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	for i := 0; i < N; i++ {
		msg := r.Pop()
		if msg == nil {
			t.Fatalf("Pop nil at %d", i)
		}
		want := uint64(i + 1)
		if msg.Id() != want {
			t.Fatalf("msg %d id = %d, want %d", i, msg.Id(), want)
		}
		got := binary.BigEndian.Uint64(msg.Message()[0:8])
		if got != uint64(i) {
			t.Fatalf("seq mismatch: got %d want %d", got, i)
		}
		msg.Done()
	}

	if cnt, err := r.Counter(); err != nil {
		t.Errorf("Reader.Counter err: %v", err)
	} else if cnt < N {
		// Counter() in ax-wal counts NextCalls, which equals N here.
		t.Errorf("Reader.Counter() = %d, want >= %d", cnt, N)
	}
	if lid, err := r.LastID(); err != nil || lid != N {
		t.Errorf("Reader.LastID() = %d err=%v, want %d", lid, err, N)
	}
	if fid, err := r.LastFID(); err != nil || fid != 41 {
		t.Errorf("Reader.LastFID() = %d err=%v, want 41", fid, err)
	}
}

// TestSegmentsHelper validates the segment-name parser independent of the
// rest of the adapter (cheap to run, catches regression on rename rules).
func TestSegmentsHelper(t *testing.T) {
	dir := t.TempDir()

	// Empty dir: nothing.
	ids, err := listSegmentFIDs(dir)
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("expected empty, got %v", ids)
	}

	// Synthesize a few segment files (we don't need real WAL contents —
	// the parser only looks at filenames).
	for _, name := range []string{
		"wal-1-100",      // closed
		"wal-101-200.zst", // closed + compressed
		"wal-201-",       // open
		"unrelated.txt",  // ignored
		"wal-bogus",      // ignored
	} {
		path := filepath.Join(dir, name)
		_ = quickTouch(t, path)
	}

	ids, err = listSegmentFIDs(dir)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []uint64{1, 101, 201}
	if len(ids) != len(want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Errorf("ids[%d] = %d, want %d", i, ids[i], want[i])
		}
	}

	min, max, err := minMaxSegmentFID(dir)
	if err != nil || min != 1 || max != 201 {
		t.Errorf("minMax = (%d, %d), err=%v, want (1, 201)", min, max, err)
	}
}

func quickTouch(t *testing.T, path string) error {
	t.Helper()
	f, err := openOrCreate(path)
	if err != nil {
		t.Fatalf("touch %s: %v", path, err)
	}
	return f.Close()
}
