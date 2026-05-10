package axwal

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// crashWriter releases the writer's flock without going through Close, leaving
// the on-disk state exactly as it would be after a SIGKILL.
func crashWriter(t *testing.T, w *Writer) {
	t.Helper()
	require.NoError(t, w.lock.Release())
	w.ctxCancel()
	// Cancel any pending heartbeat / janitor / rotation goroutines so we don't
	// leak between subtests.
	w.mu.Lock()
	w.closed = true
	if w.gcCond != nil {
		w.gcCond.Broadcast()
	}
	w.mu.Unlock()
}

func reopenAndCount(t *testing.T, dir string) (lastIdx uint64, recordsRead int) {
	t.Helper()
	w2, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	lastIdx = w2.LastIndex()
	require.NoError(t, w2.Close())

	r, err := OpenReader(ReaderOptions{Dir: dir, Name: "verify", StartFrom: StartEarliest})
	require.NoError(t, err)
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		_, idx, err := r.Next(ctx)
		if err != nil {
			break
		}
		recordsRead++
		require.NoError(t, r.Ack(idx))
		if idx == lastIdx {
			break
		}
	}
	return
}

// TestCrash_AfterAppendNoRotation: a writer with N records in the open segment
// gets SIGKILLed (lock released without Close). Recovery must rename the open
// segment to wal-1-N and a fresh writer must continue from N+1.
func TestCrash_AfterAppendNoRotation(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}
	crashWriter(t, w)

	last, count := reopenAndCount(t, dir)
	require.Equal(t, uint64(50), last)
	require.Equal(t, 50, count)
}

// TestCrash_DuringRotation: triggers many rotations, then crashes leaving
// (a) several closed wal-F-L segments AND (b) a still-open wal-X- with extra
// records. Recovery must finalize the open segment and not lose any data.
func TestCrash_DuringRotation(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{
		Dir:            dir,
		MaxSegmentSize: 100, // tiny → frequent rotations
	})
	require.NoError(t, err)
	for i := 0; i < 80; i++ {
		_, err := w.Append([]byte("xxxxxxxxxxx"))
		require.NoError(t, err)
	}
	// Make sure rotation goroutines have done some work but maybe not all of it.
	time.Sleep(20 * time.Millisecond)
	crashWriter(t, w)

	// On disk we now likely have:
	//   wal-1-N1, wal-(N1+1)-N2, ... (some rotated, possibly mid-rename)
	//   wal-X-     (the active segment)
	// Plus possibly some still-renaming intermediate forms that the writer's
	// finalizeRotated abandoned when ctx was canceled.
	last, count := reopenAndCount(t, dir)
	require.Equal(t, uint64(80), last)
	require.Equal(t, 80, count, "must read every record back")
}

// TestCrash_TwoOpenSegmentsAfterCrash: synthesize an unusual but legal post-
// crash state where TWO open-form filenames exist for adjacent ranges. This
// can happen when the writer rotated (closed wal-1- and opened wal-(N+1)-)
// but the async rename to wal-1-N didn't run before the kill.
func TestCrash_TwoOpenSegmentsAfterCrash(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: produce 30 records in wal-1-30 (closed by clean Close).
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	for i := 0; i < 30; i++ {
		_, err := w.Append([]byte("abc"))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	// Manually rename wal-1-30 -> wal-1- to mimic "rotation goroutine never
	// finished the rename". Now we have wal-1- with 30 records.
	require.NoError(t, os.Rename(filepath.Join(dir, "wal-1-30"), filepath.Join(dir, "wal-1-")))

	// Phase 2: open a writer; recovery sees wal-1-, finalizes it, and starts
	// a new wal-31-. Append 20 more.
	w2, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	require.Equal(t, uint64(30), w2.LastIndex(), "recovery should have read tail of wal-1-")
	for i := 0; i < 20; i++ {
		_, err := w2.Append([]byte("def"))
		require.NoError(t, err)
	}
	// Crash again with wal-31- open.
	crashWriter(t, w2)

	last, count := reopenAndCount(t, dir)
	require.Equal(t, uint64(50), last)
	require.Equal(t, 50, count)
}

// TestCrash_PartialFrameAtEnd: crash during AppendBatch — the bufio buffer
// flushed some bytes but not the entire batch. Mimic this by manually
// truncating the open segment to a non-frame boundary, then verify recovery
// trims back to the last good frame.
func TestCrash_PartialFrameAtEnd(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err := w.Append([]byte("xxxxxxx")) // 7B payload
		require.NoError(t, err)
	}
	require.NoError(t, w.lock.Release())
	w.ctxCancel()
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()

	// Append 5 garbage bytes that look like a valid header (length=999) without
	// a payload. Recovery should detect ErrTruncatedRecord and trim them.
	openPath := filepath.Join(dir, "wal-1-")
	f, err := os.OpenFile(openPath, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte{0, 0, 3, 0xE7, 0xAA}) // len=999 high byte + 1 random
	require.NoError(t, err)
	require.NoError(t, f.Close())

	last, count := reopenAndCount(t, dir)
	require.Equal(t, uint64(10), last, "torn frame at tail must be trimmed")
	require.Equal(t, 10, count)
}

// TestCrash_PropertyBased: 30 random scenarios — each interleaves Appends
// (single + batched) with manual Rotates and ends with a simulated kill.
// Recovery should always reach an index == count of successful Appends.
func TestCrash_PropertyBased(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping crash-property test in -short")
	}
	for trial := 0; trial < 30; trial++ {
		trial := trial
		// Per-trial seed so subtests are reproducible and independent.
		rng := rand.New(rand.NewSource(int64(1000 + trial)))
		t.Run(fmt.Sprintf("trial-%02d", trial), func(t *testing.T) {
			dir := t.TempDir()
			maxSeg := int64(64 + rng.Intn(2048)) // 64..2KB
			w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: maxSeg})
			require.NoError(t, err)

			expected := uint64(0)
			steps := 20 + rng.Intn(60)
			for s := 0; s < steps; s++ {
				switch rng.Intn(10) {
				case 0:
					if err := w.Rotate(); err != nil {
						t.Fatalf("Rotate: %v", err)
					}
				case 1, 2:
					n := 1 + rng.Intn(8)
					batch := make([][]byte, n)
					for i := range batch {
						batch[i] = []byte(fmt.Sprintf("t%d-s%d-i%d-payload", trial, s, i))
					}
					_, err := w.AppendBatch(batch)
					require.NoError(t, err)
					expected += uint64(n)
				default:
					_, err := w.Append([]byte(fmt.Sprintf("t%d-s%d-rec", trial, s)))
					require.NoError(t, err)
					expected++
				}
			}

			// Random outcome: clean close vs hard crash.
			if rng.Intn(2) == 0 {
				require.NoError(t, w.Close())
			} else {
				crashWriter(t, w)
			}

			last, count := reopenAndCount(t, dir)
			if expected != last || int(expected) != count {
				entries, _ := os.ReadDir(dir)
				var names []string
				for _, e := range entries {
					names = append(names, e.Name())
				}
				t.Logf("dir contents: %v", names)
			}
			require.Equal(t, expected, last, "lastIndex mismatch (trial %d, steps %d)", trial, steps)
			require.Equal(t, int(expected), count, "records readable mismatch (trial %d, lastIndex %d)", trial, last)
		})
	}
}
