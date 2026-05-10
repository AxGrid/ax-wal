package axwal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRoundtrip_SingleSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		idx, err := w.Append([]byte(fmt.Sprintf("rec-%d", i)))
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), idx)
	}
	require.NoError(t, w.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := OpenReader(ReaderOptions{Dir: dir, Name: "test", StartFrom: StartEarliest})
	require.NoError(t, err)
	defer r.Close()

	for i := 0; i < 100; i++ {
		payload, idx, err := r.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), idx)
		require.Equal(t, fmt.Sprintf("rec-%d", i), string(payload))
		require.NoError(t, r.Ack(idx))
	}
}

func TestRotationAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	// Tiny segments so each Append rotates eagerly.
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 64})
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("payload-%03d", i)))
		require.NoError(t, err)
	}
	w.awaitRotations()
	require.NoError(t, w.Close())

	closedSegs := listFilenames(t, dir, "wal-*-*")
	require.Greater(t, len(closedSegs), 1, "expected multiple segments, got: %v", closedSegs)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := OpenReader(ReaderOptions{Dir: dir, Name: "rot", StartFrom: StartEarliest})
	require.NoError(t, err)
	defer r.Close()

	for i := 0; i < 50; i++ {
		_, idx, err := r.Next(ctx)
		require.NoError(t, err, "i=%d", i)
		require.Equal(t, uint64(i+1), idx)
		require.NoError(t, r.Ack(idx))
	}
}

func TestRecovery_TornTail(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}
	// Simulate crash: don't call Close; release the lock manually so the next
	// Open can take over. The open segment "wal-1-" still exists with all 10
	// records intact, but no closed-form rename has happened.
	require.NoError(t, w.lock.Release())

	// Append garbage (a partial frame) directly to the open segment to mimic
	// torn write at crash time.
	openPath := filepath.Join(dir, "wal-1-")
	f, err := os.OpenFile(openPath, os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte{0, 0, 0, 0xFF, 0xAA}) // claims len=255 then dies
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Now open a new writer — should run recovery and discover lastIndex == 10.
	w2, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	require.Equal(t, uint64(10), w2.LastIndex())
	require.NoError(t, w2.Close())

	// And readers see exactly the 10 valid records.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := OpenReader(ReaderOptions{Dir: dir, Name: "tt", StartFrom: StartEarliest})
	require.NoError(t, err)
	defer r.Close()
	for i := 0; i < 10; i++ {
		payload, idx, err := r.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), idx)
		require.Equal(t, fmt.Sprintf("v%d", i), string(payload))
	}
}

func TestRecovery_BitFlipInMiddle(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("aaaaa-%d", i)))
		require.NoError(t, err)
	}
	require.NoError(t, w.lock.Release())

	openPath := filepath.Join(dir, "wal-1-")
	data, err := os.ReadFile(openPath)
	require.NoError(t, err)
	// Flip a bit in the third record's payload area (first record starts at offset 0).
	// Each frame: 8B header + len bytes payload. Records here have len=7 -> 15B per frame.
	flipAt := 8 + 15 + 15 + 4 // somewhere in third record's CRC region or after
	if flipAt < len(data) {
		data[flipAt] ^= 0xFF
		require.NoError(t, os.WriteFile(openPath, data, 0644))
	}

	w2, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	defer w2.Close()
	// Recovery should truncate at the bad record, retaining only the first two.
	require.Equal(t, uint64(2), w2.LastIndex(), "should keep only records before the bit flip")
}

func TestTailAndFollow(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	defer w.Close()

	r, err := OpenReader(ReaderOptions{
		Dir:               dir,
		Name:              "follower",
		StartFrom:         StartEarliest,
		HeartbeatInterval: 100 * time.Millisecond,
		PollInterval:      50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got := make(chan uint64, 20)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			payload, idx, err := r.Next(ctx)
			if err != nil {
				return
			}
			require.Equal(t, fmt.Sprintf("rec-%d", i), string(payload))
			require.NoError(t, r.Ack(idx))
			got <- idx
		}
	}()

	// Sprinkle writes with delays so the reader has to actually wait.
	for i := 0; i < 20; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("rec-%d", i)))
		require.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
	}

	wg.Wait()
	close(got)
	var received []uint64
	for x := range got {
		received = append(received, x)
	}
	require.Len(t, received, 20)
	for i, v := range received {
		require.Equal(t, uint64(i+1), v)
	}
}

func TestStartLatestSkipsHistory(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		_, err := w.Append([]byte("old"))
		require.NoError(t, err)
	}

	r, err := OpenReader(ReaderOptions{
		Dir:          dir,
		Name:         "latest-only",
		StartFrom:    StartLatest,
		PollInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	defer r.Close()
	require.Equal(t, uint64(6), r.NextIndex(), "latest should start AFTER the existing 5 records")

	for i := 0; i < 3; i++ {
		_, err := w.Append([]byte("new"))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		payload, idx, err := r.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, "new", string(payload))
		require.Equal(t, uint64(6+i), idx)
		require.NoError(t, r.Ack(idx))
	}
}

func TestMultipleReadersIndependent(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r1, err := OpenReader(ReaderOptions{Dir: dir, Name: "alpha", StartFrom: StartEarliest})
	require.NoError(t, err)
	defer r1.Close()
	r2, err := OpenReader(ReaderOptions{Dir: dir, Name: "beta", StartFrom: StartEarliest})
	require.NoError(t, err)
	defer r2.Close()

	// r1 reads everything; r2 reads only first 10.
	for i := 0; i < 50; i++ {
		_, idx, err := r1.Next(ctx)
		require.NoError(t, err)
		require.NoError(t, r1.Ack(idx))
	}
	for i := 0; i < 10; i++ {
		_, idx, err := r2.Next(ctx)
		require.NoError(t, err)
		require.NoError(t, r2.Ack(idx))
	}
	require.Equal(t, uint64(50), r1.CurrentIndex())
	require.Equal(t, uint64(10), r2.CurrentIndex())

	// Re-open r2: it should resume at index 11.
	require.NoError(t, r2.Close())
	r2b, err := OpenReader(ReaderOptions{Dir: dir, Name: "beta"})
	require.NoError(t, err)
	defer r2b.Close()
	require.Equal(t, uint64(11), r2b.NextIndex())
}

func TestReaderName_Exclusive(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	defer w.Close()

	r1, err := OpenReader(ReaderOptions{Dir: dir, Name: "exclusive"})
	require.NoError(t, err)
	defer r1.Close()

	_, err = OpenReader(ReaderOptions{Dir: dir, Name: "exclusive"})
	require.Error(t, err, "second reader with same name must fail")
	require.True(t, strings.Contains(err.Error(), "another reader"), "got: %v", err)
}

func TestRetention_DeletesAcknowledgedSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{
		Dir:            dir,
		MaxSegmentSize: 64, // tiny -> many segments
		Retention: RetentionConfig{
			Enabled:          true,
			Interval:         50 * time.Millisecond,
			MinRetainRecords: 5,
			StaleReaderTTL:   2 * time.Second,
		},
	})
	require.NoError(t, err)
	defer w.Close()

	// Reader Acks promptly so cursor advances.
	r, err := OpenReader(ReaderOptions{
		Dir:               dir,
		Name:              "drainer",
		StartFrom:         StartEarliest,
		HeartbeatInterval: 50 * time.Millisecond,
		PollInterval:      20 * time.Millisecond,
	})
	require.NoError(t, err)
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		for i := 0; i < 100; i++ {
			_, _ = w.Append([]byte(fmt.Sprintf("v%d", i)))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	for i := 0; i < 100; i++ {
		_, idx, err := r.Next(ctx)
		require.NoError(t, err)
		require.NoError(t, r.Ack(idx))
	}

	// Wait for janitor to run a few times.
	require.Eventually(t, func() bool {
		segs := listFilenames(t, dir, "wal-*-*")
		// 100 records total + MinRetainRecords=5 -> we expect roughly only the
		// last few segments around indices ~95..100 to remain.
		var remaining []string
		for _, s := range segs {
			if strings.HasSuffix(s, ".lock") || strings.HasSuffix(s, ".tmp") || strings.HasSuffix(s, ".cursor") {
				continue
			}
			remaining = append(remaining, s)
		}
		return len(remaining) <= 5
	}, 3*time.Second, 100*time.Millisecond, "expected old segments to be GC'd")
}

func TestRetention_StaleReaderIgnored(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{
		Dir:            dir,
		MaxSegmentSize: 64,
		Retention: RetentionConfig{
			Enabled:          true,
			Interval:         50 * time.Millisecond,
			MinRetainRecords: 0,
			StaleReaderTTL:   200 * time.Millisecond, // very short for test
		},
	})
	require.NoError(t, err)
	defer w.Close()

	// Manually plant a stale cursor file: pretend a reader Ack'd index=1 a long
	// time ago but never came back.
	cdir := filepath.Join(dir, cursorsSubdir)
	require.NoError(t, os.MkdirAll(cdir, 0755))
	staleRec := []byte(`{"name":"ghost","index":1,"pid":99999,"started_at_unix":0,"last_heartbeat_unix":0}`)
	require.NoError(t, os.WriteFile(filepath.Join(cdir, "ghost.cursor"), staleRec, 0644))

	for i := 0; i < 20; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}

	// Janitor should recognize ghost as stale (heartbeat=0) and proceed to
	// delete segments below currentLast even though ghost's index is 1.
	require.Eventually(t, func() bool {
		segs := listFilenames(t, dir, "wal-*-*")
		var closed []string
		for _, s := range segs {
			if strings.HasSuffix(s, ".lock") || strings.HasSuffix(s, ".tmp") || strings.HasSuffix(s, ".cursor") {
				continue
			}
			closed = append(closed, s)
		}
		return len(closed) <= 2
	}, 2*time.Second, 50*time.Millisecond)
}

func TestReader_FastForwardOnStaleCursor(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: produce 50 records, close. They land in wal-1-50.
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("old%d", i)))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	// Phase 2: produce 5 more, close. They land in wal-51-55 (recovery picks
	// up the existing chain and continues numbering).
	w2, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		_, err := w2.Append([]byte(fmt.Sprintf("new%d", i)))
		require.NoError(t, err)
	}
	require.NoError(t, w2.Close())

	// Phase 3: simulate retention having removed the older segment behind a
	// stalled reader.
	require.NoError(t, os.Remove(filepath.Join(dir, "wal-1-50")))

	// Plant a cursor pointing into the now-missing range.
	cdir := filepath.Join(dir, cursorsSubdir)
	require.NoError(t, os.MkdirAll(cdir, 0755))
	body := []byte(`{"name":"laggard","index":3,"pid":0,"started_at_unix":0,"last_heartbeat_unix":1}`)
	require.NoError(t, os.WriteFile(filepath.Join(cdir, "laggard.cursor"), body, 0644))

	// Reader opens; should warn + fast-forward to index 51.
	r, err := OpenReader(ReaderOptions{Dir: dir, Name: "laggard"})
	require.NoError(t, err)
	defer r.Close()
	require.Equal(t, uint64(51), r.NextIndex(), "should have fast-forwarded to earliest available")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	payload, idx, err := r.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(51), idx)
	require.Equal(t, "new0", string(payload))
}

func TestRetention_RequiresArchivedMark(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{
		Dir:            dir,
		MaxSegmentSize: 64,
		Retention: RetentionConfig{
			Enabled:             true,
			Interval:            50 * time.Millisecond,
			RequireArchivedMark: true,
			MinRetainRecords:    0,
			StaleReaderTTL:      5 * time.Second,
		},
	})
	require.NoError(t, err)

	for i := 0; i < 30; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("v%d", i)))
		require.NoError(t, err)
	}
	w.awaitRotations()

	// Wait long enough for the janitor to scan a few times.
	time.Sleep(300 * time.Millisecond)

	// Without any .archived sidecars, NO segment should have been deleted.
	segs := closedSegments(t, dir)
	require.Greater(t, len(segs), 1, "no segments should be deleted yet (none archived)")

	// Mark the first one archived.
	first := segs[0]
	require.NoError(t, MarkArchived(dir, first.FirstIndex, first.LastIndex))

	require.Eventually(t, func() bool {
		s := closedSegments(t, dir)
		return len(s) == len(segs)-1
	}, 1*time.Second, 50*time.Millisecond, "marked segment should be deletable now")

	require.NoError(t, w.Close())
}

func TestRotation_ByRecordCount(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{
		Dir:               dir,
		MaxSegmentSize:    1 << 20,
		MaxSegmentRecords: 10,
	})
	require.NoError(t, err)

	for i := 0; i < 25; i++ {
		_, err := w.Append([]byte("xxxxxxxxxx")) // small enough that size won't trigger
		require.NoError(t, err)
	}
	w.awaitRotations()
	require.NoError(t, w.Close())

	segs := closedSegments(t, dir)
	require.GreaterOrEqual(t, len(segs), 2, "expected at least 2 segments from record-count trigger; got: %v", segs)
}

func TestRotation_ByAge(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{
		Dir:            dir,
		MaxSegmentSize: 1 << 20,
		MaxSegmentAge:  150 * time.Millisecond,
	})
	require.NoError(t, err)

	_, err = w.Append([]byte("first"))
	require.NoError(t, err)

	// Wait past the age threshold; the rotateAgeLoop polls at 1s minimum, so
	// be patient.
	time.Sleep(1500 * time.Millisecond)

	_, err = w.Append([]byte("second"))
	require.NoError(t, err)
	w.awaitRotations()

	require.NoError(t, w.Close())

	segs := closedSegments(t, dir)
	require.GreaterOrEqual(t, len(segs), 2, "age trigger should have rotated; got: %v", segs)
}

// closedSegments returns all closed segments in dir, parsed and sorted by
// FirstIndex.
func closedSegments(t *testing.T, dir string) []segName {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "wal-*"))
	require.NoError(t, err)
	var out []segName
	for _, p := range matches {
		s, err := parseSegName(filepath.Base(p))
		if err != nil {
			continue
		}
		if s.Closed {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FirstIndex < out[j].FirstIndex })
	return out
}

// listFilenames returns sorted basenames of glob matches in dir.
func listFilenames(t *testing.T, dir, pattern string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	require.NoError(t, err)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, filepath.Base(m))
	}
	sort.Strings(out)
	return out
}
