package axwal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These tests walk a Reader through everything that can happen to the
// segments under it. The property checked everywhere: the reader never
// stalls and never hands out a record under the wrong index — it either
// delivers the record, waits because the log really ends there, skips a hole
// that really exists (and says so), or returns an error.

func fastGaps(t *testing.T) {
	t.Helper()
	old := gapGrace
	gapGrace = 30 * time.Millisecond
	t.Cleanup(func() { gapGrace = old })
}

func scenarioReader(t *testing.T, dir string, opts ReaderOptions) *Reader {
	t.Helper()
	opts.Dir = dir
	if opts.Name == "" {
		opts.Name = "r"
	}
	opts.PollInterval = 5 * time.Millisecond
	r, err := OpenReader(opts)
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })
	return r
}

// mustNext reads one record and checks index and payload ("rec-<idx>").
func mustNext(t *testing.T, r *Reader, want uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, idx, err := r.Next(ctx)
	require.NoError(t, err, "reader stalled or failed waiting for index %d", want)
	require.Equal(t, want, idx)
	require.Equal(t, fmt.Sprintf("rec-%d", want), string(p))
}

// mustWait asserts that Next has nothing to deliver: it blocks until the
// context expires, without an error of its own.
func mustWait(t *testing.T, r *Reader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, idx, err := r.Next(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "expected the reader to wait, got index %d", idx)
}

func scenarioWriter(t *testing.T, dir string, opts WriterOptions) *Writer {
	t.Helper()
	opts.Dir = dir
	if opts.MaxSegmentRecords == 0 {
		opts.MaxSegmentRecords = 10
	}
	w, err := OpenWriter(opts)
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() })
	return w
}

func removeSegment(t *testing.T, dir string, first, last uint64) {
	t.Helper()
	ms, _ := filepath.Glob(filepath.Join(dir, formatClosedName(first, last)+"*"))
	require.NotEmpty(t, ms, "segment %d-%d not found", first, last)
	for _, m := range ms {
		require.NoError(t, os.Remove(m))
	}
}

// --- holes ------------------------------------------------------------------

// The segments a lagging reader would need next are truncated away while it
// is in the middle of an earlier one.
func TestReaderScenario_PrefixTruncatedUnderLaggingReader(t *testing.T) {
	fastGaps(t)
	dir := t.TempDir()
	w := scenarioWriter(t, dir, WriterOptions{})
	appendRecs(t, w, 1, 45)
	w.awaitRotations()

	r := scenarioReader(t, dir, ReaderOptions{})
	for i := uint64(1); i <= 5; i++ {
		mustNext(t, r, i)
	}
	n, err := w.TruncatePrefix(31) // wal-1-10, wal-11-20, wal-21-30 go
	require.NoError(t, err)
	require.Equal(t, 3, n)

	// It still holds wal-1-10 open, so it finishes that one…
	for i := uint64(6); i <= 10; i++ {
		mustNext(t, r, i)
	}
	// …then finds 11..30 gone and continues with what exists.
	for i := uint64(31); i <= 45; i++ {
		mustNext(t, r, i)
	}
	require.Equal(t, uint64(20), r.Stats().SkippedRecords)

	appendRecs(t, w, 46, 60)
	for i := uint64(46); i <= 60; i++ {
		mustNext(t, r, i)
	}
	mustWait(t, r)
}

// The segment disappears before the reader has opened anything at all, and
// the log it finds starts later than where it wants to begin.
func TestReaderScenario_LogStartsAfterReaderPosition(t *testing.T) {
	fastGaps(t)
	dir := t.TempDir()
	w := scenarioWriter(t, dir, WriterOptions{})
	appendRecs(t, w, 1, 25)
	w.awaitRotations()

	r := scenarioReader(t, dir, ReaderOptions{}) // positioned at 1
	removeSegment(t, dir, 1, 10)
	for i := uint64(11); i <= 25; i++ {
		mustNext(t, r, i)
	}
	require.Equal(t, uint64(10), r.Stats().SkippedRecords)
}

// A segment in the middle is missing (deleted by hand, removed by fsck).
func TestReaderScenario_MissingSegmentInTheMiddle(t *testing.T) {
	fastGaps(t)
	dir := t.TempDir()
	w := scenarioWriter(t, dir, WriterOptions{})
	appendRecs(t, w, 1, 35)
	w.awaitRotations()
	removeSegment(t, dir, 11, 20)

	r := scenarioReader(t, dir, ReaderOptions{})
	want := []uint64{}
	for i := uint64(1); i <= 10; i++ {
		want = append(want, i)
	}
	for i := uint64(21); i <= 35; i++ {
		want = append(want, i)
	}
	for _, i := range want {
		mustNext(t, r, i)
	}
	require.Equal(t, uint64(10), r.Stats().SkippedRecords)
	mustWait(t, r)

	// Same through NextBatch on a fresh reader.
	rb := scenarioReader(t, dir, ReaderOptions{Name: "batch"})
	var got []uint64
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for len(got) < len(want) {
		_, idxs, err := rb.NextBatch(ctx, 7)
		require.NoError(t, err)
		got = append(got, idxs...)
	}
	require.Equal(t, want, got)
}

// A hole is only skipped once it has persisted: a segment that shows up
// within the grace period is read, nothing is lost.
func TestReaderScenario_HoleThatFillsInIsNotSkipped(t *testing.T) {
	old := gapGrace
	gapGrace = 400 * time.Millisecond
	defer func() { gapGrace = old }()

	dir := t.TempDir()
	w := scenarioWriter(t, dir, WriterOptions{})
	appendRecs(t, w, 1, 25)
	w.awaitRotations()
	seg := filepath.Join(dir, "wal-11-20")
	hidden := filepath.Join(t.TempDir(), "wal-11-20")
	require.NoError(t, os.Rename(seg, hidden))

	r := scenarioReader(t, dir, ReaderOptions{})
	for i := uint64(1); i <= 10; i++ {
		mustNext(t, r, i)
	}
	time.AfterFunc(100*time.Millisecond, func() { _ = os.Rename(hidden, seg) })
	for i := uint64(11); i <= 25; i++ {
		mustNext(t, r, i)
	}
	require.Zero(t, r.Stats().SkippedRecords)
}

// --- retention / archive ----------------------------------------------------

// Segments are uploaded to the archive and then deleted locally while a
// reader follows the writer. Nothing it still needs is taken away.
func TestReaderScenario_ArchivedAndDeletedWhileFollowing(t *testing.T) {
	fastGaps(t)
	dir := t.TempDir()
	store := newMemStore()
	w := scenarioWriter(t, dir, WriterOptions{
		CompressOnRotate: true,
		Archive:          &ArchiveConfig{Store: store, Prefix: "p", Retry: fastRetry()},
		Retention:        RetentionConfig{Enabled: true, Interval: 10 * time.Millisecond, StaleReaderTTL: time.Hour},
	})
	r := scenarioReader(t, dir, ReaderOptions{})

	const total = 300
	go func() {
		for i := 1; i <= total; i++ {
			if _, err := w.Append([]byte(fmt.Sprintf("rec-%d", i))); err != nil {
				return
			}
			if i%25 == 0 {
				time.Sleep(15 * time.Millisecond) // let archiver and janitor interleave
			}
		}
	}()
	for i := uint64(1); i <= total; i++ {
		mustNext(t, r, i)
		require.NoError(t, r.Ack(i))
	}
	require.Zero(t, r.Stats().SkippedRecords)

	// The old segments really did go to the archive and off the local disk.
	waitFor(t, "local segments reclaimed", func() bool { return w.Stats().RetentionDeletes >= 20 })
	require.GreaterOrEqual(t, len(store.segmentKeys()), 20)
	mustWait(t, r)
}

// A reader that stopped acknowledging keeps its segments: archive + retention
// delete only what lies behind its cursor.
func TestReaderScenario_RetentionSparesLaggingReader(t *testing.T) {
	fastGaps(t)
	dir := t.TempDir()
	store := newMemStore()
	w := scenarioWriter(t, dir, WriterOptions{
		Archive:   &ArchiveConfig{Store: store, Prefix: "p", Retry: fastRetry()},
		Retention: RetentionConfig{Enabled: true, Interval: 10 * time.Millisecond, StaleReaderTTL: time.Hour},
	})
	r := scenarioReader(t, dir, ReaderOptions{})
	appendRecs(t, w, 1, 100)
	for i := uint64(1); i <= 25; i++ {
		mustNext(t, r, i)
	}
	require.NoError(t, r.Ack(25))

	// 9 closed segments (wal-91-100 is still open).
	waitFor(t, "everything archived", func() bool { return len(store.segmentKeys()) >= 9 })
	waitFor(t, "segments behind the cursor reclaimed", func() bool { return w.Stats().RetentionDeletes >= 2 })
	time.Sleep(50 * time.Millisecond) // several more janitor passes
	require.Equal(t, uint64(2), w.Stats().RetentionDeletes, "only wal-1-10 and wal-11-20 are behind the cursor")

	for i := uint64(26); i <= 100; i++ {
		mustNext(t, r, i)
	}
	require.Zero(t, r.Stats().SkippedRecords)
}

// MaxRetainAge is a hard purge: it deletes under a live reader. The reader
// loses those records, says so, and carries on.
func TestReaderScenario_HardPurgeUnderReader(t *testing.T) {
	fastGaps(t)
	dir := t.TempDir()
	w := scenarioWriter(t, dir, WriterOptions{
		Retention: RetentionConfig{Enabled: true, Interval: time.Hour, MaxRetainAge: time.Hour, StaleReaderTTL: time.Hour},
	})
	// The reader registers first: without any cursor, retention reclaims
	// closed segments as soon as they rotate.
	r := scenarioReader(t, dir, ReaderOptions{})
	appendRecs(t, w, 1, 45)
	w.awaitRotations()
	for i := uint64(1); i <= 3; i++ {
		mustNext(t, r, i)
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, name := range []string{"wal-11-20", "wal-21-30"} {
		require.NoError(t, os.Chtimes(filepath.Join(dir, name), old, old))
	}
	require.NoError(t, w.runRetention())

	for i := uint64(4); i <= 10; i++ {
		mustNext(t, r, i)
	}
	for i := uint64(31); i <= 45; i++ {
		mustNext(t, r, i)
	}
	require.Equal(t, uint64(20), r.Stats().SkippedRecords)
}

// Rotation + background compression constantly rename and replace files
// right where a tailing reader is working.
func TestReaderScenario_TailThroughRotationAndCompression(t *testing.T) {
	dir := t.TempDir()
	w := scenarioWriter(t, dir, WriterOptions{MaxSegmentRecords: 3, CompressOnRotate: true, SyncMode: SyncNone})
	readers := []*Reader{
		scenarioReader(t, dir, ReaderOptions{Name: "a"}),
		scenarioReader(t, dir, ReaderOptions{Name: "b", UseMmap: true}),
	}
	const total = 1500
	go func() {
		for i := 1; i <= total; i++ {
			if _, err := w.Append([]byte(fmt.Sprintf("rec-%d", i))); err != nil {
				return
			}
		}
	}()
	var wg sync.WaitGroup
	for _, r := range readers {
		wg.Add(1)
		go func(r *Reader) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for want := uint64(1); want <= total; want++ {
				p, idx, err := r.Next(ctx)
				if err != nil || idx != want || string(p) != fmt.Sprintf("rec-%d", want) {
					t.Errorf("reader %s: want %d, got idx=%d payload=%q err=%v", r.opts.Name, want, idx, p, err)
					return
				}
			}
			if s := r.Stats().SkippedRecords; s != 0 {
				t.Errorf("reader %s skipped %d records", r.opts.Name, s)
			}
		}(r)
	}
	wg.Wait()
}

// --- end of the log ---------------------------------------------------------

func TestReaderScenario_EndOfLogAndWriterRestarts(t *testing.T) {
	dir := t.TempDir()
	r := scenarioReader(t, dir, ReaderOptions{})
	mustWait(t, r) // empty directory, no writer yet

	w, err := OpenWriter(WriterOptions{Dir: dir})
	require.NoError(t, err)
	mustWait(t, r) // writer open, nothing written
	appendRecs(t, w, 1, 3)
	for i := uint64(1); i <= 3; i++ {
		mustNext(t, r, i)
	}
	mustWait(t, r)

	// Writer goes away cleanly: still the end of the log, not an error.
	require.NoError(t, w.Close())
	mustWait(t, r)

	// A new writer continues; the reader picks it up.
	w, err = OpenWriter(WriterOptions{Dir: dir, CompressOnRotate: true})
	require.NoError(t, err)
	appendRecs(t, w, 4, 6)
	for i := uint64(4); i <= 6; i++ {
		mustNext(t, r, i)
	}
	// Explicit rotation at the tail, then more.
	require.NoError(t, w.Rotate())
	mustWait(t, r)
	appendRecs(t, w, 7, 9)
	for i := uint64(7); i <= 9; i++ {
		mustNext(t, r, i)
	}
	require.NoError(t, w.Close())
	mustWait(t, r)
	require.Zero(t, r.Stats().SkippedRecords)
}

// The writer dies mid-record. The reader, sitting on the tail, sees garbage
// there: it must wait (not fail, not skip) until the restarted writer has cut
// the torn tail off and writes on.
func TestReaderScenario_WriterCrashLeavesTornTail(t *testing.T) {
	for _, garbage := range []string{"partial frame", "complete frame with bad CRC"} {
		t.Run(garbage, func(t *testing.T) {
			dir := t.TempDir()
			w, err := OpenWriter(WriterOptions{Dir: dir})
			require.NoError(t, err)
			appendRecs(t, w, 1, 5)
			r := scenarioReader(t, dir, ReaderOptions{})
			for i := uint64(1); i <= 5; i++ {
				mustNext(t, r, i)
			}
			open := w.curPath
			crashWriter(t, w)

			frame, err := encodeRecord([]byte("rec-6"))
			require.NoError(t, err)
			torn := frame[:len(frame)-3]
			if garbage != "partial frame" {
				frame[len(frame)-1] ^= 0xff
				torn = frame
			}
			f, err := os.OpenFile(open, os.O_WRONLY|os.O_APPEND, 0)
			require.NoError(t, err)
			_, err = f.Write(torn)
			require.NoError(t, err)
			require.NoError(t, f.Close())

			mustWait(t, r)
			mustWait(t, r)

			w, err = OpenWriter(WriterOptions{Dir: dir}) // recovery truncates the tail
			require.NoError(t, err)
			defer w.Close()
			require.Equal(t, uint64(5), w.LastIndex())
			appendRecs(t, w, 6, 8)
			for i := uint64(6); i <= 8; i++ {
				mustNext(t, r, i)
			}
			require.Zero(t, r.Stats().SkippedRecords)
		})
	}
}

// The writer lost its tail (crash without fsync) and is behind the reader's
// position. The reader waits for the log to reach it again instead of failing.
func TestReaderScenario_WriterBehindReader(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir})
	require.NoError(t, err)
	appendRecs(t, w, 1, 20)
	r := scenarioReader(t, dir, ReaderOptions{})
	for i := uint64(1); i <= 20; i++ {
		mustNext(t, r, i)
	}
	require.NoError(t, r.Ack(20))
	require.NoError(t, r.Close())
	require.NoError(t, w.Close())

	// The WAL comes back holding only 1..15.
	removeSegment(t, dir, 1, 20)
	w, err = OpenWriter(WriterOptions{Dir: dir})
	require.NoError(t, err)
	defer w.Close()
	appendRecs(t, w, 1, 15)

	r = scenarioReader(t, dir, ReaderOptions{}) // cursor says 20
	require.Equal(t, uint64(21), r.NextIndex())
	mustWait(t, r)
	appendRecs(t, w, 16, 23)
	for i := uint64(21); i <= 23; i++ {
		mustNext(t, r, i)
	}
}

// --- damaged segments -------------------------------------------------------

func damageScenario(t *testing.T, damage func(path string)) (dir string) {
	t.Helper()
	dir = t.TempDir()
	w := scenarioWriter(t, dir, WriterOptions{})
	appendRecs(t, w, 1, 35)
	w.awaitRotations()
	damage(filepath.Join(dir, "wal-11-20"))
	return dir
}

func TestReaderScenario_DamagedClosedSegment(t *testing.T) {
	fastGaps(t)
	flipByte := func(path string) {
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		b[len(b)/2] ^= 0xff // somewhere in record 15 or 16
		require.NoError(t, os.WriteFile(path, b, 0644))
	}
	cutShort := func(path string) {
		fi, err := os.Stat(path)
		require.NoError(t, err)
		require.NoError(t, os.Truncate(path, fi.Size()/2+3)) // mid-record
	}
	cutAtBoundary := func(path string) {
		fi, err := os.Stat(path)
		require.NoError(t, err)
		require.NoError(t, os.Truncate(path, fi.Size()/2)) // 5 whole records left
	}
	for name, tc := range map[string]struct {
		damage   func(string)
		sentinel error
	}{
		"bit flip":              {flipByte, ErrCorruptRecord},
		"truncated mid-record":  {cutShort, ErrTruncatedRecord},
		"truncated at boundary": {cutAtBoundary, ErrTruncatedRecord},
	} {
		t.Run(name, func(t *testing.T) {
			dir := damageScenario(t, tc.damage)

			// Default: the error is reported, and reported again — the reader
			// neither moves past the damage nor mislabels later records.
			r := scenarioReader(t, dir, ReaderOptions{Name: "strict"})
			var lastGood uint64
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for {
				p, idx, err := r.Next(ctx)
				if err != nil {
					require.ErrorIs(t, err, tc.sentinel)
					break
				}
				require.Equal(t, fmt.Sprintf("rec-%d", idx), string(p))
				lastGood = idx
			}
			require.GreaterOrEqual(t, lastGood, uint64(10))
			require.Less(t, lastGood, uint64(20))
			for i := 0; i < 3; i++ {
				_, _, err := r.Next(ctx)
				require.ErrorIs(t, err, tc.sentinel, "the error must be sticky")
			}
			require.Equal(t, lastGood+1, r.NextIndex())

			// Opt-in: give up the rest of that segment and go on.
			rs := scenarioReader(t, dir, ReaderOptions{Name: "lenient", SkipCorruptSegments: true})
			var got []uint64
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
				p, idx, err := rs.Next(ctx)
				cancel()
				if errors.Is(err, context.DeadlineExceeded) {
					break
				}
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("rec-%d", idx), string(p))
				got = append(got, idx)
			}
			require.Equal(t, uint64(1), got[0])
			require.Equal(t, uint64(35), got[len(got)-1])
			require.Equal(t, uint64(35-len(got)), rs.Stats().SkippedRecords)
			require.Equal(t, int(lastGood)+15, len(got), "everything before the damage and all of 21..35")
			require.Positive(t, rs.Stats().ErrCorruptSegs)
		})
	}
}

// Damage inside the OPEN segment with a live writer appending past it: this
// is not a torn tail, so the reader must report it rather than wait forever.
func TestReaderScenario_DamagedOpenSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir})
	require.NoError(t, err)
	defer w.Close()
	appendRecs(t, w, 1, 3)
	r := scenarioReader(t, dir, ReaderOptions{})
	for i := uint64(1); i <= 3; i++ {
		mustNext(t, r, i)
	}
	appendRecs(t, w, 4, 6)
	// Corrupt record 5 in place, before the reader has seen it.
	b, err := os.ReadFile(w.curPath)
	require.NoError(t, err)
	recLen := len(b) / 6
	f, err := os.OpenFile(w.curPath, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{b[4*recLen+recLen-1] ^ 0xff}, int64(4*recLen+recLen-1))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	mustNext(t, r, 4)
	mustWait(t, r) // could still be a torn tail: wait
	appendRecs(t, w, 7, 9)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err = r.Next(ctx)
	require.ErrorIs(t, err, ErrCorruptRecord)
	_, _, err = r.Next(ctx)
	require.ErrorIs(t, err, ErrCorruptRecord, "sticky")
	require.Equal(t, uint64(5), r.NextIndex())
}

// --- restarts ---------------------------------------------------------------

// A reader that was away while its segments were deleted is fast-forwarded
// when it comes back, and one that returns to an intact log resumes exactly
// after its last Ack.
func TestReaderScenario_ReturnAfterAbsence(t *testing.T) {
	fastGaps(t)
	dir := t.TempDir()
	w := scenarioWriter(t, dir, WriterOptions{})
	appendRecs(t, w, 1, 45)
	w.awaitRotations()

	r := scenarioReader(t, dir, ReaderOptions{})
	for i := uint64(1); i <= 7; i++ {
		mustNext(t, r, i)
	}
	require.NoError(t, r.Ack(7))
	require.NoError(t, r.Close())

	r = scenarioReader(t, dir, ReaderOptions{}) // intact: resumes at 8
	mustNext(t, r, 8)
	require.NoError(t, r.Ack(8))
	require.NoError(t, r.Close())

	_, err := w.TruncatePrefix(31)
	require.NoError(t, err)
	r = scenarioReader(t, dir, ReaderOptions{}) // 9..30 are gone
	for i := uint64(31); i <= 45; i++ {
		mustNext(t, r, i)
	}
	mustWait(t, r)
}

func TestListCursorsAndInspectLog(t *testing.T) {
	dir := t.TempDir()
	info, err := InspectLog(dir)
	require.NoError(t, err)
	require.Equal(t, LogInfo{}, info)
	cs, err := ListCursors(dir)
	require.NoError(t, err)
	require.Empty(t, cs)

	w := scenarioWriter(t, dir, WriterOptions{})
	info, err = InspectLog(dir)
	require.NoError(t, err)
	require.Zero(t, info.LastIndex, "open but empty")
	appendRecs(t, w, 1, 37)
	w.awaitRotations()

	a := scenarioReader(t, dir, ReaderOptions{Name: "alpha"})
	scenarioReader(t, dir, ReaderOptions{Name: "beta"})
	for i := uint64(1); i <= 12; i++ {
		mustNext(t, a, i)
	}
	require.NoError(t, a.Ack(12))

	info, err = InspectLog(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(1), info.FirstIndex)
	require.Equal(t, uint64(37), info.LastIndex, "includes the open segment")
	require.Equal(t, 4, info.Segments)
	require.Positive(t, info.Bytes)

	cs, err = ListCursors(dir)
	require.NoError(t, err)
	require.Len(t, cs, 2)
	require.Equal(t, "alpha", cs[0].Name)
	require.Equal(t, uint64(12), cs[0].Index)
	require.Equal(t, os.Getpid(), cs[0].PID)
	require.WithinDuration(t, time.Now(), cs[0].LastHeartbeat, 5*time.Second)
	require.Equal(t, "beta", cs[1].Name)
	require.Zero(t, cs[1].Index)

	// Looking must not register anything.
	entries, err := os.ReadDir(filepath.Join(dir, cursorsSubdir))
	require.NoError(t, err)
	n := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".cursor" {
			n++
		}
	}
	require.Equal(t, 2, n)
}
