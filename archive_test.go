package axwal

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// memStore is an in-memory ArchiveStore with fault injection.
type memStore struct {
	mu      sync.Mutex
	objs    map[string][]byte
	mod     map[string]time.Time
	puts    map[string]int
	failPut int                   // fail this many segment Puts
	corrupt int                   // silently truncate this many segment Puts
	failKey func(key string) bool // fail Puts for matching keys
}

func newMemStore() *memStore {
	return &memStore{objs: map[string][]byte{}, mod: map[string]time.Time{}, puts: map[string]int{}}
}

func (m *memStore) Put(ctx context.Context, key string, body io.Reader, info ArchivePutInfo) error {
	b, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	isSeg := !strings.HasSuffix(key, archiveManifestName)
	if m.failKey != nil && m.failKey(key) {
		return errors.New("memstore: injected key failure")
	}
	if isSeg && m.failPut > 0 {
		m.failPut--
		return errors.New("memstore: injected put failure")
	}
	if int64(len(b)) != info.Size {
		return fmt.Errorf("memstore: size %d != info %d", len(b), info.Size)
	}
	if isSeg && m.corrupt > 0 {
		m.corrupt--
		b = b[:len(b)/2]
	}
	m.objs[key] = b
	m.mod[key] = time.Now()
	m.puts[key]++
	return nil
}

func (m *memStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrObjectNotFound, key)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *memStore) List(ctx context.Context, prefix string) ([]ArchiveObject, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []ArchiveObject
	for k, b := range m.objs {
		if strings.HasPrefix(k, prefix) {
			sum := sha1.Sum(b)
			out = append(out, ArchiveObject{Key: k, Size: int64(len(b)), ModTime: m.mod[k], SHA1: hex.EncodeToString(sum[:])})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (m *memStore) obj(key string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.objs[key]
}

func (m *memStore) putCount(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.puts[key]
}

func (m *memStore) segmentKeys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.objs {
		if !strings.HasSuffix(k, archiveManifestName) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func fastRetry() Backoff { return Backoff{Min: 10 * time.Millisecond, Max: 40 * time.Millisecond} }

func appendN(t *testing.T, w *Writer, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("record-%04d", i)))
		require.NoError(t, err)
	}
}

// allArchived reports whether every closed segment in dir has its mark.
func allArchived(t *testing.T, dir string) bool {
	segs := closedSegments(t, dir)
	for _, s := range segs {
		if !isArchived(dir, s) {
			return false
		}
	}
	return len(segs) > 0
}

// Regression: with CompressOnRotate the .meta sidecar used to be hashed after
// compression had already removed the uncompressed segment.
func TestMetaSidecar_WithCompression(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir, CompressOnRotate: true, WriteMetaSidecar: true})
	require.NoError(t, err)
	appendN(t, w, 6)
	require.NoError(t, w.Rotate())
	w.awaitRotations()

	meta, err := readMetaSidecar(dir, 1, 6)
	require.NoError(t, err)
	require.NotNil(t, meta, "meta sidecar missing for compressed segment")
	require.Equal(t, uint64(6), meta.NumRecords)
	require.Equal(t, "zst", meta.Compression)
	require.Len(t, meta.SHA256Uncompressed, 64)
	require.NotZero(t, meta.FirstRecordUnix)
	require.FileExists(t, filepath.Join(dir, "wal-1-6.zst"))
	require.NoFileExists(t, filepath.Join(dir, "wal-1-6"))
	require.NoError(t, w.Close())
}

func TestArchiver_UploadsCompressedSegments(t *testing.T) {
	dir := t.TempDir()
	store := newMemStore()
	w, err := OpenWriter(WriterOptions{
		Dir:               dir,
		MaxSegmentRecords: 10,
		CompressOnRotate:  true,
		Archive: &ArchiveConfig{
			Store: store, Prefix: "/wal/test/", Layout: LayoutByDay, Retry: fastRetry(),
			DrainOnClose: 5 * time.Second,
		},
	})
	require.NoError(t, err)
	appendN(t, w, 35)
	waitFor(t, "3 uploads", func() bool { return w.Stats().ArchiveUploads >= 3 })
	st := w.Stats()
	require.Zero(t, st.ArchiveErrors)
	require.False(t, st.ArchiveLastSuccess.IsZero())
	require.NoError(t, w.Close()) // RotateOnClose + drain: the tail goes too

	day := time.Now().UTC().Format("2006/01/02")
	require.Equal(t, []string{
		"wal/test/" + day + "/wal-1-10.zst",
		"wal/test/" + day + "/wal-11-20.zst",
		"wal/test/" + day + "/wal-21-30.zst",
		"wal/test/" + day + "/wal-31-35.zst",
	}, store.segmentKeys())
	require.True(t, allArchived(t, dir))

	m, err := LoadArchiveManifest(context.Background(), store, "wal/test")
	require.NoError(t, err)
	require.Len(t, m.Segments, 4)
	for i, s := range m.Segments {
		require.Equal(t, uint64(i*10+1), s.FirstIndex)
		require.Equal(t, "zst", s.Compression)
		require.Len(t, s.SHA1, 40)
		require.Equal(t, int64(len(store.obj(s.Key))), s.SizeBytes)
		require.NotZero(t, s.FirstRecordUnix)
	}
	for k, n := range store.puts {
		if !strings.HasSuffix(k, archiveManifestName) {
			require.Equal(t, 1, n, "segment %s uploaded more than once", k)
		}
	}
}

func TestArchiver_RetriesFailuresAndVerifies(t *testing.T) {
	dir := t.TempDir()
	store := newMemStore()
	store.failPut = 3 // transport errors
	store.corrupt = 1 // then one upload that "succeeds" but stores garbage
	w, err := OpenWriter(WriterOptions{
		Dir:               dir,
		MaxSegmentRecords: 10,
		Archive: &ArchiveConfig{Store: store, Prefix: "p", Retry: fastRetry(), Concurrency: 1,
			DrainOnClose: 5 * time.Second},
	})
	require.NoError(t, err)
	appendN(t, w, 25)
	waitFor(t, "2 uploads despite failures", func() bool { return w.Stats().ArchiveUploads >= 2 })
	st := w.Stats()
	require.GreaterOrEqual(t, st.ArchiveErrors, uint64(4))
	waitFor(t, "pending gauge to settle", func() bool { return w.Stats().ArchivePending == 0 })
	require.NoError(t, w.Close())

	// What is in the store is byte-identical to the local files.
	for _, s := range closedSegments(t, dir) {
		local, err := os.ReadFile(filepath.Join(dir, s.Name))
		require.NoError(t, err)
		require.Equal(t, local, store.obj("p/"+s.Name), s.Name)
	}
}

func TestArchiver_ManifestFailureKeepsSegmentPending(t *testing.T) {
	dir := t.TempDir()
	store := newMemStore()
	var mu sync.Mutex
	failManifest := true
	store.failKey = func(key string) bool {
		mu.Lock()
		defer mu.Unlock()
		return failManifest && strings.HasSuffix(key, archiveManifestName)
	}
	w, err := OpenWriter(WriterOptions{
		Dir:     dir,
		Archive: &ArchiveConfig{Store: store, Prefix: "p", Retry: fastRetry()},
	})
	require.NoError(t, err)
	defer w.Close()
	appendN(t, w, 5)
	require.NoError(t, w.Rotate())
	waitFor(t, "errors", func() bool { return w.Stats().ArchiveErrors >= 2 })
	require.False(t, isArchived(dir, segName{FirstIndex: 1, LastIndex: 5, Closed: true}),
		"segment must not be marked while the manifest write fails")
	require.Equal(t, 1, w.Stats().ArchivePending)
	require.Greater(t, w.Stats().ArchiveOldestPendingAge, time.Duration(0))

	mu.Lock()
	failManifest = false
	mu.Unlock()
	waitFor(t, "archived", func() bool { return allArchived(t, dir) })
	require.Equal(t, 1, store.putCount("p/wal-1-5"), "object must not be re-uploaded on retry")
}

func TestArchiver_PicksUpLeftoversOnOpen(t *testing.T) {
	dir := t.TempDir()
	// First run: no archiver at all; the last segment is only renamed by Close.
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentRecords: 10})
	require.NoError(t, err)
	appendN(t, w, 25)
	require.NoError(t, w.Close())

	// A crashed run leaves an open segment behind.
	open := filepath.Join(dir, formatOpenName(26))
	var buf []byte
	for i := 0; i < 3; i++ {
		frame, err := encodeRecord([]byte("after-crash"))
		require.NoError(t, err)
		buf = append(buf, frame...)
	}
	require.NoError(t, os.WriteFile(open, buf, 0644))

	store := newMemStore()
	w, err = OpenWriter(WriterOptions{
		Dir:              dir,
		CompressOnRotate: true,
		Archive:          &ArchiveConfig{Store: store, Prefix: "p", Retry: fastRetry()},
	})
	require.NoError(t, err)
	defer w.Close()
	waitFor(t, "leftovers archived", func() bool { return len(store.segmentKeys()) == 4 })
	require.Equal(t, []string{"p/wal-1-10.zst", "p/wal-11-20.zst", "p/wal-21-25.zst", "p/wal-26-28.zst"},
		store.segmentKeys())
	waitFor(t, "marks", func() bool { return allArchived(t, dir) })
}

func TestRotateOnClose(t *testing.T) {
	dir := t.TempDir()
	var rotated []SegmentInfo
	w, err := OpenWriter(WriterOptions{
		Dir: dir, CompressOnRotate: true, WriteMetaSidecar: true, RotateOnClose: true,
		OnRotate: func(s SegmentInfo) { rotated = append(rotated, s) },
	})
	require.NoError(t, err)
	appendN(t, w, 4)
	require.NoError(t, w.Close())

	require.FileExists(t, filepath.Join(dir, "wal-1-4.zst"))
	require.FileExists(t, filepath.Join(dir, "wal-1-4.meta"))
	require.Len(t, rotated, 1)
	require.True(t, rotated[0].Compressed)
}

func TestRetention_MinRetainAge(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{
		Dir:               dir,
		MaxSegmentRecords: 10,
		Retention:         RetentionConfig{Enabled: true, Interval: time.Hour, MinRetainAge: time.Hour},
	})
	require.NoError(t, err)
	defer w.Close()
	appendN(t, w, 35)
	w.awaitRotations()

	// No readers at all: without MinRetainAge everything closed would go.
	require.NoError(t, w.runRetention())
	require.Len(t, closedSegments(t, dir), 3)

	old := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(filepath.Join(dir, "wal-1-10"), old, old))
	require.NoError(t, w.runRetention())
	segs := closedSegments(t, dir)
	require.Len(t, segs, 2)
	require.Equal(t, uint64(11), segs[0].FirstIndex)
}

func TestRetention_MaxTotalBytes(t *testing.T) {
	for _, policy := range []DiskFullPolicy{DiskFullReject, DiskFullDrop} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			dir := t.TempDir()
			w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentRecords: 10})
			require.NoError(t, err)
			appendN(t, w, 35) // ~19 bytes per frame: 3 closed segments of 190 bytes
			require.NoError(t, w.Close())

			w, err = OpenWriter(WriterOptions{
				Dir:               dir,
				MaxSegmentRecords: 10,
				Retention: RetentionConfig{
					Enabled: true, Interval: time.Hour,
					MinRetainAge:        time.Hour, // disk pressure overrides it
					RequireArchivedMark: true,
					MaxTotalBytes:       400,
					OnDiskFull:          policy,
				},
			})
			require.NoError(t, err)
			defer w.Close()

			// Over the limit, nothing archived: nothing may be deleted. The
			// janitor notices right at open.
			waitFor(t, "disk-full state", func() bool { return w.Stats().DiskFull })
			require.Len(t, closedSegments(t, dir), 4)

			idx, err := w.Append([]byte("x"))
			if policy == DiskFullReject {
				require.ErrorIs(t, err, ErrDiskFull)
				require.Equal(t, uint64(1), w.Stats().DiskFullRejects)
			} else {
				require.NoError(t, err)
				require.Zero(t, idx)
				require.Equal(t, uint64(1), w.Stats().Dropped)
			}
			require.Equal(t, uint64(35), w.LastIndex())

			// Once the two oldest are archived they are evicted, oldest first,
			// only as far as needed.
			require.NoError(t, w.MarkArchived(1, 10))
			require.NoError(t, w.MarkArchived(11, 20))
			require.NoError(t, w.runRetention())
			segs := closedSegments(t, dir)
			require.Len(t, segs, 2)
			require.Equal(t, uint64(21), segs[0].FirstIndex)
			require.False(t, w.Stats().DiskFull)
			_, err = w.Append([]byte("x"))
			require.NoError(t, err)
			require.Equal(t, uint64(36), w.LastIndex())
		})
	}
}

func TestRotateAligned(t *testing.T) {
	dir := t.TempDir()
	var mu sync.Mutex
	now := time.Date(2026, 10, 11, 22, 59, 50, 0, time.UTC)
	set := func(t time.Time) { mu.Lock(); now = t; mu.Unlock() }
	w, err := OpenWriter(WriterOptions{
		Dir: dir, WriteMetaSidecar: true, RotateAligned: time.Hour,
		nowFn: func() time.Time { mu.Lock(); defer mu.Unlock(); return now },
	})
	require.NoError(t, err)
	defer w.Close()

	appendN(t, w, 3) // 22:59:50
	set(time.Date(2026, 10, 11, 22, 59, 59, 0, time.UTC))
	appendN(t, w, 2)
	set(time.Date(2026, 10, 11, 23, 0, 0, 0, time.UTC)) // boundary: rotate before this write
	appendN(t, w, 4)
	w.awaitRotations()

	meta, err := readMetaSidecar(dir, 1, 5)
	require.NoError(t, err)
	require.NotNil(t, meta, "segment must have been cut at 23:00")
	require.Equal(t, time.Date(2026, 10, 11, 22, 59, 50, 0, time.UTC).Unix(), meta.FirstRecordUnix)
	require.Equal(t, time.Date(2026, 10, 11, 22, 59, 59, 0, time.UTC).Unix(), meta.LastRecordUnix)

	// An idle segment is closed by the timer once the next window starts.
	set(time.Date(2026, 10, 12, 0, 0, 1, 0, time.UTC))
	waitFor(t, "idle rotation at midnight", func() bool {
		_, err := os.Stat(filepath.Join(dir, "wal-6-9"))
		return err == nil
	})

	day := 24 * time.Hour
	msk := time.FixedZone("MSK", 3*3600)
	at := time.Date(2026, 10, 11, 22, 30, 0, 0, time.UTC) // 01:30 on the 12th in MSK
	require.Equal(t, time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC), alignedWindowStart(at, day, time.UTC))
	require.True(t, time.Date(2026, 10, 12, 0, 0, 0, 0, msk).Equal(alignedWindowStart(at, day, msk)))
	require.Equal(t, time.Date(2026, 10, 11, 22, 30, 0, 0, time.UTC), alignedWindowStart(at, 15*time.Minute, time.UTC))
}

func TestAppendAsync(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir})
	require.NoError(t, err)
	require.ErrorIs(t, w.AppendAsync([]byte("x")), ErrAsyncDisabled)
	require.NoError(t, w.Close())

	w, err = OpenWriter(WriterOptions{Dir: dir, AsyncQueueSize: 64, SyncMode: SyncNone})
	require.NoError(t, err)
	// Hold the writer lock so the drainer can't make progress: the queue
	// fills up and the overflow is dropped without blocking the caller.
	w.mu.Lock()
	queued, dropped := 0, 0
	for i := 0; i < 200; i++ {
		switch err := w.AppendAsync([]byte(fmt.Sprintf("async-%03d", i))); {
		case err == nil:
			queued++
		case errors.Is(err, ErrQueueFull):
			dropped++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	w.mu.Unlock()
	require.GreaterOrEqual(t, queued, 64)
	require.Positive(t, dropped)
	require.Equal(t, uint64(dropped), w.Stats().Dropped)

	// Close writes out everything that was accepted.
	require.NoError(t, w.Close())
	require.ErrorIs(t, w.AppendAsync([]byte("late")), ErrWriterClosed)

	r, err := OpenReader(ReaderOptions{Dir: dir, Name: "r"})
	require.NoError(t, err)
	defer r.Close()
	head, err := r.HeadIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(queued), head)
	p, idx, err := r.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(1), idx)
	require.Equal(t, "async-000", string(p))
}

func TestRotateKey(t *testing.T) {
	dir := t.TempDir()
	key1 := bytes.Repeat([]byte{1}, 32)
	key2 := bytes.Repeat([]byte{2}, 32)
	require.NotEqual(t, KeyFingerprint(key1), KeyFingerprint(key2))
	require.Len(t, KeyFingerprint(key1), 16)

	w, err := OpenWriter(WriterOptions{Dir: dir, WriteMetaSidecar: true, Encryption: &EncryptionConfig{Key: key1}})
	require.NoError(t, err)
	appendN(t, w, 5)
	require.NoError(t, w.RotateKey(key2))
	appendN(t, w, 5)
	require.NoError(t, w.Rotate())
	w.awaitRotations()

	m1, err := readMetaSidecar(dir, 1, 5)
	require.NoError(t, err)
	require.Equal(t, KeyFingerprint(key1), m1.KeyFingerprint)
	m2, err := readMetaSidecar(dir, 6, 10)
	require.NoError(t, err)
	require.Equal(t, KeyFingerprint(key2), m2.KeyFingerprint)
	require.NoError(t, w.Close())

	// A reader with the key set reads across the rotation.
	r, err := OpenReader(ReaderOptions{Dir: dir, Name: "both",
		Encryption: &EncryptionConfig{Key: key2, OldKeys: [][]byte{key1}}})
	require.NoError(t, err)
	for i := 1; i <= 10; i++ {
		p, idx, err := r.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(i), idx)
		require.Equal(t, fmt.Sprintf("record-%04d", (i-1)%5), string(p))
	}
	require.NoError(t, r.Close())

	// Only the new key: the old segment can't be opened.
	r, err = OpenReader(ReaderOptions{Dir: dir, Name: "new-only", Encryption: &EncryptionConfig{Key: key2}})
	require.NoError(t, err)
	defer r.Close()
	_, _, err = r.Next(context.Background())
	require.ErrorIs(t, err, ErrEncryption)
}

// Regression: HeadIndex ignored the open segment whenever a closed one existed.
func TestHeadIndex_IncludesOpenSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentRecords: 10})
	require.NoError(t, err)
	defer w.Close()
	appendN(t, w, 13)
	w.awaitRotations()
	r, err := OpenReader(ReaderOptions{Dir: dir, Name: "r"})
	require.NoError(t, err)
	defer r.Close()
	head, err := r.HeadIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(13), head)
}

func TestArchiveReader_RangesAndMirror(t *testing.T) {
	dir := t.TempDir()
	store := newMemStore()
	key := bytes.Repeat([]byte{7}, 32)
	w, err := OpenWriter(WriterOptions{
		Dir:               dir,
		MaxSegmentRecords: 10,
		CompressOnRotate:  true,
		Encryption:        &EncryptionConfig{Key: key},
		Archive:           &ArchiveConfig{Store: store, Prefix: "arc", Retry: fastRetry(), DrainOnClose: 5 * time.Second},
	})
	require.NoError(t, err)
	for i := 1; i <= 45; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("rec-%d", i)))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	require.Len(t, store.segmentKeys(), 5)

	ctx := context.Background()
	read := func(opts ArchiveReaderOptions) []uint64 {
		opts.Store, opts.Prefix = store, "arc"
		opts.Encryption = &EncryptionConfig{Key: key}
		r, err := OpenArchiveReader(ctx, opts)
		require.NoError(t, err)
		defer r.Close()
		var got []uint64
		for {
			p, idx, err := r.Next(ctx)
			if err == io.EOF {
				return got
			}
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("rec-%d", idx), string(p))
			got = append(got, idx)
		}
	}

	all := read(ArchiveReaderOptions{})
	require.Len(t, all, 45)
	require.Equal(t, uint64(1), all[0])
	require.Equal(t, uint64(45), all[44])

	part := read(ArchiveReaderOptions{FromIndex: 8, ToIndex: 23})
	require.Len(t, part, 16)
	require.Equal(t, uint64(8), part[0])
	require.Equal(t, uint64(23), part[15])

	require.Len(t, read(ArchiveReaderOptions{FromTime: time.Now().Add(-time.Minute)}), 45)
	require.Empty(t, read(ArchiveReaderOptions{FromTime: time.Now().Add(time.Hour)}))
	require.Empty(t, read(ArchiveReaderOptions{ToTime: time.Now().Add(-time.Hour)}))

	// Without the manifest the catalog is rebuilt from the listing.
	delete(store.objs, "arc/"+archiveManifestName)
	require.Len(t, read(ArchiveReaderOptions{FromIndex: 40}), 6)

	// Mirror from the archive into a fresh WAL, then resume after a restart.
	dst := t.TempDir()
	mirror := func(want int64) {
		mctx, cancel := context.WithCancel(ctx)
		done := make(chan int64, 1)
		go func() {
			n, err := Mirror(mctx, MirrorOptions{
				DstDir: dst,
				SrcArchive: &ArchiveReaderOptions{Store: store, Prefix: "arc",
					Encryption: &EncryptionConfig{Key: key}, PollInterval: 20 * time.Millisecond},
			})
			require.NoError(t, err)
			done <- n
		}()
		waitFor(t, "mirror position", func() bool {
			pos, _ := readArchiveMirrorPos(filepath.Join(dst, archiveMirrorPosFile))
			return pos == 45
		})
		cancel()
		require.Equal(t, want, <-done)
	}
	mirror(45)
	mirror(0) // nothing new: resumes from the saved position, no duplicates

	r, err := OpenReader(ReaderOptions{Dir: dst, Name: "check"})
	require.NoError(t, err)
	defer r.Close()
	head, err := r.HeadIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(45), head)
}
