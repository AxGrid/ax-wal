package axwal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func (m *memStore) del(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objs[key]; !ok {
		panic("memStore.del: no such object " + key)
	}
	delete(m.objs, key)
}

func (m *memStore) set(key string, b []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = b
}

var gapKey = bytes.Repeat([]byte{3}, 32)

// buildArchive writes `total` records ("rec-<idx>") in segments of 10 and
// returns the store plus the object key of the segment starting at an index.
func buildArchive(t *testing.T, total int, encrypted, manifest bool) (dir string, store *memStore, keyOf func(first uint64) string, enc *ArchiveEncryption) {
	t.Helper()
	dir = t.TempDir()
	store = newMemStore()
	cfg := &ArchiveConfig{Store: store, Prefix: "p", Retry: fastRetry(), DisableManifest: !manifest,
		DrainOnClose: 5 * time.Second}
	if encrypted {
		enc = &ArchiveEncryption{Key: gapKey}
		cfg.Encryption = enc
	}
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentRecords: 10, CompressOnRotate: true, Archive: cfg})
	require.NoError(t, err)
	appendRecs(t, w, 1, total)
	require.NoError(t, w.Close())
	require.True(t, allArchived(t, dir))
	crypto, _ := newArchiveCrypto(enc)
	keyOf = func(first uint64) string {
		if crypto != nil {
			return "p/" + crypto.segmentName(first)
		}
		last := first + 9
		if last > uint64(total) {
			last = uint64(total)
		}
		return fmt.Sprintf("p/wal-%d-%d.zst", first, last)
	}
	return dir, store, keyOf, enc
}

// readArchive drains a reader, checking that every record carries its own
// index in the payload. It returns the indices delivered and the terminal
// error (nil for a clean io.EOF).
func readArchive(t *testing.T, store ArchiveStore, opts ArchiveReaderOptions) (got []uint64, skipped uint64, err error) {
	t.Helper()
	opts.Store, opts.Prefix = store, "p"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := OpenArchiveReader(ctx, opts)
	if err != nil {
		return nil, 0, err
	}
	defer r.Close()
	for {
		p, idx, err := r.Next(ctx)
		if err == io.EOF {
			return got, r.Stats().SkippedRecords, nil
		}
		if err != nil {
			// The error must be sticky: the reader may not move past it.
			_, _, again := r.Next(ctx)
			require.Error(t, again, "an error must not clear by itself")
			return got, r.Stats().SkippedRecords, err
		}
		require.Equal(t, fmt.Sprintf("rec-%d", idx), string(p))
		got = append(got, idx)
	}
}

func seq(from, to uint64) []uint64 {
	var out []uint64
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

// A hole in the archive is never passed over silently: by default the reader
// stops with ErrArchiveGap naming the range; with SkipMissing it jumps and
// accounts for every record it could not deliver.
func TestArchiveReader_Holes(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%v", encrypted), func(t *testing.T) {
			_, store, keyOf, enc := buildArchive(t, 60, encrypted, true)
			base := ArchiveReaderOptions{ArchiveEncryption: enc}
			with := func(mod func(*ArchiveReaderOptions)) ArchiveReaderOptions {
				o := base
				mod(&o)
				return o
			}

			// Intact: everything, nothing skipped.
			got, skipped, err := readArchive(t, store, base)
			require.NoError(t, err)
			require.Equal(t, seq(1, 60), got)
			require.Zero(t, skipped)

			// 1. An object vanished from the bucket (manifest still lists it).
			saved := store.obj(keyOf(21))
			store.del(keyOf(21))
			got, _, err = readArchive(t, store, base)
			require.ErrorIs(t, err, ErrArchiveGap)
			require.ErrorContains(t, err, "21..30")
			require.Equal(t, seq(1, 20), got)

			got, skipped, err = readArchive(t, store, with(func(o *ArchiveReaderOptions) { o.SkipMissing = true }))
			require.NoError(t, err)
			require.Equal(t, append(seq(1, 20), seq(31, 60)...), got)
			require.Equal(t, uint64(10), skipped)

			// A range request that lies entirely inside the hole, or ends in it.
			_, _, err = readArchive(t, store, with(func(o *ArchiveReaderOptions) { o.FromIndex, o.ToIndex = 23, 27 }))
			require.ErrorIs(t, err, ErrArchiveGap)
			got, skipped, err = readArchive(t, store, with(func(o *ArchiveReaderOptions) {
				o.FromIndex, o.ToIndex, o.SkipMissing = 15, 34, true
			}))
			require.NoError(t, err)
			require.Equal(t, append(seq(15, 20), seq(31, 34)...), got)
			require.Equal(t, uint64(10), skipped)
			store.set(keyOf(21), saved)

			// 2. A segment is absent from the manifest itself.
			crypto, _ := newArchiveCrypto(enc)
			m, err := loadManifest(context.Background(), store, "p", crypto)
			require.NoError(t, err)
			require.Len(t, m.Segments, 6)
			full := append([]ArchiveSegment(nil), m.Segments...)
			m.Segments = append(append([]ArchiveSegment(nil), full[:3]...), full[4:]...) // drop 31..40
			require.NoError(t, putManifest(context.Background(), store, "p", m, crypto))

			got, _, err = readArchive(t, store, base)
			require.ErrorIs(t, err, ErrArchiveGap)
			require.ErrorContains(t, err, "31..40")
			require.Equal(t, seq(1, 30), got)
			got, skipped, err = readArchive(t, store, with(func(o *ArchiveReaderOptions) { o.SkipMissing = true }))
			require.NoError(t, err)
			require.Equal(t, append(seq(1, 30), seq(41, 60)...), got)
			require.Equal(t, uint64(10), skipped)
			// The request ends inside the hole: still not a clean end.
			_, _, err = readArchive(t, store, with(func(o *ArchiveReaderOptions) { o.FromIndex, o.ToIndex = 25, 36 }))
			require.ErrorIs(t, err, ErrArchiveGap)
			require.ErrorContains(t, err, "31..36")
			// A request that never touches the hole is unaffected.
			got, _, err = readArchive(t, store, with(func(o *ArchiveReaderOptions) { o.FromIndex, o.ToIndex = 41, 55 }))
			require.NoError(t, err)
			require.Equal(t, seq(41, 55), got)

			// 3. The archive starts later than the index asked for.
			m.Segments = full[1:] // no 1..10
			require.NoError(t, putManifest(context.Background(), store, "p", m, crypto))
			_, _, err = readArchive(t, store, with(func(o *ArchiveReaderOptions) { o.FromIndex = 4 }))
			require.ErrorIs(t, err, ErrArchiveGap)
			require.ErrorContains(t, err, "4..10")
			// The manifest declares that the log starts at 1, so reading
			// "from the beginning" notices the missing beginning too.
			require.Equal(t, uint64(1), m.FirstIndex)
			_, _, err = readArchive(t, store, base)
			require.ErrorIs(t, err, ErrArchiveGap)
			require.ErrorContains(t, err, "1..10")
			got, skipped, err = readArchive(t, store, with(func(o *ArchiveReaderOptions) { o.SkipMissing = true }))
			require.NoError(t, err)
			require.Equal(t, seq(11, 60), got)
			require.Equal(t, uint64(10), skipped)

			// A manifest from before that field existed: the reader can only
			// begin where the listed segments begin.
			m.FirstIndex = 0
			require.NoError(t, putManifest(context.Background(), store, "p", m, crypto))
			got, skipped, err = readArchive(t, store, base)
			require.NoError(t, err)
			require.Equal(t, seq(11, 60), got)
			require.Zero(t, skipped)
		})
	}
}

// Without a manifest the reader walks the chain of derived names. A missing
// object must not look like the end of the log.
func TestArchiveReader_ChainHoleIsNotTheEnd(t *testing.T) {
	_, store, keyOf, enc := buildArchive(t, 60, true, false)
	opts := ArchiveReaderOptions{ArchiveEncryption: enc}

	got, _, err := readArchive(t, store, opts)
	require.NoError(t, err)
	require.Equal(t, seq(1, 60), got)

	for _, first := range []uint64{31, 51} { // in the middle; the very last segment
		saved := store.obj(keyOf(first))
		store.del(keyOf(first))
		got, _, err = readArchive(t, store, opts)
		require.ErrorIs(t, err, ErrArchiveGap, "segment %d missing", first)
		require.Equal(t, seq(1, first-1), got)
		// It can't be skipped either: nothing says where the next one starts.
		o := opts
		o.SkipMissing = true
		_, _, err = readArchive(t, store, o)
		require.ErrorIs(t, err, ErrArchiveGap)
		store.set(keyOf(first), saved)
	}
}

// slowStore delays the upload of chosen keys, so segments land out of order.
type slowStore struct {
	*memStore
	slow func(key string) time.Duration
}

func (s *slowStore) Put(ctx context.Context, key string, body io.Reader, info ArchivePutInfo) error {
	if d := s.slow(key); d > 0 {
		time.Sleep(d)
	}
	return s.memStore.Put(ctx, key, body, info)
}

// Parallel uploads finish out of order and some fail and are retried. A
// follower must deliver every record exactly once, in order — it waits for a
// late segment instead of jumping over it.
func TestArchiveReader_FollowerLosesNothing(t *testing.T) {
	for _, mode := range []string{"plain+manifest", "encrypted+manifest", "encrypted+chain"} {
		t.Run(mode, func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(len(mode))))
			mem := newMemStore()
			mem.failPut = 12
			var mu sync.Mutex
			store := &slowStore{memStore: mem, slow: func(key string) time.Duration {
				mu.Lock()
				defer mu.Unlock()
				if strings.Contains(key, "manifest") {
					return 0
				}
				return time.Duration(rng.Intn(25)) * time.Millisecond
			}}
			cfg := &ArchiveConfig{Store: store, Prefix: "p", Concurrency: 4, DrainOnClose: 15 * time.Second,
				Retry: fastRetry(), DisableManifest: mode == "encrypted+chain"}
			var enc *ArchiveEncryption
			if mode != "plain+manifest" {
				enc = &ArchiveEncryption{Key: gapKey}
				cfg.Encryption = enc
			}
			w, err := OpenWriter(WriterOptions{Dir: t.TempDir(), MaxSegmentRecords: 7, CompressOnRotate: true,
				SyncMode: SyncNone, Archive: cfg})
			require.NoError(t, err)

			const total = 900
			writerDone := make(chan struct{})
			go func() {
				defer close(writerDone)
				wr := rand.New(rand.NewSource(99))
				for i := 1; i <= total; {
					n := 1 + wr.Intn(5) // batches straddle segment boundaries
					var batch [][]byte
					for j := 0; j < n && i <= total; j, i = j+1, i+1 {
						batch = append(batch, []byte(fmt.Sprintf("rec-%d", i)))
					}
					if _, err := w.AppendBatch(batch); err != nil {
						return
					}
					if wr.Intn(20) == 0 {
						_ = w.Rotate()
					}
				}
				_ = w.Close()
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			r, err := OpenArchiveReader(ctx, ArchiveReaderOptions{
				Store: store, Prefix: "p", ArchiveEncryption: enc,
				Follow: true, PollInterval: 5 * time.Millisecond, GapTimeout: 30 * time.Second,
			})
			require.NoError(t, err)
			defer r.Close()
			for want := uint64(1); want <= total; want++ {
				p, idx, err := r.Next(ctx)
				require.NoError(t, err, "waiting for %d", want)
				require.Equal(t, want, idx)
				require.Equal(t, fmt.Sprintf("rec-%d", want), string(p))
			}
			require.Zero(t, r.Stats().SkippedRecords)
			<-writerDone
			require.Positive(t, w.Stats().ArchiveErrors, "the injected failures must have happened")
		})
	}
}

func TestCheckArchive(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []struct {
		name                string
		encrypted, manifest bool
	}{{"plain", false, true}, {"encrypted", true, true}, {"encrypted, no manifest", true, false}} {
		t.Run(mode.name, func(t *testing.T) {
			dir, store, keyOf, enc := buildArchive(t, 55, mode.encrypted, mode.manifest)
			opts := ArchiveCheckOptions{Store: store, Prefix: "p", ArchiveEncryption: enc, Dir: dir}
			deep := opts
			deep.Deep = true

			res, err := CheckArchive(ctx, deep)
			require.NoError(t, err)
			require.True(t, res.OK(), "%+v", res)
			require.Equal(t, uint64(55), res.LocalLastIndex)
			require.Empty(t, res.LocalPending)
			if mode.manifest {
				require.Equal(t, 6, res.Segments)
				require.Equal(t, uint64(1), res.FirstIndex)
				require.Equal(t, uint64(55), res.LastIndex)
			}

			// Damage inside an object: only the deep check can see it.
			good := store.obj(keyOf(11))
			bad := append([]byte(nil), good...)
			bad[len(bad)/2] ^= 0xff
			store.set(keyOf(11), bad)
			res, err = CheckArchive(ctx, opts)
			require.NoError(t, err)
			require.True(t, res.OK())
			res, err = CheckArchive(ctx, deep)
			require.NoError(t, err)
			require.False(t, res.OK())
			require.Len(t, res.Problems, 1)
			require.False(t, res.Problems[0].Missing)
			require.Equal(t, IndexRange{11, 20}, res.Problems[0].IndexRange)
			require.Equal(t, []IndexRange{{11, 20}}, res.LocalLost)

			// An object gone entirely, plus the damaged one: repair un-marks
			// both local segments…
			store.del(keyOf(31))
			rep := deep
			rep.Repair = true
			res, err = CheckArchive(ctx, rep)
			require.NoError(t, err)
			require.Equal(t, []IndexRange{{11, 20}, {31, 40}}, res.LocalLost)
			require.Equal(t, res.LocalLost, res.Repaired)
			require.False(t, isArchived(dir, segName{FirstIndex: 31, LastIndex: 40, Closed: true}))

			// …and the next writer run uploads them again.
			cfg := &ArchiveConfig{Store: store, Prefix: "p", Retry: fastRetry(), DisableManifest: !mode.manifest,
				Encryption: enc, DrainOnClose: 5 * time.Second}
			w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentRecords: 10, CompressOnRotate: true, Archive: cfg})
			require.NoError(t, err)
			waitFor(t, "re-upload", func() bool { return w.Stats().ArchiveUploads >= 2 })
			require.NoError(t, w.Close())
			res, err = CheckArchive(ctx, deep)
			require.NoError(t, err)
			require.True(t, res.OK(), "%+v", res)
			require.True(t, bytes.Equal(good, store.obj(keyOf(11))), "re-upload must restore the identical object")

			// And the log reads back complete.
			got, _, err := readArchive(t, store, ArchiveReaderOptions{ArchiveEncryption: enc})
			require.NoError(t, err)
			require.Equal(t, seq(1, 55), got)
		})
	}

	// A hole in the manifest, and a segment still waiting for upload.
	dir, store, _, _ := buildArchive(t, 40, false, true)
	m, err := LoadArchiveManifest(ctx, store, "p")
	require.NoError(t, err)
	m.Segments = append(m.Segments[:1], m.Segments[2:]...)
	body, _ := json.Marshal(m)
	store.set("p/"+archiveManifestName, body)
	require.NoError(t, removeArchivedSidecar(dir, segName{FirstIndex: 31, LastIndex: 40, Closed: true}))
	res, err := CheckArchive(ctx, ArchiveCheckOptions{Store: store, Prefix: "p", Dir: dir})
	require.NoError(t, err)
	require.False(t, res.OK())
	require.Equal(t, []IndexRange{{11, 20}}, res.Gaps)
	require.Equal(t, []IndexRange{{11, 20}}, res.LocalLost, "marked on disk, unknown to the catalog")
	require.Equal(t, []IndexRange{{31, 40}}, res.LocalPending)
}
