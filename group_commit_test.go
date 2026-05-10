package axwal

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGroupCommit_Correctness(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(WriterOptions{
		Dir:            dir,
		MaxSegmentSize: 1 << 20,
		GroupCommit: GroupCommitConfig{
			Enabled:    true,
			MaxLatency: 2 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	// Concurrent writers; each Append should still see its records durable on return.
	const writers = 8
	const perWriter = 200

	var wg sync.WaitGroup
	wg.Add(writers)
	for w_i := 0; w_i < writers; w_i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				_, err := w.Append([]byte(fmt.Sprintf("w%d-%d", id, j)))
				require.NoError(t, err)
			}
		}(w_i)
	}
	wg.Wait()
	require.Equal(t, uint64(writers*perWriter), w.LastIndex())
	require.NoError(t, w.Close())

	// Read everything back: indices 1..writers*perWriter present, no gaps.
	r, err := OpenReader(ReaderOptions{Dir: dir, Name: "verify", StartFrom: StartEarliest})
	require.NoError(t, err)
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	seen := make(map[uint64]bool, writers*perWriter)
	for i := 0; i < writers*perWriter; i++ {
		_, idx, err := r.Next(ctx)
		require.NoError(t, err)
		require.False(t, seen[idx], "duplicate index %d", idx)
		seen[idx] = true
	}
	require.Equal(t, writers*perWriter, len(seen))
}

func BenchmarkAppend_PerCallFsync(b *testing.B) {
	benchAppend(b, false)
}

func BenchmarkAppend_GroupCommit(b *testing.B) {
	benchAppend(b, true)
}

func benchAppend(b *testing.B, groupCommit bool) {
	dir := b.TempDir()
	opts := WriterOptions{
		Dir:            dir,
		MaxSegmentSize: 64 << 20,
	}
	if groupCommit {
		opts.GroupCommit = GroupCommitConfig{
			Enabled:    true,
			MaxLatency: time.Millisecond,
		}
	}
	w, err := OpenWriter(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	payload := make([]byte, 128)
	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := w.Append(payload)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
