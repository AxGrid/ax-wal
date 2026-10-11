package fsstore_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	axwal "github.com/axgrid/ax-wal"
	"github.com/axgrid/ax-wal/store/fsstore"
	"github.com/stretchr/testify/require"
)

func TestStore_Basics(t *testing.T) {
	root := t.TempDir()
	s := fsstore.New(root)
	ctx := context.Background()

	body := []byte("payload")
	require.Error(t, s.Put(ctx, "a/b", bytes.NewReader(body), axwal.ArchivePutInfo{Size: 3}), "size mismatch")
	require.Error(t, s.Put(ctx, "../escape", bytes.NewReader(body), axwal.ArchivePutInfo{Size: 7}))
	require.NoError(t, s.Put(ctx, "a/b", bytes.NewReader(body), axwal.ArchivePutInfo{Size: 7}))
	require.NoError(t, s.Put(ctx, "a/c", bytes.NewReader(body), axwal.ArchivePutInfo{Size: 7}))
	require.NoError(t, s.Put(ctx, "ab", bytes.NewReader(body), axwal.ArchivePutInfo{Size: 7}))

	_, err := s.Get(ctx, "nope")
	require.ErrorIs(t, err, axwal.ErrObjectNotFound)

	objs, err := s.List(ctx, "a/")
	require.NoError(t, err)
	require.Len(t, objs, 2)
	objs, err = s.List(ctx, "a")
	require.NoError(t, err)
	require.Len(t, objs, 3)
	objs, err = s.List(ctx, "missing/dir/")
	require.NoError(t, err)
	require.Empty(t, objs)
}

// End to end through the public API: writer → archiver → directory store →
// archive reader.
func TestArchiveRoundtrip(t *testing.T) {
	dir, root := t.TempDir(), t.TempDir()
	store := fsstore.New(root)
	w, err := axwal.OpenWriter(axwal.WriterOptions{
		Dir:               dir,
		MaxSegmentRecords: 100,
		CompressOnRotate:  true,
		Archive: &axwal.ArchiveConfig{
			Store: store, Prefix: "wal/kiosk", Layout: axwal.LayoutByDay, DrainOnClose: 10 * time.Second,
		},
	})
	require.NoError(t, err)
	for i := 1; i <= 250; i++ {
		_, err := w.Append([]byte(fmt.Sprintf(`{"n":%d}`, i)))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	day := time.Now().UTC().Format("2006/01/02")
	for _, name := range []string{"wal-1-100.zst", "wal-101-200.zst", "wal-201-250.zst"} {
		require.FileExists(t, filepath.Join(root, "wal/kiosk", filepath.FromSlash(day), name))
		local, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		remote, err := os.ReadFile(filepath.Join(root, "wal/kiosk", filepath.FromSlash(day), name))
		require.NoError(t, err)
		require.Equal(t, local, remote)
	}
	require.FileExists(t, filepath.Join(root, "wal/kiosk/manifest.json"))

	ctx := context.Background()
	r, err := axwal.OpenArchiveReader(ctx, axwal.ArchiveReaderOptions{
		Store: store, Prefix: "wal/kiosk", FromIndex: 95, ToIndex: 205,
	})
	require.NoError(t, err)
	defer r.Close()
	require.Len(t, r.Segments(), 3)
	want := uint64(95)
	for {
		p, idx, err := r.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, want, idx)
		require.Equal(t, fmt.Sprintf(`{"n":%d}`, idx), string(p))
		want++
	}
	require.Equal(t, uint64(206), want)
}
