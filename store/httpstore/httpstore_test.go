package httpstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	axwal "github.com/axgrid/ax-wal"
	"github.com/axgrid/ax-wal/store/fsstore"
	"github.com/axgrid/ax-wal/store/httpstore"
	"github.com/stretchr/testify/require"
)

// The whole point: a writer uploads an encrypted archive; somebody with
// nothing but the key and a public URL reads the log back as a stream.
func TestPublicEncryptedArchive(t *testing.T) {
	for _, manifest := range []bool{true, false} {
		t.Run(fmt.Sprintf("manifest=%v", manifest), func(t *testing.T) {
			dir, bucket := t.TempDir(), t.TempDir()
			key := bytes.Repeat([]byte{7}, 32)
			w, err := axwal.OpenWriter(axwal.WriterOptions{
				Dir:               dir,
				MaxSegmentRecords: 50,
				CompressOnRotate:  true,
				Archive: &axwal.ArchiveConfig{
					Store: fsstore.New(bucket), Prefix: "wal/kiosk",
					Encryption:      &axwal.ArchiveEncryption{Key: key},
					DisableManifest: !manifest,
					DrainOnClose:    10 * time.Second,
				},
			})
			require.NoError(t, err)
			for i := 1; i <= 220; i++ {
				_, err := w.Append([]byte(fmt.Sprintf(`{"n":%d}`, i)))
				require.NoError(t, err)
			}
			require.NoError(t, w.Rotate())

			// The "public bucket": a static file server. It can't list, and
			// it counts what is asked of it.
			var gets, lists int
			files := http.FileServer(http.Dir(bucket))
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				if fi, err := os.Stat(filepath.Join(bucket, filepath.FromSlash(req.URL.Path))); err == nil && fi.IsDir() {
					lists++
					http.Error(rw, "listing disabled", http.StatusForbidden)
					return
				}
				gets++
				files.ServeHTTP(rw, req)
			}))
			defer srv.Close()

			store := httpstore.New(srv.URL+"/wal/kiosk/", nil)
			require.ErrorIs(t, store.Put(context.Background(), "x", nil, axwal.ArchivePutInfo{}), httpstore.ErrReadOnly)
			_, err = store.Get(context.Background(), "nope")
			require.ErrorIs(t, err, axwal.ErrObjectNotFound)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			waitArchived := func(n int) {
				require.Eventually(t, func() bool { return w.Stats().ArchiveUploads >= uint64(n) },
					10*time.Second, 10*time.Millisecond)
			}
			waitArchived(5)

			r, err := axwal.OpenArchiveReader(ctx, axwal.ArchiveReaderOptions{
				Store: store, ArchiveEncryption: &axwal.ArchiveEncryption{Key: key},
				Follow: true, PollInterval: 20 * time.Millisecond,
			})
			require.NoError(t, err)
			defer r.Close()
			for want := uint64(1); want <= 220; want++ {
				p, idx, err := r.Next(ctx)
				require.NoError(t, err)
				require.Equal(t, want, idx)
				require.Equal(t, fmt.Sprintf(`{"n":%d}`, idx), string(p))
			}

			// The log grows; the follower picks the new segments up.
			for i := 221; i <= 300; i++ {
				_, err := w.Append([]byte(fmt.Sprintf(`{"n":%d}`, i)))
				require.NoError(t, err)
			}
			require.NoError(t, w.Close())
			for want := uint64(221); want <= 300; want++ {
				p, idx, err := r.Next(ctx)
				require.NoError(t, err)
				require.Equal(t, want, idx)
				require.Equal(t, fmt.Sprintf(`{"n":%d}`, idx), string(p))
			}
			require.Zero(t, lists, "the reader must never need a listing")
			require.Positive(t, gets)

			// Without the key the same URL yields nothing useful.
			_, err = axwal.OpenArchiveReader(ctx, axwal.ArchiveReaderOptions{Store: store})
			require.Error(t, err)
			require.True(t, errors.Is(err, httpstore.ErrReadOnly), "keyless open needs a listing: %v", err)

			// Bare-hands access, as documented: name from key + index,
			// decrypt the body as it downloads.
			name, err := axwal.ArchiveObjectName(key, 1)
			require.NoError(t, err)
			rc, err := store.Get(ctx, name)
			require.NoError(t, err)
			defer rc.Close()
			plain, err := axwal.NewArchiveDecrypter(key, rc)
			require.NoError(t, err)
			head := make([]byte, 4)
			_, err = io.ReadFull(plain, head)
			require.NoError(t, err)
			require.Equal(t, []byte{0x28, 0xB5, 0x2F, 0xFD}, head, "the plaintext is the zstd segment file")
		})
	}
}
