package axwal

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testArchiveCrypto(t *testing.T, fill byte) *archiveCrypto {
	t.Helper()
	c, err := newArchiveCrypto(&ArchiveEncryption{Key: bytes.Repeat([]byte{fill}, 32)})
	require.NoError(t, err)
	return c
}

func TestArchiveCrypto_StreamRoundtripAndTamper(t *testing.T) {
	c := testArchiveCrypto(t, 1)
	salt := bytes.Repeat([]byte{9}, archiveEncSaltSize)
	const tag = 16
	hdr := len(archiveEncMagic) + archiveEncSaltSize

	for _, size := range []int{0, 1, archiveEncChunk - 1, archiveEncChunk, archiveEncChunk + 1, 3*archiveEncChunk + 5} {
		plain := make([]byte, size)
		_, _ = rand.Read(plain)
		var enc bytes.Buffer
		require.NoError(t, c.encrypt(&enc, bytes.NewReader(plain), salt))
		chunks := (size + archiveEncChunk - 1) / archiveEncChunk
		if chunks == 0 {
			chunks = 1
		}
		require.Equal(t, hdr+size+chunks*tag, enc.Len(), "size %d", size)
		if size > 64 {
			require.False(t, bytes.Contains(enc.Bytes(), plain[:64]))
		}

		// Decrypts as a stream, in small reads.
		got, err := io.ReadAll(io.LimitReader(c.decryptReader(iotestOneByte{bytes.NewReader(enc.Bytes())}), int64(size)+10))
		require.NoError(t, err)
		require.True(t, bytes.Equal(plain, got), "size %d", size)

		// Same salt → same bytes (what makes upload retries recognizable).
		var again bytes.Buffer
		require.NoError(t, c.encrypt(&again, bytes.NewReader(plain), salt))
		require.True(t, bytes.Equal(enc.Bytes(), again.Bytes()))

		mustFail := func(what string, data []byte, key *archiveCrypto) {
			_, err := io.ReadAll(key.decryptReader(bytes.NewReader(data)))
			require.ErrorIs(t, err, ErrEncryption, "%s, size %d", what, size)
		}
		mustFail("wrong key", enc.Bytes(), testArchiveCrypto(t, 2))
		flipped := append([]byte(nil), enc.Bytes()...)
		flipped[len(flipped)-1] ^= 1
		mustFail("flipped bit", flipped, c)
		mustFail("truncated mid-chunk", enc.Bytes()[:enc.Len()-1], c)
		mustFail("header only", enc.Bytes()[:hdr], c)
		full := archiveEncChunk + tag
		if chunks > 1 {
			mustFail("last chunk cut off", enc.Bytes()[:hdr+(chunks-1)*full], c)
		}
		if chunks > 2 {
			swapped := append([]byte(nil), enc.Bytes()...)
			copy(swapped[hdr:hdr+full], enc.Bytes()[hdr+full:hdr+2*full])
			copy(swapped[hdr+full:hdr+2*full], enc.Bytes()[hdr:hdr+full])
			mustFail("chunks reordered", swapped, c)
		}
	}
	_, err := io.ReadAll(c.decryptReader(strings.NewReader("definitely not an encrypted object....")))
	require.ErrorIs(t, err, ErrEncryption)
	_, err = newArchiveCrypto(&ArchiveEncryption{Key: []byte("short")})
	require.ErrorIs(t, err, ErrEncryption)
}

// iotestOneByte hands out one byte per Read.
type iotestOneByte struct{ r io.Reader }

func (o iotestOneByte) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return o.r.Read(p[:1])
}

func TestArchiveCrypto_Names(t *testing.T) {
	a, b := testArchiveCrypto(t, 1), testArchiveCrypto(t, 2)
	hex32 := regexp.MustCompile(`^[0-9a-f]{32}$`)
	seen := map[string]bool{}
	for i := uint64(1); i <= 2000; i++ {
		n := a.segmentName(i)
		require.Regexp(t, hex32, n)
		require.False(t, seen[n])
		seen[n] = true
	}
	require.Equal(t, a.segmentName(42), a.segmentName(42))
	require.NotEqual(t, a.segmentName(42), b.segmentName(42))
	require.NotEqual(t, a.manifestName(), b.manifestName())
	require.False(t, seen[a.manifestName()])

	pub, err := ArchiveObjectName(bytes.Repeat([]byte{1}, 32), 42)
	require.NoError(t, err)
	require.Equal(t, a.segmentName(42), pub)
}

func TestArchiver_Encrypted(t *testing.T) {
	dir := t.TempDir()
	store := newMemStore()
	key := bytes.Repeat([]byte{5}, 32)
	failManifestOnce := true
	store.failKey = func(k string) bool {
		c, _ := newArchiveCrypto(&ArchiveEncryption{Key: key})
		if failManifestOnce && strings.HasSuffix(k, c.manifestName()) {
			failManifestOnce = false
			return true
		}
		return false
	}
	w, err := OpenWriter(WriterOptions{
		Dir:               dir,
		MaxSegmentRecords: 10,
		CompressOnRotate:  true,
		Archive: &ArchiveConfig{
			Store: store, Prefix: "pub", Layout: LayoutByDay, Retry: fastRetry(), Concurrency: 1,
			Encryption:   &ArchiveEncryption{Key: key},
			DrainOnClose: 5 * time.Second,
		},
	})
	require.NoError(t, err)
	for i := 1; i <= 45; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("secret-record-%d", i)))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	require.True(t, allArchived(t, dir))
	require.Empty(t, listFilenames(t, dir, archiveUploadTmpPrefix+"*"), "temp files must be cleaned up")

	// What an outsider sees: 5 segments + manifest, all under opaque names
	// directly below the prefix (no dates), nothing readable inside.
	crypto := testArchiveCrypto(t, 5)
	name := regexp.MustCompile(`^pub/[0-9a-f]{32}$`)
	objs, err := store.List(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, objs, 6)
	for _, o := range objs {
		require.Regexp(t, name, o.Key)
		body := store.obj(o.Key)
		require.True(t, bytes.HasPrefix(body, archiveEncMagic))
		for _, leak := range []string{"secret-record", "wal-", "first_index", "manifest"} {
			require.NotContains(t, string(body), leak, o.Key)
		}
	}
	for _, first := range []uint64{1, 11, 21, 31, 41} {
		require.NotNil(t, store.obj("pub/"+crypto.segmentName(first)), "segment starting at %d", first)
	}
	// The manifest write failed once; the retry must not re-send the segment.
	require.Equal(t, 1, store.putCount("pub/"+crypto.segmentName(1)))

	read := func(opts ArchiveReaderOptions) ([]uint64, error) {
		opts.Store, opts.Prefix = store, "pub"
		r, err := OpenArchiveReader(context.Background(), opts)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		var got []uint64
		for {
			p, idx, err := r.Next(context.Background())
			if err == io.EOF {
				return got, nil
			}
			if err != nil {
				return got, err
			}
			require.Equal(t, fmt.Sprintf("secret-record-%d", idx), string(p))
			got = append(got, idx)
		}
	}
	enc := &ArchiveEncryption{Key: key}

	all, err := read(ArchiveReaderOptions{ArchiveEncryption: enc})
	require.NoError(t, err)
	require.Len(t, all, 45)
	part, err := read(ArchiveReaderOptions{ArchiveEncryption: enc, FromIndex: 17, ToIndex: 33})
	require.NoError(t, err)
	require.Len(t, part, 17)
	require.Equal(t, uint64(17), part[0])

	m, err := LoadEncryptedArchiveManifest(context.Background(), store, "pub", *enc)
	require.NoError(t, err)
	require.Len(t, m.Segments, 5)
	require.Equal(t, archiveEncAlgo, m.Segments[0].Encryption)
	require.Equal(t, int64(len(store.obj(m.Segments[0].Key))), m.Segments[0].SizeBytes)

	// No key / wrong key: nothing comes out.
	_, err = read(ArchiveReaderOptions{})
	require.NoError(t, err) // a keyless listing finds no segment-shaped names at all
	got, _ := read(ArchiveReaderOptions{})
	require.Empty(t, got)
	_, err = read(ArchiveReaderOptions{ArchiveEncryption: &ArchiveEncryption{Key: bytes.Repeat([]byte{6}, 32)}})
	require.ErrorContains(t, err, "wrong archive key")
	require.ErrorIs(t, err, ErrObjectNotFound)

	// Without the manifest the reader walks the chain of derived names.
	delete(store.objs, "pub/"+crypto.manifestName())
	chain, err := read(ArchiveReaderOptions{ArchiveEncryption: enc, FromIndex: 8})
	require.NoError(t, err)
	require.Len(t, chain, 38)
	require.Equal(t, uint64(8), chain[0])
	require.Equal(t, uint64(45), chain[37])
	_, err = read(ArchiveReaderOptions{ArchiveEncryption: enc, FromTime: time.Now().Add(-time.Hour)})
	require.ErrorContains(t, err, "no manifest")
}
