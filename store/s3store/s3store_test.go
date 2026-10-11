package s3store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	axwal "github.com/axgrid/ax-wal"
	"github.com/stretchr/testify/require"
)

// Known-answer tests from the AWS "Signature Version 4" S3 examples.
func TestSigV4_AWSExamples(t *testing.T) {
	s, err := New(Config{
		Bucket: "examplebucket", Region: "us-east-1",
		AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	})
	require.NoError(t, err)
	s.now = func() time.Time { return time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC) }
	empty := hexSHA256(nil)

	for _, tc := range []struct{ query, sig string }{
		{"lifecycle=", "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"},
		{"max-keys=2&prefix=J", "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"},
	} {
		req, err := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/?"+tc.query, nil)
		require.NoError(t, err)
		s.sign(req, "/", empty)
		require.Contains(t, req.Header.Get("Authorization"), "Signature="+tc.sig, tc.query)
	}
}

// fakeS3 is a minimal path-style S3: PUT/GET object, ListObjectsV2, multipart.
type fakeS3 struct {
	mu      sync.Mutex
	objs    map[string][]byte
	uploads map[string]map[int][]byte
	aborted int
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=AK/") {
		http.Error(w, "<Error><Code>AccessDenied</Code></Error>", http.StatusForbidden)
		return
	}
	key := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/bkt"), "/")
	q := r.URL.Query()
	body, _ := io.ReadAll(r.Body)
	if want := r.Header.Get("x-amz-content-sha256"); want != unsignedPayload {
		if got := sha256.Sum256(body); hex.EncodeToString(got[:]) != want {
			http.Error(w, "<Error><Code>XAmzContentSHA256Mismatch</Code></Error>", http.StatusBadRequest)
			return
		}
	}
	switch {
	case r.Method == http.MethodGet && q.Get("list-type") == "2":
		type content struct {
			Key          string
			Size         int
			LastModified string
		}
		var keys []string
		for k := range f.objs {
			if strings.HasPrefix(k, q.Get("prefix")) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		// Page size 2 to exercise continuation.
		start := 0
		fmt.Sscanf(q.Get("continuation-token"), "%d", &start)
		res := struct {
			XMLName               xml.Name `xml:"ListBucketResult"`
			Contents              []content
			IsTruncated           bool
			NextContinuationToken string
		}{}
		for i := start; i < len(keys) && i < start+2; i++ {
			res.Contents = append(res.Contents, content{keys[i], len(f.objs[keys[i]]), "2026-10-11T10:00:00.000Z"})
		}
		if start+2 < len(keys) {
			res.IsTruncated, res.NextContinuationToken = true, fmt.Sprint(start+2)
		}
		xml.NewEncoder(w).Encode(res)
	case r.Method == http.MethodPost && q.Has("uploads"):
		id := fmt.Sprintf("up%d", len(f.uploads)+1)
		f.uploads[id] = map[int][]byte{}
		fmt.Fprintf(w, "<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>", id)
	case r.Method == http.MethodPut && q.Has("uploadId"):
		var n int
		fmt.Sscanf(q.Get("partNumber"), "%d", &n)
		f.uploads[q.Get("uploadId")][n] = body
		w.Header().Set("ETag", fmt.Sprintf(`"etag-%d"`, n))
	case r.Method == http.MethodPost && q.Has("uploadId"):
		var doc struct {
			Part []struct {
				PartNumber int
				ETag       string
			}
		}
		if err := xml.Unmarshal(body, &doc); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var whole []byte
		for _, p := range doc.Part {
			whole = append(whole, f.uploads[q.Get("uploadId")][p.PartNumber]...)
		}
		f.objs[key] = whole
		delete(f.uploads, q.Get("uploadId"))
		fmt.Fprint(w, "<CompleteMultipartUploadResult/>")
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		delete(f.uploads, q.Get("uploadId"))
		f.aborted++
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut:
		f.objs[key] = body
	case r.Method == http.MethodGet:
		b, ok := f.objs[key]
		if !ok {
			http.Error(w, "<Error><Code>NoSuchKey</Code></Error>", http.StatusNotFound)
			return
		}
		w.Write(b)
	default:
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}
}

func putInfo(b []byte) axwal.ArchivePutInfo {
	s1, s256 := sha1.Sum(b), sha256.Sum256(b)
	return axwal.ArchivePutInfo{Size: int64(len(b)), SHA1: hex.EncodeToString(s1[:]), SHA256: hex.EncodeToString(s256[:])}
}

func TestStore_AgainstFake(t *testing.T) {
	fake := &fakeS3{objs: map[string][]byte{}, uploads: map[string]map[int][]byte{}}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	s, err := New(Config{Bucket: "bkt", Endpoint: srv.URL, AccessKey: "AK", SecretKey: "SK",
		PartSize: 5 << 20, MultipartThreshold: 6 << 20})
	require.NoError(t, err)
	ctx := context.Background()

	small := []byte("hello segment")
	require.NoError(t, s.Put(ctx, "wal/2026/10/11/wal-1-10.zst", bytes.NewReader(small), putInfo(small)))

	big := make([]byte, 11<<20) // 3 parts: 5 + 5 + 1 MiB
	_, _ = rand.Read(big)
	require.NoError(t, s.Put(ctx, "wal/big seg+1", bytes.NewReader(big), putInfo(big)))
	// Same through the buffering path (body without ReadAt).
	require.NoError(t, s.Put(ctx, "wal/big2", io.MultiReader(bytes.NewReader(big)), putInfo(big)))
	require.Empty(t, fake.uploads)
	require.True(t, bytes.Equal(big, fake.objs["wal/big seg+1"]))
	require.True(t, bytes.Equal(big, fake.objs["wal/big2"]))

	// A body that doesn't match its declared SHA-256 is rejected.
	bad := putInfo(small)
	bad.SHA256 = strings.Repeat("0", 64)
	require.Error(t, s.Put(ctx, "wal/bad", bytes.NewReader(small), bad))
	require.NotContains(t, fake.objs, "wal/bad")

	rc, err := s.Get(ctx, "wal/2026/10/11/wal-1-10.zst")
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	require.Equal(t, small, got)

	_, err = s.Get(ctx, "wal/missing")
	require.ErrorIs(t, err, axwal.ErrObjectNotFound)

	objs, err := s.List(ctx, "wal/")
	require.NoError(t, err)
	require.Len(t, objs, 3)
	require.Equal(t, "wal/2026/10/11/wal-1-10.zst", objs[0].Key)
	require.Equal(t, int64(len(small)), objs[0].Size)
	require.Equal(t, 2026, objs[0].ModTime.Year())
}
