package b2store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"

	axwal "github.com/axgrid/ax-wal"
	"github.com/stretchr/testify/require"
)

type fakeFile struct {
	data []byte
	sha1 string // as B2 would report it in contentSha1
	info map[string]string
}

// fakeB2 implements the slice of the B2 native API the store uses.
type fakeB2 struct {
	url string

	mu        sync.Mutex
	files     map[string]*fakeFile
	large     map[string]*fakeFile // fileId -> pending large file (name in info["name"])
	parts     map[string]map[string][]byte
	logins    int
	expireOne bool // reject the next API call once with expired_auth_token
	canceled  int
}

func (f *fakeB2) fail(w http.ResponseWriter, status int, code string) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"status": status, "code": code, "message": code})
}

func (f *fakeB2) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	var in map[string]any
	_ = json.Unmarshal(body, &in)
	token := fmt.Sprintf("tok%d", f.logins)
	op := strings.TrimPrefix(r.URL.Path, "/b2api/v2/")

	if op == "b2_authorize_account" {
		if u, p, ok := r.BasicAuth(); !ok || u != "keyid" || p != "appkey" {
			f.fail(w, 401, "bad_auth_token")
			return
		}
		f.logins++
		json.NewEncoder(w).Encode(map[string]any{
			"accountId": "acc", "authorizationToken": fmt.Sprintf("tok%d", f.logins),
			"apiUrl": f.url, "downloadUrl": f.url,
			"allowed": map[string]any{"bucketId": "bid", "bucketName": "bkt"},
		})
		return
	}

	sum := sha1.Sum(body)
	gotSHA := hex.EncodeToString(sum[:])
	switch {
	case strings.HasPrefix(r.URL.Path, "/upload/"):
		if r.Header.Get("Authorization") != "uptok" {
			f.fail(w, 401, "bad_auth_token")
			return
		}
		if r.Header.Get("X-Bz-Content-Sha1") != gotSHA {
			f.fail(w, 400, "bad_request")
			return
		}
		name, _ := url.PathUnescape(r.Header.Get("X-Bz-File-Name"))
		f.files[name] = &fakeFile{data: body, sha1: gotSHA}
		w.Write([]byte("{}"))
		return
	case strings.HasPrefix(r.URL.Path, "/part/"):
		if r.Header.Get("X-Bz-Content-Sha1") != gotSHA {
			f.fail(w, 400, "bad_request")
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/part/")
		f.parts[id][r.Header.Get("X-Bz-Part-Number")] = body
		w.Write([]byte("{}"))
		return
	case strings.HasPrefix(r.URL.Path, "/file/bkt/"):
		name, _ := url.PathUnescape(strings.TrimPrefix(r.URL.EscapedPath(), "/file/bkt/"))
		file, ok := f.files[name]
		if !ok {
			f.fail(w, 404, "not_found")
			return
		}
		w.Write(file.data)
		return
	}

	// Authorized JSON API calls.
	if f.expireOne {
		f.expireOne = false
		f.logins++ // the old token is now invalid
		f.fail(w, 401, "expired_auth_token")
		return
	}
	if r.Header.Get("Authorization") != token {
		f.fail(w, 401, "bad_auth_token")
		return
	}
	switch op {
	case "b2_get_upload_url":
		json.NewEncoder(w).Encode(map[string]any{"uploadUrl": f.url + "/upload/x", "authorizationToken": "uptok"})
	case "b2_start_large_file":
		id := fmt.Sprintf("large%d", len(f.large)+1)
		info := map[string]string{"name": in["fileName"].(string)}
		for k, v := range in["fileInfo"].(map[string]any) {
			info[k] = v.(string)
		}
		f.large[id] = &fakeFile{info: info}
		f.parts[id] = map[string][]byte{}
		json.NewEncoder(w).Encode(map[string]any{"fileId": id})
	case "b2_get_upload_part_url":
		json.NewEncoder(w).Encode(map[string]any{
			"uploadUrl": f.url + "/part/" + in["fileId"].(string), "authorizationToken": "uptok"})
	case "b2_finish_large_file":
		id := in["fileId"].(string)
		lf := f.large[id]
		for i := range in["partSha1Array"].([]any) {
			lf.data = append(lf.data, f.parts[id][fmt.Sprint(i+1)]...)
		}
		lf.sha1 = "none"
		f.files[lf.info["name"]] = lf
		delete(f.large, id)
		w.Write([]byte("{}"))
	case "b2_cancel_large_file":
		delete(f.large, in["fileId"].(string))
		f.canceled++
		w.Write([]byte("{}"))
	case "b2_list_file_names":
		prefix, _ := in["prefix"].(string)
		start, _ := in["startFileName"].(string)
		var names []string
		for n := range f.files {
			if strings.HasPrefix(n, prefix) && n >= start {
				names = append(names, n)
			}
		}
		sort.Strings(names)
		out := map[string]any{"nextFileName": nil}
		var files []map[string]any
		for i, n := range names {
			if i == 2 { // page size 2 to exercise pagination
				out["nextFileName"] = n
				break
			}
			files = append(files, map[string]any{
				"fileName": n, "contentLength": len(f.files[n].data), "contentSha1": f.files[n].sha1,
				"uploadTimestamp": 1791676800000, "action": "upload", "fileInfo": f.files[n].info,
			})
		}
		out["files"] = files
		json.NewEncoder(w).Encode(out)
	default:
		f.fail(w, 400, "unexpected_"+op)
	}
}

func putInfo(b []byte) axwal.ArchivePutInfo {
	s := sha1.Sum(b)
	return axwal.ArchivePutInfo{Size: int64(len(b)), SHA1: hex.EncodeToString(s[:])}
}

func TestStore_AgainstFake(t *testing.T) {
	fake := &fakeB2{files: map[string]*fakeFile{}, large: map[string]*fakeFile{}, parts: map[string]map[string][]byte{}}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	fake.url = srv.URL
	s := New("keyid", "appkey", "bkt", WithBaseURL(srv.URL), WithPartSize(5<<20), WithLargeThreshold(6<<20))
	ctx := context.Background()

	small := []byte("hello segment")
	require.NoError(t, s.Put(ctx, "wal/2026/10/11/wal-1-10.zst", bytes.NewReader(small), putInfo(small)))

	// The session token expires: the next call logs in again transparently.
	fake.expireOne = true
	require.NoError(t, s.Put(ctx, "wal/имя с пробелом", bytes.NewReader(small), putInfo(small)))
	require.Equal(t, 3, fake.logins) // initial + forced expiry + re-login
	require.Contains(t, fake.files, "wal/имя с пробелом")

	big := make([]byte, 11<<20) // 3 parts: 5 + 5 + 1 MiB
	_, _ = rand.Read(big)
	require.NoError(t, s.Put(ctx, "wal/big", bytes.NewReader(big), putInfo(big)))
	require.NoError(t, s.Put(ctx, "wal/big2", io.MultiReader(bytes.NewReader(big)), putInfo(big)))
	require.True(t, bytes.Equal(big, fake.files["wal/big"].data))
	require.True(t, bytes.Equal(big, fake.files["wal/big2"].data))

	// Wrong SHA-1: B2 refuses the upload.
	bad := putInfo(small)
	bad.SHA1 = strings.Repeat("0", 40)
	require.Error(t, s.Put(ctx, "wal/bad", bytes.NewReader(small), bad))
	require.NotContains(t, fake.files, "wal/bad")

	rc, err := s.Get(ctx, "wal/имя с пробелом")
	require.NoError(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	require.Equal(t, small, got)

	_, err = s.Get(ctx, "wal/missing")
	require.ErrorIs(t, err, axwal.ErrObjectNotFound)

	objs, err := s.List(ctx, "wal/")
	require.NoError(t, err)
	require.Len(t, objs, 4)
	byKey := map[string]axwal.ArchiveObject{}
	for _, o := range objs {
		byKey[o.Key] = o
	}
	require.Equal(t, putInfo(small).SHA1, byKey["wal/2026/10/11/wal-1-10.zst"].SHA1)
	// Large files report "none" as contentSha1; the digest comes from fileInfo.
	require.Equal(t, putInfo(big).SHA1, byKey["wal/big"].SHA1)
	require.Equal(t, int64(len(big)), byKey["wal/big"].Size)
}

func TestStore_BadCredentials(t *testing.T) {
	fake := &fakeB2{files: map[string]*fakeFile{}}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	s := New("keyid", "wrong", "bkt", WithBaseURL(srv.URL))
	_, err := s.List(context.Background(), "")
	require.ErrorContains(t, err, "401")
}
