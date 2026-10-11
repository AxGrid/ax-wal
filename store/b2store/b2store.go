// Package b2store is an axwal.ArchiveStore for Backblaze B2, speaking the
// native B2 API (v2) over net/http — no SDK dependency.
//
// Uploads stream from the source: small files go through b2_upload_file with
// the SHA-1 in the request (B2 rejects the upload if the bytes don't match),
// larger ones through the large-file API, one part at a time.
package b2store

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	axwal "github.com/axgrid/ax-wal"
)

const (
	defaultBaseURL = "https://api.backblazeb2.com"

	// DefaultPartSize is the part size for large-file uploads; files up to
	// DefaultLargeThreshold are sent in a single request.
	DefaultPartSize       = 32 << 20
	DefaultLargeThreshold = 128 << 20

	minPartSize = 5 << 20 // B2's absolute minimum for all parts but the last
)

// Store is a B2 bucket. Safe for concurrent use.
type Store struct {
	keyID, appKey, bucket string

	baseURL        string
	hc             *http.Client
	partSize       int64
	largeThreshold int64

	mu   sync.Mutex
	auth *authState // nil until authorized / after a 401
}

type authState struct {
	token       string
	apiURL      string
	downloadURL string
	accountID   string
	bucketID    string
}

// Option customizes a Store.
type Option func(*Store)

// WithHTTPClient sets the HTTP client (default: a client without overall
// timeout — uploads are bounded by the caller's context).
func WithHTTPClient(c *http.Client) Option { return func(s *Store) { s.hc = c } }

// WithPartSize sets the large-file part size (minimum 5 MiB).
func WithPartSize(n int64) Option {
	return func(s *Store) {
		if n >= minPartSize {
			s.partSize = n
		}
	}
}

// WithLargeThreshold sets the size above which the large-file API is used.
func WithLargeThreshold(n int64) Option { return func(s *Store) { s.largeThreshold = n } }

// WithBaseURL overrides the B2 API endpoint (tests, proxies).
func WithBaseURL(u string) Option { return func(s *Store) { s.baseURL = strings.TrimRight(u, "/") } }

// New returns a store for bucket using an application key. Nothing is
// contacted until the first call.
func New(keyID, appKey, bucket string, opts ...Option) *Store {
	s := &Store{
		keyID: keyID, appKey: appKey, bucket: bucket,
		baseURL:        defaultBaseURL,
		hc:             &http.Client{},
		partSize:       DefaultPartSize,
		largeThreshold: DefaultLargeThreshold,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// apiError is a non-2xx B2 response.
type apiError struct {
	Status  int    `json:"status"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *apiError) Error() string {
	return fmt.Sprintf("b2: %d %s: %s", e.Status, e.Code, e.Message)
}

func readAPIError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	e := &apiError{Status: resp.StatusCode}
	if json.Unmarshal(body, e) != nil || e.Code == "" {
		e.Status = resp.StatusCode
		e.Code = http.StatusText(resp.StatusCode)
		e.Message = strings.TrimSpace(string(body))
	}
	return e
}

func isUnauthorized(err error) bool {
	var e *apiError
	return errors.As(err, &e) && e.Status == http.StatusUnauthorized
}

// authorize returns the cached session, logging in if needed.
func (s *Store) authorize(ctx context.Context) (*authState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.auth != nil {
		return s.auth, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/b2api/v2/b2_authorize_account", nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(s.keyID, s.appKey)
	resp, err := s.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("b2 authorize: %w", readAPIError(resp))
	}
	var out struct {
		AccountID          string `json:"accountId"`
		AuthorizationToken string `json:"authorizationToken"`
		APIURL             string `json:"apiUrl"`
		DownloadURL        string `json:"downloadUrl"`
		Allowed            struct {
			BucketID   string `json:"bucketId"`
			BucketName string `json:"bucketName"`
		} `json:"allowed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("b2 authorize: %w", err)
	}
	a := &authState{
		token:       out.AuthorizationToken,
		apiURL:      strings.TrimRight(out.APIURL, "/"),
		downloadURL: strings.TrimRight(out.DownloadURL, "/"),
		accountID:   out.AccountID,
	}
	// A key restricted to one bucket already tells us its ID.
	if out.Allowed.BucketName == s.bucket && out.Allowed.BucketID != "" {
		a.bucketID = out.Allowed.BucketID
	} else {
		var lb struct {
			Buckets []struct {
				BucketID   string `json:"bucketId"`
				BucketName string `json:"bucketName"`
			} `json:"buckets"`
		}
		err := postJSON(ctx, s.hc, a.apiURL+"/b2api/v2/b2_list_buckets", a.token,
			map[string]any{"accountId": a.accountID, "bucketName": s.bucket}, &lb)
		if err != nil {
			return nil, fmt.Errorf("b2 list buckets: %w", err)
		}
		for _, b := range lb.Buckets {
			if b.BucketName == s.bucket {
				a.bucketID = b.BucketID
			}
		}
		if a.bucketID == "" {
			return nil, fmt.Errorf("b2: bucket %q not found (or not allowed for this key)", s.bucket)
		}
	}
	s.auth = a
	return a, nil
}

func (s *Store) dropAuth(a *authState) {
	s.mu.Lock()
	if s.auth == a {
		s.auth = nil
	}
	s.mu.Unlock()
}

// call runs an authorized JSON API call, re-authorizing once if the session
// token has expired.
func (s *Store) call(ctx context.Context, op string, in, out any) error {
	for attempt := 0; ; attempt++ {
		a, err := s.authorize(ctx)
		if err != nil {
			return err
		}
		err = postJSON(ctx, s.hc, a.apiURL+"/b2api/v2/"+op, a.token, in, out)
		if isUnauthorized(err) && attempt == 0 {
			s.dropAuth(a)
			continue
		}
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		return nil
	}
}

func postJSON(ctx context.Context, hc *http.Client, u, token string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return readAPIError(resp)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// upload POSTs a body to an upload URL obtained from B2.
func (s *Store) upload(ctx context.Context, u, token string, hdr map[string]string, body io.Reader, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, io.NopCloser(body))
	if err != nil {
		return err
	}
	req.ContentLength = size
	req.Header.Set("Authorization", token)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return readAPIError(resp)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// Put uploads body under key. The body is streamed; for large files each part
// is read straight from body when it implements io.ReaderAt (an *os.File
// does), otherwise one part at a time is buffered in memory.
func (s *Store) Put(ctx context.Context, key string, body io.Reader, info axwal.ArchivePutInfo) error {
	a, err := s.authorize(ctx)
	if err != nil {
		return err
	}
	if info.Size > s.largeThreshold {
		return s.putLarge(ctx, a, key, body, info)
	}

	sum := info.SHA1
	if sum == "" {
		// Without a precomputed digest the body has to be buffered to hash it.
		b, err := io.ReadAll(body)
		if err != nil {
			return err
		}
		h := sha1.Sum(b)
		sum, body = hex.EncodeToString(h[:]), bytes.NewReader(b)
	}
	var up struct {
		UploadURL          string `json:"uploadUrl"`
		AuthorizationToken string `json:"authorizationToken"`
	}
	if err := s.call(ctx, "b2_get_upload_url", map[string]any{"bucketId": a.bucketID}, &up); err != nil {
		return err
	}
	err = s.upload(ctx, up.UploadURL, up.AuthorizationToken, map[string]string{
		"X-Bz-File-Name":    encodeName(key),
		"Content-Type":      "b2/x-auto",
		"X-Bz-Content-Sha1": sum,
	}, body, info.Size)
	if err != nil {
		if isUnauthorized(err) {
			s.dropAuth(a) // start the next attempt from a fresh login
		}
		return fmt.Errorf("b2_upload_file %s: %w", key, err)
	}
	return nil
}

func (s *Store) putLarge(ctx context.Context, a *authState, key string, body io.Reader, info axwal.ArchivePutInfo) (err error) {
	fileInfo := map[string]string{}
	if info.SHA1 != "" {
		fileInfo["large_file_sha1"] = info.SHA1
	}
	var started struct {
		FileID string `json:"fileId"`
	}
	if err := s.call(ctx, "b2_start_large_file", map[string]any{
		"bucketId": a.bucketID, "fileName": key, "contentType": "b2/x-auto", "fileInfo": fileInfo,
	}, &started); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			// Don't leave an unfinished large file accruing storage. The
			// caller's ctx may already be dead, so use a fresh one.
			cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = s.call(cctx, "b2_cancel_large_file", map[string]any{"fileId": started.FileID}, nil)
		}
	}()

	var up struct {
		UploadURL          string `json:"uploadUrl"`
		AuthorizationToken string `json:"authorizationToken"`
	}
	if err := s.call(ctx, "b2_get_upload_part_url", map[string]any{"fileId": started.FileID}, &up); err != nil {
		return err
	}

	ra, _ := body.(io.ReaderAt)
	var buf []byte
	var partSums []string
	for off, n := int64(0), 1; off < info.Size; n++ {
		size := min(s.partSize, info.Size-off)
		var part io.Reader
		h := sha1.New()
		if ra != nil {
			if _, err := io.Copy(h, io.NewSectionReader(ra, off, size)); err != nil {
				return fmt.Errorf("hash part %d: %w", n, err)
			}
			part = io.NewSectionReader(ra, off, size)
		} else {
			if buf == nil {
				buf = make([]byte, s.partSize)
			}
			if _, err := io.ReadFull(body, buf[:size]); err != nil {
				return fmt.Errorf("read part %d: %w", n, err)
			}
			h.Write(buf[:size])
			part = bytes.NewReader(buf[:size])
		}
		sum := hex.EncodeToString(h.Sum(nil))
		err := s.upload(ctx, up.UploadURL, up.AuthorizationToken, map[string]string{
			"X-Bz-Part-Number":  strconv.Itoa(n),
			"X-Bz-Content-Sha1": sum,
		}, part, size)
		if err != nil {
			return fmt.Errorf("b2_upload_part %s #%d: %w", key, n, err)
		}
		partSums = append(partSums, sum)
		off += size
	}
	return s.call(ctx, "b2_finish_large_file",
		map[string]any{"fileId": started.FileID, "partSha1Array": partSums}, nil)
}

// Get downloads an object by name.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	for attempt := 0; ; attempt++ {
		a, err := s.authorize(ctx)
		if err != nil {
			return nil, err
		}
		u := a.downloadURL + "/file/" + url.PathEscape(s.bucket) + "/" + encodeName(key)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", a.token)
		resp, err := s.hc.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusOK {
			return resp.Body, nil
		}
		err = readAPIError(resp)
		resp.Body.Close()
		switch {
		case resp.StatusCode == http.StatusNotFound:
			return nil, fmt.Errorf("%w: %s", axwal.ErrObjectNotFound, key)
		case resp.StatusCode == http.StatusUnauthorized && attempt == 0:
			s.dropAuth(a)
			continue
		}
		return nil, fmt.Errorf("b2 download %s: %w", key, err)
	}
}

// List returns all files whose name starts with prefix, with their SHA-1.
func (s *Store) List(ctx context.Context, prefix string) ([]axwal.ArchiveObject, error) {
	a, err := s.authorize(ctx)
	if err != nil {
		return nil, err
	}
	var out []axwal.ArchiveObject
	var start *string
	for {
		in := map[string]any{"bucketId": a.bucketID, "prefix": prefix, "maxCount": 1000}
		if start != nil {
			in["startFileName"] = *start
		}
		var page struct {
			Files []struct {
				FileName        string            `json:"fileName"`
				ContentLength   int64             `json:"contentLength"`
				ContentSha1     string            `json:"contentSha1"`
				UploadTimestamp int64             `json:"uploadTimestamp"`
				Action          string            `json:"action"`
				FileInfo        map[string]string `json:"fileInfo"`
			} `json:"files"`
			NextFileName *string `json:"nextFileName"`
		}
		if err := s.call(ctx, "b2_list_file_names", in, &page); err != nil {
			return nil, err
		}
		for _, f := range page.Files {
			if f.Action != "" && f.Action != "upload" {
				continue // folders, hide markers, unfinished large files
			}
			sum := strings.TrimPrefix(f.ContentSha1, "unverified:")
			if sum == "" || sum == "none" {
				sum = f.FileInfo["large_file_sha1"] // large files carry it here
			}
			out = append(out, axwal.ArchiveObject{
				Key:     f.FileName,
				Size:    f.ContentLength,
				ModTime: time.UnixMilli(f.UploadTimestamp),
				SHA1:    sum,
			})
		}
		if page.NextFileName == nil {
			return out, nil
		}
		start = page.NextFileName
	}
}

// encodeName percent-encodes a file name the way B2 wants it in headers and
// download URLs: UTF-8 bytes, "/" left as is.
func encodeName(name string) string {
	const safe = "-._~/!$'()*;=:@"
	var b strings.Builder
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte(safe, c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}
