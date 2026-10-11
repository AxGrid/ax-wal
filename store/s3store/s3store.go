// Package s3store is an axwal.ArchiveStore for S3 and S3-compatible storage
// (AWS, MinIO, Backblaze B2's S3 endpoint, Cloudflare R2, …). It signs
// requests with AWS Signature V4 over net/http — no SDK dependency.
//
// Uploads stream from the source. A single PUT carries the body's SHA-256 in
// the signature, so the server rejects bytes that don't match; larger objects
// use multipart upload, one part at a time.
package s3store

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	axwal "github.com/axgrid/ax-wal"
)

const (
	// DefaultPartSize is the multipart part size; objects up to
	// DefaultMultipartThreshold go in a single PUT.
	DefaultPartSize           = 32 << 20
	DefaultMultipartThreshold = 128 << 20

	minPartSize     = 5 << 20
	unsignedPayload = "UNSIGNED-PAYLOAD"
)

// Config describes the bucket and credentials.
type Config struct {
	Bucket    string
	Region    string // default "us-east-1"
	AccessKey string
	SecretKey string
	// SessionToken is optional (temporary credentials).
	SessionToken string

	// Endpoint is the service URL for non-AWS storage, e.g.
	// "https://s3.us-west-004.backblazeb2.com" or "http://minio:9000".
	// Empty: AWS, https://<bucket>.s3.<region>.amazonaws.com.
	Endpoint string

	// PathStyle addresses the bucket as <endpoint>/<bucket>/<key> instead of
	// <bucket>.<endpoint-host>/<key>. Implied when Endpoint is set.
	PathStyle bool

	PartSize           int64 // default DefaultPartSize, minimum 5 MiB
	MultipartThreshold int64 // default DefaultMultipartThreshold

	HTTPClient *http.Client
}

// Store is an S3 bucket. Safe for concurrent use.
type Store struct {
	cfg  Config
	base *url.URL // scheme://host[/bucket]
	now  func() time.Time
}

// New validates cfg and returns a store. Nothing is contacted until the first
// call.
func New(cfg Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3store: Bucket is required")
	}
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, errors.New("s3store: AccessKey and SecretKey are required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.PartSize < minPartSize {
		cfg.PartSize = DefaultPartSize
	}
	if cfg.MultipartThreshold <= 0 {
		cfg.MultipartThreshold = DefaultMultipartThreshold
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{}
	}

	var raw string
	switch {
	case cfg.Endpoint != "":
		raw = strings.TrimRight(cfg.Endpoint, "/") + "/" + cfg.Bucket
	case cfg.PathStyle:
		raw = "https://s3." + cfg.Region + ".amazonaws.com/" + cfg.Bucket
	default:
		raw = "https://" + cfg.Bucket + ".s3." + cfg.Region + ".amazonaws.com"
	}
	base, err := url.Parse(raw)
	if err != nil || base.Host == "" {
		return nil, fmt.Errorf("s3store: bad endpoint %q", raw)
	}
	return &Store{cfg: cfg, base: base, now: time.Now}, nil
}

// s3Error is a non-2xx response.
type s3Error struct {
	Status  int
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func (e *s3Error) Error() string { return fmt.Sprintf("s3: %d %s: %s", e.Status, e.Code, e.Message) }

func readError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	e := &s3Error{}
	if xml.Unmarshal(body, e) != nil || e.Code == "" {
		e.Code = http.StatusText(resp.StatusCode)
		e.Message = strings.TrimSpace(string(body))
	}
	e.Status = resp.StatusCode
	return e
}

// do sends a signed request. body may be nil; payloadHash is the hex SHA-256
// of the body or UNSIGNED-PAYLOAD.
func (s *Store) do(ctx context.Context, method, key string, query url.Values, body io.Reader, size int64, payloadHash string) (*http.Response, error) {
	u := *s.base
	u.Path = strings.TrimRight(u.Path, "/")
	if key != "" || u.Path == "" {
		u.Path += "/" + key
	}
	u.RawPath = uriEncode(u.Path, false)
	u.RawQuery = canonicalQuery(query)

	if body != nil {
		// Keep net/http from closing the caller's file.
		body = io.NopCloser(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = size
	s.sign(req, u.RawPath, payloadHash)
	return s.cfg.HTTPClient.Do(req)
}

// sign adds SigV4 headers to req.
func (s *Store) sign(req *http.Request, canonicalURI, payloadHash string) {
	now := s.now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if s.cfg.SessionToken != "" {
		req.Header.Set("x-amz-security-token", s.cfg.SessionToken)
	}

	hdrs := map[string]string{"host": req.URL.Host}
	for k, v := range req.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-") || lk == "content-type" {
			hdrs[lk] = strings.TrimSpace(strings.Join(v, ","))
		}
	}
	names := make([]string, 0, len(hdrs))
	for k := range hdrs {
		names = append(names, k)
	}
	sort.Strings(names)
	var canonHdrs strings.Builder
	for _, k := range names {
		canonHdrs.WriteString(k + ":" + hdrs[k] + "\n")
	}
	signed := strings.Join(names, ";")

	canonReq := strings.Join([]string{
		req.Method, canonicalURI, req.URL.RawQuery, canonHdrs.String(), signed, payloadHash,
	}, "\n")
	scope := date + "/" + s.cfg.Region + "/s3/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hexSHA256([]byte(canonReq))

	k := hmacSHA256([]byte("AWS4"+s.cfg.SecretKey), date)
	k = hmacSHA256(k, s.cfg.Region)
	k = hmacSHA256(k, "s3")
	k = hmacSHA256(k, "aws4_request")
	sig := hex.EncodeToString(hmacSHA256(k, toSign))

	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.cfg.AccessKey+"/"+scope+
		", SignedHeaders="+signed+", Signature="+sig)
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func hexSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// uriEncode applies the SigV4 URI encoding. With encodeSlash=false "/" is
// kept (paths).
func uriEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func canonicalQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		for _, v := range q[k] {
			parts = append(parts, uriEncode(k, true)+"="+uriEncode(v, true))
		}
	}
	return strings.Join(parts, "&")
}

// Put uploads body under key. The body is streamed; multipart parts are read
// straight from body when it implements io.ReaderAt (an *os.File does),
// otherwise one part at a time is buffered in memory.
func (s *Store) Put(ctx context.Context, key string, body io.Reader, info axwal.ArchivePutInfo) error {
	if info.Size > s.cfg.MultipartThreshold {
		return s.putMultipart(ctx, key, body, info)
	}
	hash := info.SHA256
	if hash == "" {
		hash = unsignedPayload
	}
	resp, err := s.do(ctx, http.MethodPut, key, nil, body, info.Size, hash)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("s3 put %s: %w", key, readError(resp))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (s *Store) putMultipart(ctx context.Context, key string, body io.Reader, info axwal.ArchivePutInfo) (err error) {
	resp, err := s.do(ctx, http.MethodPost, key, url.Values{"uploads": {""}}, nil, 0, hexSHA256(nil))
	if err != nil {
		return err
	}
	var init struct {
		UploadID string `xml:"UploadId"`
	}
	err = decodeXML(resp, &init)
	if err == nil && init.UploadID == "" {
		err = errors.New("no UploadId in response")
	}
	if err != nil {
		return fmt.Errorf("s3 create multipart %s: %w", key, err)
	}
	uq := func(extra url.Values) url.Values {
		q := url.Values{"uploadId": {init.UploadID}}
		for k, v := range extra {
			q[k] = v
		}
		return q
	}
	defer func() {
		if err != nil {
			// Abandoned parts are billed until aborted. The caller's ctx may
			// already be dead, so use a fresh one.
			actx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if r, aerr := s.do(actx, http.MethodDelete, key, uq(nil), nil, 0, hexSHA256(nil)); aerr == nil {
				r.Body.Close()
			}
		}
	}()

	type part struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	}
	var parts []part
	ra, _ := body.(io.ReaderAt)
	var buf []byte
	for off, n := int64(0), 1; off < info.Size; n++ {
		size := min(s.cfg.PartSize, info.Size-off)
		var pr io.Reader
		h := sha256.New()
		if ra != nil {
			if _, err := io.Copy(h, io.NewSectionReader(ra, off, size)); err != nil {
				return fmt.Errorf("hash part %d: %w", n, err)
			}
			pr = io.NewSectionReader(ra, off, size)
		} else {
			if buf == nil {
				buf = make([]byte, s.cfg.PartSize)
			}
			if _, err := io.ReadFull(body, buf[:size]); err != nil {
				return fmt.Errorf("read part %d: %w", n, err)
			}
			h.Write(buf[:size])
			pr = bytes.NewReader(buf[:size])
		}
		resp, err := s.do(ctx, http.MethodPut, key, uq(url.Values{"partNumber": {strconv.Itoa(n)}}),
			pr, size, hex.EncodeToString(h.Sum(nil)))
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			err = readError(resp)
			resp.Body.Close()
			return fmt.Errorf("s3 upload part %s #%d: %w", key, n, err)
		}
		etag := resp.Header.Get("ETag")
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if etag == "" {
			return fmt.Errorf("s3 upload part %s #%d: no ETag in response", key, n)
		}
		parts = append(parts, part{n, etag})
		off += size
	}

	doc, err := xml.Marshal(struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   []part   `xml:"Part"`
	}{Parts: parts})
	if err != nil {
		return err
	}
	resp, err = s.do(ctx, http.MethodPost, key, uq(nil), bytes.NewReader(doc), int64(len(doc)), hexSHA256(doc))
	if err != nil {
		return err
	}
	// CompleteMultipartUpload can answer 200 with an <Error> body.
	var done struct {
		XMLName xml.Name
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if err := decodeXML(resp, &done); err != nil {
		return fmt.Errorf("s3 complete multipart %s: %w", key, err)
	}
	if done.XMLName.Local == "Error" {
		return fmt.Errorf("s3 complete multipart %s: %s: %s", key, done.Code, done.Message)
	}
	return nil
}

func decodeXML(resp *http.Response, out any) error {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return readError(resp)
	}
	return xml.NewDecoder(resp.Body).Decode(out)
}

// Get downloads an object.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := s.do(ctx, http.MethodGet, key, nil, nil, 0, hexSHA256(nil))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		return resp.Body, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: %s", axwal.ErrObjectNotFound, key)
	}
	return nil, fmt.Errorf("s3 get %s: %w", key, readError(resp))
}

// List returns all objects whose key starts with prefix (ListObjectsV2).
// S3 doesn't report content SHA-1, so ArchiveObject.SHA1 is empty.
func (s *Store) List(ctx context.Context, prefix string) ([]axwal.ArchiveObject, error) {
	var out []axwal.ArchiveObject
	token := ""
	for {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if token != "" {
			q.Set("continuation-token", token)
		}
		resp, err := s.do(ctx, http.MethodGet, "", q, nil, 0, hexSHA256(nil))
		if err != nil {
			return nil, err
		}
		var page struct {
			Contents []struct {
				Key          string    `xml:"Key"`
				Size         int64     `xml:"Size"`
				LastModified time.Time `xml:"LastModified"`
			} `xml:"Contents"`
			IsTruncated           bool   `xml:"IsTruncated"`
			NextContinuationToken string `xml:"NextContinuationToken"`
		}
		if err := decodeXML(resp, &page); err != nil {
			return nil, fmt.Errorf("s3 list %q: %w", prefix, err)
		}
		for _, c := range page.Contents {
			out = append(out, axwal.ArchiveObject{Key: c.Key, Size: c.Size, ModTime: c.LastModified})
		}
		if !page.IsTruncated || page.NextContinuationToken == "" {
			return out, nil
		}
		token = page.NextContinuationToken
	}
}
