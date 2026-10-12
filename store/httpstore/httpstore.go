// Package httpstore is a read-only axwal.ArchiveStore over plain HTTP(S)
// GET — for archives in a public bucket (or behind any static file server /
// CDN), where no credentials are involved at all.
//
// It can only fetch objects by name: there is no listing. That is exactly
// what reading an archive written with axwal.ArchiveEncryption needs, since
// the reader derives object names from the key. Typical base URLs:
//
//	https://f004.backblazeb2.com/file/<bucket>/<prefix>      Backblaze B2, public bucket
//	https://<bucket>.s3.<region>.amazonaws.com/<prefix>      S3, public-read objects
package httpstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	axwal "github.com/axgrid/ax-wal"
)

// ErrReadOnly is returned by Put and List.
var ErrReadOnly = errors.New("httpstore: read-only store (GET only, no listing)")

// Store fetches <base>/<key>.
type Store struct {
	base string
	hc   *http.Client
}

// New returns a store rooted at baseURL. A nil client means http.DefaultClient.
func New(baseURL string, client *http.Client) *Store {
	if client == nil {
		client = http.DefaultClient
	}
	return &Store{base: strings.TrimRight(baseURL, "/"), hc: client}
}

// Get streams the object. 404 maps to axwal.ErrObjectNotFound.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	u := s.base + "/" + strings.Join(parts, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return resp.Body, nil
	case http.StatusNotFound:
		resp.Body.Close()
		return nil, fmt.Errorf("%w: %s", axwal.ErrObjectNotFound, key)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	resp.Body.Close()
	return nil, fmt.Errorf("httpstore: GET %s: %s: %s", u, resp.Status, strings.TrimSpace(string(body)))
}

// Put is not supported.
func (s *Store) Put(context.Context, string, io.Reader, axwal.ArchivePutInfo) error {
	return ErrReadOnly
}

// List is not supported.
func (s *Store) List(context.Context, string) ([]axwal.ArchiveObject, error) {
	return nil, ErrReadOnly
}
