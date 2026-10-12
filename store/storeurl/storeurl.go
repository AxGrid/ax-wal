// Package storeurl opens an axwal.ArchiveStore from a URL, for command-line
// tools and config files:
//
//	b2://bucket/prefix      Backblaze B2 native API
//	                        credentials: B2_KEY_ID, B2_APP_KEY
//	s3://bucket/prefix      S3 / S3-compatible; ?region=…&endpoint=…
//	                        credentials: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
//	                        (+ AWS_SESSION_TOKEN); AWS_REGION and
//	                        AWS_ENDPOINT_URL are used when the query has none
//	https://host/path       read-only, plain GET, no credentials: a public
//	                        bucket or any static file server. No listing —
//	                        for archives written with ArchiveEncryption.
//	                        ?prefix=wal/kiosk if the URL is the bucket root.
//	file:///abs/dir         a local directory (also a bare path);
//	                        ?prefix=wal/kiosk for a key prefix inside it
package storeurl

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	axwal "github.com/axgrid/ax-wal"
	"github.com/axgrid/ax-wal/store/b2store"
	"github.com/axgrid/ax-wal/store/fsstore"
	"github.com/axgrid/ax-wal/store/httpstore"
	"github.com/axgrid/ax-wal/store/s3store"
)

// Open returns the store and the key prefix encoded in raw.
func Open(raw string) (store axwal.ArchiveStore, prefix string, err error) {
	if raw == "" {
		return nil, "", errors.New("storeurl: empty store URL")
	}
	if !strings.Contains(raw, "://") {
		return fsstore.New(raw), "", nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, "", fmt.Errorf("storeurl: %w", err)
	}
	prefix = strings.Trim(u.Path, "/")
	switch u.Scheme {
	case "http", "https":
		// The URL is the base. An archive in the clear records full object
		// keys in its manifest, so there the base must be the bucket root
		// with the writer's prefix passed as ?prefix=…; an encrypted archive
		// can simply be pointed at directly.
		prefix = strings.Trim(u.Query().Get("prefix"), "/")
		u.RawQuery, u.Fragment = "", ""
		return httpstore.New(u.String(), nil), prefix, nil

	case "file":
		if u.Host != "" && u.Host != "localhost" {
			return nil, "", fmt.Errorf("storeurl: file URL must be file:///abs/path, got %q", raw)
		}
		return fsstore.New(u.Path), strings.Trim(u.Query().Get("prefix"), "/"), nil

	case "b2":
		keyID, appKey := firstEnv("B2_KEY_ID", "B2_APPLICATION_KEY_ID"), firstEnv("B2_APP_KEY", "B2_APPLICATION_KEY")
		if u.Host == "" || keyID == "" || appKey == "" {
			return nil, "", errors.New("storeurl: b2://bucket/prefix needs B2_KEY_ID and B2_APP_KEY in the environment")
		}
		return b2store.New(keyID, appKey, u.Host), prefix, nil

	case "s3":
		q := u.Query()
		st, err := s3store.New(s3store.Config{
			Bucket:       u.Host,
			Region:       firstNonEmpty(q.Get("region"), os.Getenv("AWS_REGION"), os.Getenv("AWS_DEFAULT_REGION")),
			Endpoint:     firstNonEmpty(q.Get("endpoint"), os.Getenv("AWS_ENDPOINT_URL")),
			AccessKey:    os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretKey:    os.Getenv("AWS_SECRET_ACCESS_KEY"),
			SessionToken: os.Getenv("AWS_SESSION_TOKEN"),
			PathStyle:    q.Get("path-style") == "true",
		})
		if err != nil {
			return nil, "", fmt.Errorf("storeurl: %w (credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)", err)
		}
		return st, prefix, nil
	}
	return nil, "", fmt.Errorf("storeurl: unsupported scheme %q (want b2, s3, https or file)", u.Scheme)
}

func firstEnv(names ...string) string {
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	return ""
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
