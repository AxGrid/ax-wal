// Package fsstore is an axwal.ArchiveStore backed by a local directory (or
// any mounted filesystem: NFS, a second disk, a FUSE bucket mount). It is
// also the reference implementation of the interface.
package fsstore

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	axwal "github.com/axgrid/ax-wal"
)

// Store keeps objects as files under Root; a key maps to Root/<key>.
type Store struct {
	root string
}

// New returns a store rooted at dir. The directory is created on first Put.
func New(dir string) *Store {
	return &Store{root: dir}
}

func (s *Store) path(key string) (string, error) {
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains("/"+key+"/", "/../") {
		return "", fmt.Errorf("fsstore: invalid key %q", key)
	}
	return filepath.Join(s.root, filepath.FromSlash(key)), nil
}

// Put writes body to a temp file, checks size and SHA-1 against info and
// renames it into place.
func (s *Store) Put(ctx context.Context, key string, body io.Reader, info axwal.ArchivePutInfo) error {
	dst, err := s.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".put-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename

	h := sha1.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), &ctxReader{ctx, body})
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if n != info.Size {
		return fmt.Errorf("fsstore: %s: wrote %d bytes, want %d", key, n, info.Size)
	}
	if got := hex.EncodeToString(h.Sum(nil)); info.SHA1 != "" && !strings.EqualFold(got, info.SHA1) {
		return fmt.Errorf("fsstore: %s: sha1 %s, want %s", key, got, info.SHA1)
	}
	return os.Rename(tmp.Name(), dst)
}

// Get opens the object file.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	p, err := s.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", axwal.ErrObjectNotFound, key)
	}
	return f, err
}

// List walks the root and returns objects whose key starts with prefix.
// SHA1 is not reported (it would mean reading every file).
func (s *Store) List(ctx context.Context, prefix string) ([]axwal.ArchiveObject, error) {
	var out []axwal.ArchiveObject
	// Only descend into the directory part of the prefix.
	start := s.root
	if i := strings.LastIndexByte(prefix, '/'); i >= 0 {
		start = filepath.Join(s.root, filepath.FromSlash(prefix[:i]))
	}
	err := filepath.WalkDir(start, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), ".put-") {
			return nil
		}
		rel, err := filepath.Rel(s.root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		out = append(out, axwal.ArchiveObject{Key: key, Size: fi.Size(), ModTime: fi.ModTime()})
		return nil
	})
	return out, err
}

// ctxReader aborts a copy when ctx is canceled.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}
