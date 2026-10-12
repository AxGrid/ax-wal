package axwal

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"
)

// ErrObjectNotFound must be returned (possibly wrapped) by ArchiveStore.Get
// when the key doesn't exist.
var ErrObjectNotFound = errors.New("axwal: archive object not found")

// ArchiveStore is the object storage the archiver uploads to and the archive
// reader downloads from. Implementations for Backblaze B2, S3-compatible
// storage and a local directory live in the store/ subpackages; implement
// these three methods to plug in anything else.
//
// Keys are slash-separated paths without a leading slash.
type ArchiveStore interface {
	// Put stores body under key, replacing any existing object. It must be
	// atomic (no partially written object visible under key) and should
	// stream: info.Size is the exact body length, and body additionally
	// implements io.ReaderAt when the archiver calls it, so multipart
	// uploads can read parts without buffering them. info.SHA1 / info.SHA256
	// are hex digests of the whole body; pass them to the backend when it
	// can verify content server-side.
	Put(ctx context.Context, key string, body io.Reader, info ArchivePutInfo) error

	// Get opens the object for streaming. Missing key: ErrObjectNotFound.
	Get(ctx context.Context, key string) (io.ReadCloser, error)

	// List returns every object whose key starts with prefix.
	List(ctx context.Context, prefix string) ([]ArchiveObject, error)
}

// ArchivePutInfo describes the body handed to ArchiveStore.Put.
type ArchivePutInfo struct {
	Size   int64
	SHA1   string // hex, always set by the archiver
	SHA256 string // hex, always set by the archiver
}

// ArchiveObject is one entry returned by ArchiveStore.List.
type ArchiveObject struct {
	Key     string
	Size    int64
	ModTime time.Time
	// SHA1 is the hex SHA-1 of the content if the backend reports one
	// (B2 does), else "".
	SHA1 string
}

// ArchiveLayout selects how segment objects are named under the prefix.
type ArchiveLayout int

const (
	// LayoutFlat: <prefix>/wal-F-L.zst
	LayoutFlat ArchiveLayout = iota
	// LayoutByDay: <prefix>/2026/10/11/wal-F-L.zst — the UTC day of the
	// segment's first record. Combine with RotateAligned so that a segment
	// never spans two days.
	LayoutByDay
	// LayoutByHour: <prefix>/2026/10/11/07/wal-F-L.zst (UTC).
	LayoutByHour
)

// Backoff is an exponential retry schedule: Min after the first failure,
// doubling up to Max.
type Backoff struct {
	Min time.Duration
	Max time.Duration
}

func (b Backoff) delay(attempt int) time.Duration {
	d := b.Min
	for i := 1; i < attempt && d < b.Max; i++ {
		d *= 2
	}
	if d > b.Max {
		d = b.Max
	}
	return d
}

// ArchiveConfig enables the writer's built-in archiver.
//
// Every closed segment is uploaded once it has left the rotation pipeline
// (i.e. after compression, not at OnRotate time). The .archived sidecar is
// written only after the upload has been verified against the store, so with
// retention enabled a segment is never deleted locally before it is safely
// in the archive. Failed uploads are retried forever with Retry backoff;
// segments left over from a previous run — including one finalized by crash
// recovery — are picked up when the writer opens.
type ArchiveConfig struct {
	// Store is the destination. Required.
	Store ArchiveStore

	// Prefix is prepended to every object key ("wal/kiosk-prod").
	Prefix string

	// Layout selects the key scheme under Prefix. Default LayoutFlat.
	Layout ArchiveLayout

	// Concurrency is the number of parallel uploads. Default 2.
	Concurrency int

	// Retry is the backoff between attempts for a failing segment.
	// Default {Min: 5s, Max: 10m}.
	Retry Backoff

	// AttemptTimeout bounds a single upload attempt so a hung connection
	// can't occupy a worker forever. Default 30m.
	AttemptTimeout time.Duration

	// ScanInterval is how often the directory is rescanned for pending
	// segments as a safety net (uploads normally start right after rotation).
	// Default 1m.
	ScanInterval time.Duration

	// DrainOnClose makes Writer.Close wait up to this long for pending
	// uploads — in particular the segment Close itself just finished.
	// Zero: Close doesn't wait; the rest is uploaded on the next open.
	DrainOnClose time.Duration

	// DisableManifest turns off <prefix>/manifest.json maintenance.
	DisableManifest bool

	// Encryption, if non-nil, encrypts every uploaded object and replaces
	// object names with keyed hashes, so the bucket may be public. See
	// ArchiveEncryption. Layout is ignored in this mode.
	Encryption *ArchiveEncryption

	crypto *archiveCrypto // derived from Encryption by withDefaults
}

func (c ArchiveConfig) withDefaults() (ArchiveConfig, error) {
	if c.Store == nil {
		return c, errors.New("axwal: ArchiveConfig.Store is required")
	}
	crypto, err := newArchiveCrypto(c.Encryption)
	if err != nil {
		return c, err
	}
	c.crypto = crypto
	c.Prefix = strings.Trim(c.Prefix, "/")
	if c.Concurrency <= 0 {
		c.Concurrency = 2
	}
	if c.Retry.Min <= 0 {
		c.Retry.Min = 5 * time.Second
	}
	if c.Retry.Max < c.Retry.Min {
		c.Retry.Max = 10 * time.Minute
		if c.Retry.Max < c.Retry.Min {
			c.Retry.Max = c.Retry.Min
		}
	}
	if c.AttemptTimeout <= 0 {
		c.AttemptTimeout = 30 * time.Minute
	}
	if c.ScanInterval <= 0 {
		c.ScanInterval = time.Minute
	}
	return c, nil
}

// archiveManifestName is the object holding the manifest, under the prefix.
const archiveManifestName = "manifest.json"

// ArchiveSegment is one uploaded segment as recorded in the manifest.
type ArchiveSegment struct {
	// Key is the full object key (including the prefix).
	Key        string `json:"key"`
	FirstIndex uint64 `json:"first_index"`
	LastIndex  uint64 `json:"last_index"`
	NumRecords uint64 `json:"num_records"`

	// SizeBytes / SHA1 / SHA256 describe the object as stored (i.e. the
	// compressed file when Compression is "zst").
	SizeBytes   int64  `json:"size_bytes"`
	SHA1        string `json:"sha1,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	Compression string `json:"compression"`

	// FirstRecordUnix / LastRecordUnix: append time of the first / last
	// record. For segments whose times weren't recorded (finalized by crash
	// recovery, or listed from a store without a manifest) both fall back to
	// the file's modification time.
	FirstRecordUnix int64 `json:"first_record_unix"`
	LastRecordUnix  int64 `json:"last_record_unix"`

	// KeyFingerprint identifies the encryption key (see KeyFingerprint);
	// empty when unencrypted or unknown.
	KeyFingerprint string `json:"key_fingerprint,omitempty"`

	// Encryption names the archive-level encryption of the object
	// ("aes-256-gcm-stream-v1"), empty if it is stored in the clear. For an
	// encrypted object SizeBytes / SHA1 / SHA256 describe the ciphertext.
	Encryption string `json:"encryption,omitempty"`

	UploadedAtUnix int64 `json:"uploaded_at_unix,omitempty"`
}

// ArchiveManifest is the catalog of an archive prefix, stored as
// <prefix>/manifest.json and rewritten by the archiver after every upload.
type ArchiveManifest struct {
	Version       int   `json:"version"`
	UpdatedAtUnix int64 `json:"updated_at_unix"`

	// FirstIndex is the index the archive is supposed to start at: the
	// first record of the oldest segment the writer still had on disk when
	// it created the manifest. Uploads run in parallel and can land out of
	// order, so "the lowest segment listed right now" is not a safe answer
	// to "where does the log begin" — with FirstIndex a reader can tell
	// that the beginning simply hasn't arrived yet (or is missing). Zero in
	// manifests written before this field existed.
	FirstIndex uint64 `json:"first_index,omitempty"`

	Segments      []ArchiveSegment `json:"segments"` // sorted by FirstIndex
}

func archiveKey(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "/" + name
}

// segmentObjectKey builds the object key for a segment file name.
func segmentObjectKey(prefix string, layout ArchiveLayout, fileName string, t time.Time) string {
	t = t.UTC()
	switch layout {
	case LayoutByDay:
		fileName = t.Format("2006/01/02/") + fileName
	case LayoutByHour:
		fileName = t.Format("2006/01/02/15/") + fileName
	}
	return archiveKey(prefix, fileName)
}

// upsert adds or replaces the entry for seg's index range, keeping order.
func (m *ArchiveManifest) upsert(seg ArchiveSegment) {
	for i := range m.Segments {
		if m.Segments[i].FirstIndex == seg.FirstIndex && m.Segments[i].LastIndex == seg.LastIndex {
			m.Segments[i] = seg
			return
		}
	}
	m.Segments = append(m.Segments, seg)
	sort.Slice(m.Segments, func(i, j int) bool { return m.Segments[i].FirstIndex < m.Segments[j].FirstIndex })
}

// LoadArchiveManifest returns the catalog of the archive under prefix.
//
// It reads <prefix>/manifest.json. If the archive has none (manifest disabled,
// or segments uploaded by something else), the catalog is rebuilt by listing
// the prefix and parsing segment names; record times then fall back to object
// modification times and checksums / key fingerprints are empty.
//
// For an archive written with ArchiveConfig.Encryption use
// LoadEncryptedArchiveManifest.
func LoadArchiveManifest(ctx context.Context, store ArchiveStore, prefix string) (*ArchiveManifest, error) {
	return loadManifest(ctx, store, prefix, nil)
}

// LoadEncryptedArchiveManifest returns the catalog of an archive written
// with ArchiveConfig.Encryption. Only Get is used, so it works on a public
// bucket that can't be listed. Returns ErrObjectNotFound if the archive has
// no manifest (an encrypted archive can't be reconstructed from a listing:
// its object names don't say what they hold).
func LoadEncryptedArchiveManifest(ctx context.Context, store ArchiveStore, prefix string, enc ArchiveEncryption) (*ArchiveManifest, error) {
	crypto, err := newArchiveCrypto(&enc)
	if err != nil {
		return nil, err
	}
	return loadManifest(ctx, store, prefix, crypto)
}

func loadManifest(ctx context.Context, store ArchiveStore, prefix string, crypto *archiveCrypto) (*ArchiveManifest, error) {
	prefix = strings.Trim(prefix, "/")
	m, err := getManifest(ctx, store, prefix, crypto)
	if err == nil {
		return m, nil
	}
	if !errors.Is(err, ErrObjectNotFound) || crypto != nil {
		return nil, err
	}

	listPrefix := prefix
	if listPrefix != "" {
		listPrefix += "/"
	}
	objs, err := store.List(ctx, listPrefix)
	if err != nil {
		return nil, fmt.Errorf("no %s under %q and the store can't be listed "+
			"(if this archive is encrypted, the archive key is required): %w", archiveManifestName, prefix, err)
	}
	m = &ArchiveManifest{Version: 1}
	for _, o := range objs {
		s, perr := parseSegName(path.Base(o.Key))
		if perr != nil || !s.Closed {
			continue
		}
		m.upsert(ArchiveSegment{
			Key:             o.Key,
			FirstIndex:      s.FirstIndex,
			LastIndex:       s.LastIndex,
			NumRecords:      s.LastIndex - s.FirstIndex + 1,
			SizeBytes:       o.Size,
			SHA1:            o.SHA1,
			Compression:     s.Compression,
			FirstRecordUnix: o.ModTime.Unix(),
			LastRecordUnix:  o.ModTime.Unix(),
		})
	}
	return m, nil
}

// getManifest fetches and parses the manifest object; ErrObjectNotFound if
// there is none.
func getManifest(ctx context.Context, store ArchiveStore, prefix string, crypto *archiveCrypto) (*ArchiveManifest, error) {
	name := archiveManifestName
	if crypto != nil {
		name = crypto.manifestName()
	}
	rc, err := store.Get(ctx, archiveKey(prefix, name))
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	var src io.Reader = rc
	if crypto != nil {
		src = crypto.decryptReader(rc)
	}
	body, err := io.ReadAll(src)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var m ArchiveManifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	return &m, nil
}

func putManifest(ctx context.Context, store ArchiveStore, prefix string, m *ArchiveManifest, crypto *archiveCrypto) error {
	m.Version = 1
	m.UpdatedAtUnix = time.Now().Unix()
	body, err := json.MarshalIndent(m, "", " ")
	if err != nil {
		return err
	}
	name := archiveManifestName
	if crypto != nil {
		plainSum := sha256.Sum256(body)
		var enc bytes.Buffer
		if err := crypto.encrypt(&enc, bytes.NewReader(body), crypto.saltFor("manifest", plainSum[:])); err != nil {
			return err
		}
		body, name = enc.Bytes(), crypto.manifestName()
	}
	s1 := sha1.Sum(body)
	s256 := sha256.Sum256(body)
	return store.Put(ctx, archiveKey(prefix, name), bytes.NewReader(body), ArchivePutInfo{
		Size:   int64(len(body)),
		SHA1:   hex.EncodeToString(s1[:]),
		SHA256: hex.EncodeToString(s256[:]),
	})
}

// statObject looks up a single key via List. ok is false if it doesn't exist.
func statObject(ctx context.Context, store ArchiveStore, key string) (obj ArchiveObject, ok bool, err error) {
	objs, err := store.List(ctx, key)
	if err != nil {
		return ArchiveObject{}, false, err
	}
	for _, o := range objs {
		if o.Key == key {
			return o, true, nil
		}
	}
	return ArchiveObject{}, false, nil
}

// archiveHead is the minimal catalog of an encrypted archive that has no
// manifest: where the log starts and how far it has been uploaded. It lets a
// reader walking the chain of derived object names start at the right index
// and tell "one object is missing" from "the log ends here".
type archiveHead struct {
	FirstIndex    uint64 `json:"first_index"`
	LastIndex     uint64 `json:"last_index"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

func getArchiveHead(ctx context.Context, store ArchiveStore, prefix string, crypto *archiveCrypto) (archiveHead, error) {
	var h archiveHead
	rc, err := store.Get(ctx, archiveKey(prefix, crypto.headName()))
	if err != nil {
		return h, err
	}
	defer rc.Close()
	body, err := io.ReadAll(crypto.decryptReader(rc))
	if err != nil {
		return h, err
	}
	return h, json.Unmarshal(body, &h)
}

func putArchiveHead(ctx context.Context, store ArchiveStore, prefix string, crypto *archiveCrypto, h archiveHead) error {
	h.UpdatedAtUnix = time.Now().Unix()
	plain, err := json.Marshal(h)
	if err != nil {
		return err
	}
	plainSum := sha256.Sum256(plain)
	var enc bytes.Buffer
	if err := crypto.encrypt(&enc, bytes.NewReader(plain), crypto.saltFor("head", plainSum[:])); err != nil {
		return err
	}
	s1 := sha1.Sum(enc.Bytes())
	s256 := sha256.Sum256(enc.Bytes())
	return store.Put(ctx, archiveKey(prefix, crypto.headName()), bytes.NewReader(enc.Bytes()), ArchivePutInfo{
		Size:   int64(enc.Len()),
		SHA1:   hex.EncodeToString(s1[:]),
		SHA256: hex.EncodeToString(s256[:]),
	})
}
