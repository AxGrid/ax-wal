package axwal

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// ArchiveEncryption encrypts everything the archiver uploads and hides what
// the objects are, so the bucket can be public: without the key an observer
// sees opaque blobs under random-looking names — no record contents, no
// index ranges, no record counts, no ordering.
//
//   - Object bodies (segments and the manifest) are sealed with
//     AES-256-GCM in 64 KiB chunks. The format is streamable: a reader
//     decrypts while downloading, and any modification, truncation or
//     reordering of chunks is detected.
//   - Object names are HMAC-SHA256(key, first index of the segment), so a
//     reader holding the key can compute the name of the segment that
//     starts at any given index — and walk the whole log from index 1 by
//     following "last index + 1" — without ever listing the bucket.
//
// This is independent of WriterOptions.Encryption, which seals individual
// records on local disk; either or both may be used. With ArchiveEncryption
// the Layout setting is ignored (a date in the path would give the game
// away) and all objects sit directly under Prefix.
//
// What still leaks: the number of objects, their sizes and upload times.
type ArchiveEncryption struct {
	// Key is a 32-byte secret. Losing it makes the archive unreadable.
	Key []byte
}

const (
	archiveEncChunk    = 64 << 10
	archiveEncSaltSize = 16
	// archiveEncAlgo is recorded in the manifest for encrypted segments.
	archiveEncAlgo = "aes-256-gcm-stream-v1"
)

// archiveEncMagic starts every encrypted object.
var archiveEncMagic = []byte("AXWAE\x00\x00\x01")

// archiveCrypto holds the keys derived from an ArchiveEncryption key.
type archiveCrypto struct {
	encKey  []byte // derives per-object keys
	nameKey []byte // derives object names
}

func newArchiveCrypto(cfg *ArchiveEncryption) (*archiveCrypto, error) {
	if cfg == nil {
		return nil, nil
	}
	if len(cfg.Key) != 32 {
		return nil, fmt.Errorf("%w: archive key must be 32 bytes, got %d", ErrEncryption, len(cfg.Key))
	}
	return &archiveCrypto{
		encKey:  hmacSum(cfg.Key, []byte("axwal-archive-enc-v1")),
		nameKey: hmacSum(cfg.Key, []byte("axwal-archive-name-v1")),
	}, nil
}

func hmacSum(key []byte, parts ...[]byte) []byte {
	h := hmac.New(sha256.New, key)
	for _, p := range parts {
		h.Write(p)
	}
	return h.Sum(nil)
}

// segmentName is the object name of the segment whose first record is
// firstIndex: 32 hex characters.
func (c *archiveCrypto) segmentName(firstIndex uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], firstIndex)
	return hex.EncodeToString(hmacSum(c.nameKey, []byte("segment"), b[:])[:16])
}

// manifestName is the object name of the encrypted manifest.
func (c *archiveCrypto) manifestName() string {
	return hex.EncodeToString(hmacSum(c.nameKey, []byte("manifest"))[:16])
}

// saltFor derives the per-object salt from the object's identity and the
// SHA-256 of its plaintext. Deterministic on purpose: re-encrypting the same
// file yields the same bytes, so a retried upload can be recognized as
// already done instead of being sent again. Different content always gets a
// different salt, hence a different key.
func (c *archiveCrypto) saltFor(what string, plainSHA256 []byte) []byte {
	return hmacSum(c.encKey, []byte("salt"), []byte(what), plainSHA256)[:archiveEncSaltSize]
}

func (c *archiveCrypto) aead(salt []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(hmacSum(c.encKey, []byte("object"), salt))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEncryption, err)
	}
	return cipher.NewGCM(block)
}

// chunkNonce: 11-byte big-endian chunk counter + 1 byte "this is the last
// chunk". The key is unique per object, so the counter alone is enough; the
// last-chunk flag is what makes truncation at a chunk boundary detectable.
func chunkNonce(counter uint64, last bool) []byte {
	n := make([]byte, 12)
	binary.BigEndian.PutUint64(n[3:11], counter)
	if last {
		n[11] = 1
	}
	return n
}

// encrypt writes the encrypted form of src to dst:
//
//	[8B magic][16B salt] then per chunk: AES-256-GCM(≤64 KiB plaintext) = ciphertext + 16B tag
func (c *archiveCrypto) encrypt(dst io.Writer, src io.Reader, salt []byte) error {
	aead, err := c.aead(salt)
	if err != nil {
		return err
	}
	if _, err := dst.Write(archiveEncMagic); err != nil {
		return err
	}
	if _, err := dst.Write(salt); err != nil {
		return err
	}
	cur := make([]byte, archiveEncChunk)
	next := make([]byte, archiveEncChunk)
	out := make([]byte, 0, archiveEncChunk+aead.Overhead())
	n, err := io.ReadFull(src, cur)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return err
	}
	for counter := uint64(0); ; counter++ {
		// Read ahead one chunk to know whether the current one is the last.
		m, rerr := io.ReadFull(src, next)
		if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
			return rerr
		}
		last := m == 0
		out = aead.Seal(out[:0], chunkNonce(counter, last), cur[:n], nil)
		if _, err := dst.Write(out); err != nil {
			return err
		}
		if last {
			return nil
		}
		cur, next, n = next, cur, m
	}
}

// decryptReader returns a reader that yields the plaintext of an encrypted
// object as it is read from src. Any tampering surfaces as ErrEncryption.
func (c *archiveCrypto) decryptReader(src io.Reader) io.Reader {
	return &archiveDecrypter{c: c, src: bufio.NewReaderSize(src, archiveEncChunk+64)}
}

type archiveDecrypter struct {
	c       *archiveCrypto
	src     *bufio.Reader
	aead    cipher.AEAD
	counter uint64
	plain   []byte // decrypted, not yet returned
	buf     []byte
	done    bool
	err     error
}

func (d *archiveDecrypter) Read(p []byte) (int, error) {
	for len(d.plain) == 0 {
		if d.err != nil {
			return 0, d.err
		}
		if d.done {
			return 0, io.EOF
		}
		if d.err = d.fill(); d.err != nil {
			return 0, d.err
		}
	}
	n := copy(p, d.plain)
	d.plain = d.plain[n:]
	return n, nil
}

func (d *archiveDecrypter) fill() error {
	if d.aead == nil {
		hdr := make([]byte, len(archiveEncMagic)+archiveEncSaltSize)
		if _, err := io.ReadFull(d.src, hdr); err != nil {
			return fmt.Errorf("%w: reading header: %v", ErrEncryption, err)
		}
		if !bytes.Equal(hdr[:len(archiveEncMagic)], archiveEncMagic) {
			return fmt.Errorf("%w: not an encrypted archive object", ErrEncryption)
		}
		aead, err := d.c.aead(hdr[len(archiveEncMagic):])
		if err != nil {
			return err
		}
		d.aead = aead
		d.buf = make([]byte, archiveEncChunk+aead.Overhead())
	}
	n, err := io.ReadFull(d.src, d.buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		if err == io.EOF {
			return fmt.Errorf("%w: object is truncated", ErrEncryption)
		}
		return err
	}
	// The last chunk is the one nothing follows.
	_, peekErr := d.src.Peek(1)
	last := errors.Is(peekErr, io.EOF)
	if peekErr != nil && !last {
		return peekErr
	}
	plain, err := d.aead.Open(d.buf[:0], chunkNonce(d.counter, last), d.buf[:n], nil)
	if err != nil {
		return fmt.Errorf("%w: wrong key, or the object was modified or truncated", ErrEncryption)
	}
	d.counter++
	d.plain = plain
	d.done = last
	return nil
}

// ArchiveObjectName returns the object name (without prefix) under which an
// archive encrypted with key stores the segment starting at firstIndex. For
// tools that fetch objects themselves.
func ArchiveObjectName(key []byte, firstIndex uint64) (string, error) {
	c, err := newArchiveCrypto(&ArchiveEncryption{Key: key})
	if err != nil {
		return "", err
	}
	return c.segmentName(firstIndex), nil
}

// ArchiveManifestName returns the object name (without prefix) of the
// encrypted manifest.
func ArchiveManifestName(key []byte) (string, error) {
	c, err := newArchiveCrypto(&ArchiveEncryption{Key: key})
	if err != nil {
		return "", err
	}
	return c.manifestName(), nil
}

// NewArchiveDecrypter wraps the body of an encrypted archive object and
// returns its plaintext as a stream — for a segment that is the segment file
// as it was on disk (zstd-compressed if the writer compresses).
func NewArchiveDecrypter(key []byte, r io.Reader) (io.Reader, error) {
	c, err := newArchiveCrypto(&ArchiveEncryption{Key: key})
	if err != nil {
		return nil, err
	}
	return c.decryptReader(r), nil
}
