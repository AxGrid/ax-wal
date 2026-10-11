package axwal

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// EncryptionConfig configures opt-in encryption-at-rest. When set on both the
// writer and any reader, every record's payload is sealed with AES-256-GCM
// before framing.
//
// On-disk body layout: [12B nonce | gcm_seal(plaintext)] (where the seal
// already includes a 16B authentication tag). The frame's CRC32C still covers
// the full body, so framing-layer corruption is detected before the AEAD even
// runs.
//
// Key management is the caller's problem — we don't generate or derive keys.
// Rotation is supported: see Writer.RotateKey and OldKeys. A reader without the key can still scan the framing layer
// (CRC and length checks succeed) but cannot recover plaintext: Open()
// validates plaintexts only at Read time.
type EncryptionConfig struct {
	// Key is the 32-byte AES-256 key. Other lengths are rejected at OpenWriter
	// / OpenReader time.
	Key []byte

	// OldKeys are additional 32-byte keys accepted for DECRYPTION only. Use
	// them on readers during and after a key rotation (Writer.RotateKey):
	// segments sealed with a previous key stay readable while new segments
	// use Key. The right key is picked per record by AEAD authentication, so
	// the order doesn't matter. Ignored by writers.
	OldKeys [][]byte
}

// KeyFingerprint returns a short, non-reversible identifier of an encryption
// key (16 hex chars). It is recorded in segment .meta sidecars and in the
// archive manifest so that the right key for an old segment can be located
// without trial decryption. Returns "" for an empty key.
func KeyFingerprint(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	h := sha256.New()
	h.Write([]byte("axwal-key-fingerprint-v1"))
	h.Write(key)
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// ErrEncryption indicates a payload-level decryption / format failure (bad
// key, tampered ciphertext, truncated body). Wraps the underlying error.
var ErrEncryption = errors.New("axwal: encryption error")

// cipherAEAD seals with the primary key and opens with the primary or any of
// the old keys. Not safe for concurrent open() calls (it remembers which key
// matched last); seal() is safe once constructed.
type cipherAEAD struct {
	aead cipher.AEAD   // primary: used by seal
	all  []cipher.AEAD // primary + old keys, tried by open
	cur  int           // index in all of the key that opened the last record
	fp   string        // fingerprint of the primary key
}

// newCipherFromConfig builds a cipher from an EncryptionConfig, including its
// OldKeys. Returns (nil, nil) for a nil config.
func newCipherFromConfig(cfg *EncryptionConfig) (*cipherAEAD, error) {
	if cfg == nil {
		return nil, nil
	}
	c, err := newCipher(cfg.Key)
	if err != nil {
		return nil, err
	}
	for _, k := range cfg.OldKeys {
		old, err := newCipher(k)
		if err != nil {
			return nil, err
		}
		c.all = append(c.all, old.aead)
	}
	return c, nil
}

func newCipher(key []byte) (*cipherAEAD, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("%w: key must be 32 bytes (AES-256), got %d",
			ErrEncryption, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: aes init: %v", ErrEncryption, err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: gcm init: %v", ErrEncryption, err)
	}
	return &cipherAEAD{aead: aead, all: []cipher.AEAD{aead}, fp: KeyFingerprint(key)}, nil
}

// seal returns nonce || sealed-payload-with-tag.
func (c *cipherAEAD) seal(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("%w: nonce: %v", ErrEncryption, err)
	}
	out := make([]byte, len(nonce), len(nonce)+len(plaintext)+c.aead.Overhead())
	copy(out, nonce)
	return c.aead.Seal(out, nonce, plaintext, nil), nil
}

// open consumes nonce || sealed-payload and returns plaintext.
func (c *cipherAEAD) open(body []byte) ([]byte, error) {
	ns := c.aead.NonceSize()
	if len(body) < ns+c.aead.Overhead() {
		return nil, fmt.Errorf("%w: ciphertext shorter than nonce+tag", ErrEncryption)
	}
	nonce, ct := body[:ns], body[ns:]
	pt, err := c.all[c.cur].Open(nil, nonce, ct, nil)
	if err == nil {
		return pt, nil
	}
	// Key rotation: the record may have been sealed with another key of the
	// set. GCM authentication makes trial decryption safe.
	for i, a := range c.all {
		if i == c.cur {
			continue
		}
		if pt, altErr := a.Open(nil, nonce, ct, nil); altErr == nil {
			c.cur = i
			return pt, nil
		}
	}
	return nil, fmt.Errorf("%w: %v", ErrEncryption, err)
}
