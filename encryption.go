package axwal

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
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
// Key management is the caller's problem — we don't generate, rotate, or
// derive keys. A reader without the key can still scan the framing layer
// (CRC and length checks succeed) but cannot recover plaintext: Open()
// validates plaintexts only at Read time.
type EncryptionConfig struct {
	// Key is the 32-byte AES-256 key. Other lengths are rejected at OpenWriter
	// / OpenReader time.
	Key []byte
}

// ErrEncryption indicates a payload-level decryption / format failure (bad
// key, tampered ciphertext, truncated body). Wraps the underlying error.
var ErrEncryption = errors.New("axwal: encryption error")

type cipherAEAD struct {
	aead cipher.AEAD
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
	return &cipherAEAD{aead: aead}, nil
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
	pt, err := c.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEncryption, err)
	}
	return pt, nil
}
