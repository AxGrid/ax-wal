package axwal

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Transport security for the replication protocol over plain TCP, keyed by a
// shared 32-byte secret (ServerOptions.TransportKey /
// RemoteReaderOptions.TransportKey).
//
// Handshake (one round trip plus the client's confirmation):
//
//	C → S   "AXS1" | client ephemeral X25519 public key (32) | client nonce (16)
//	S → C   server ephemeral X25519 public key (32) | server nonce (16) | server MAC (32)
//	C → S   client MAC (32)
//
//	transcript = everything before the MACs
//	prk        = HMAC-SHA256(key = transport key, X25519 shared secret | transcript)
//	server MAC = HMAC-SHA256(prk, "server finished" | transcript)
//	client MAC = HMAC-SHA256(prk, "client finished" | transcript)
//	k(c→s)     = HMAC-SHA256(prk, "client to server")     AES-256-GCM
//	k(s→c)     = HMAC-SHA256(prk, "server to client")     AES-256-GCM
//
// Both sides prove knowledge of the transport key (a peer without it can't
// compute prk, so its MAC fails), and the ephemeral key exchange gives
// forward secrecy: recording the traffic and obtaining the key later doesn't
// decrypt it.
//
// After the handshake everything travels in frames of
// [4B ciphertext length][AES-256-GCM ciphertext+tag], with a per-direction
// counter as the nonce — a modified, replayed, dropped or reordered frame
// fails authentication and kills the connection.

// ErrTransportAuth means the secure handshake failed: the peers don't share
// the same transport key, or one side isn't speaking the secure protocol at
// all (key configured on one end only).
var ErrTransportAuth = errors.New("axwal: transport authentication failed")

var secureMagic = [4]byte{'A', 'X', 'S', '1'}

// errMsgKeyRequired is what a keyed server tells a client that showed up
// without a key (in the plain protocol, the only one that client speaks).
const errMsgKeyRequired = "this server requires a transport key"

const (
	secureNonceSize = 16
	secureMaxFrame  = 64 << 10 // plaintext bytes per frame
	// secureHandshakeTimeout bounds the handshake on the server side, so a
	// peer that connects and says nothing can't hold a goroutine forever.
	secureHandshakeTimeout = 10 * time.Second
)

func checkTransportKey(key []byte) error {
	if len(key) != 32 {
		return fmt.Errorf("%w: transport key must be 32 bytes, got %d", ErrTransportAuth, len(key))
	}
	return nil
}

func secureMAC(key []byte, parts ...[]byte) []byte {
	h := hmac.New(sha256.New, key)
	for _, p := range parts {
		h.Write(p)
	}
	return h.Sum(nil)
}

// secureKeys derives the session from the transport key and the handshake.
func secureKeys(psk, shared, transcript []byte) (prk []byte, c2s, s2c cipher.AEAD, err error) {
	prk = secureMAC(psk, shared, transcript)
	mk := func(label string) (cipher.AEAD, error) {
		block, err := aes.NewCipher(secureMAC(prk, []byte(label)))
		if err != nil {
			return nil, err
		}
		return cipher.NewGCM(block)
	}
	if c2s, err = mk("client to server"); err != nil {
		return nil, nil, nil, err
	}
	if s2c, err = mk("server to client"); err != nil {
		return nil, nil, nil, err
	}
	return prk, c2s, s2c, nil
}

// secureClient runs the client side of the handshake on conn.
func secureClient(conn net.Conn, psk []byte) (net.Conn, error) {
	if err := checkTransportKey(psk); err != nil {
		return nil, err
	}
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	hello := make([]byte, 0, 4+32+secureNonceSize)
	hello = append(hello, secureMagic[:]...)
	hello = append(hello, priv.PublicKey().Bytes()...)
	hello = append(hello, make([]byte, secureNonceSize)...)
	if _, err := rand.Read(hello[4+32:]); err != nil {
		return nil, err
	}
	if _, err := conn.Write(hello); err != nil {
		return nil, fmt.Errorf("secure handshake: %w", err)
	}

	reply := make([]byte, 32+secureNonceSize+sha256.Size)
	if n, err := io.ReadFull(conn, reply); err != nil {
		// A server without a key answers our "AXS1" with a short plain-text
		// ERR frame and hangs up: a configuration mismatch, not worth
		// retrying. Anything else is an ordinary connection failure.
		if n >= 4 && [4]byte(reply[:4]) == magicErr {
			return nil, fmt.Errorf("%w: the server has no transport key configured", ErrTransportAuth)
		}
		return nil, fmt.Errorf("secure handshake: %w", err)
	}
	serverPub, err := ecdh.X25519().NewPublicKey(reply[:32])
	if err != nil {
		return nil, fmt.Errorf("%w: bad server key share", ErrTransportAuth)
	}
	shared, err := priv.ECDH(serverPub)
	if err != nil {
		return nil, fmt.Errorf("%w: bad server key share", ErrTransportAuth)
	}
	transcript := append(append([]byte(nil), hello...), reply[:32+secureNonceSize]...)
	prk, c2s, s2c, err := secureKeys(psk, shared, transcript)
	if err != nil {
		return nil, err
	}
	if !hmac.Equal(reply[32+secureNonceSize:], secureMAC(prk, []byte("server finished"), transcript)) {
		return nil, fmt.Errorf("%w: the server does not have the same transport key", ErrTransportAuth)
	}
	if _, err := conn.Write(secureMAC(prk, []byte("client finished"), transcript)); err != nil {
		return nil, fmt.Errorf("secure handshake: %w", err)
	}
	return &secureConn{Conn: conn, seal: c2s, open: s2c}, nil
}

// secureServer runs the server side of the handshake on conn.
func secureServer(conn net.Conn, psk []byte) (net.Conn, error) {
	_ = conn.SetDeadline(time.Now().Add(secureHandshakeTimeout))
	defer conn.SetDeadline(time.Time{})

	hello := make([]byte, 4+32+secureNonceSize)
	if _, err := io.ReadFull(conn, hello[:4]); err != nil {
		return nil, fmt.Errorf("secure handshake: %w", err)
	}
	if [4]byte(hello[:4]) != secureMagic {
		// Most likely a client without a key speaking the plain protocol.
		// Tell it why, in the protocol it understands.
		_ = sendErr(conn, errMsgKeyRequired)
		return nil, fmt.Errorf("%w: peer did not start a secure handshake (no transport key on its side?)", ErrTransportAuth)
	}
	if _, err := io.ReadFull(conn, hello[4:]); err != nil {
		return nil, fmt.Errorf("secure handshake: %w", err)
	}
	clientPub, err := ecdh.X25519().NewPublicKey(hello[4 : 4+32])
	if err != nil {
		return nil, fmt.Errorf("%w: bad client key share", ErrTransportAuth)
	}
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	shared, err := priv.ECDH(clientPub)
	if err != nil {
		return nil, fmt.Errorf("%w: bad client key share", ErrTransportAuth)
	}
	reply := make([]byte, 32+secureNonceSize, 32+secureNonceSize+sha256.Size)
	copy(reply, priv.PublicKey().Bytes())
	if _, err := rand.Read(reply[32:]); err != nil {
		return nil, err
	}
	transcript := append(append([]byte(nil), hello...), reply...)
	prk, c2s, s2c, err := secureKeys(psk, shared, transcript)
	if err != nil {
		return nil, err
	}
	reply = append(reply, secureMAC(prk, []byte("server finished"), transcript)...)
	if _, err := conn.Write(reply); err != nil {
		return nil, fmt.Errorf("secure handshake: %w", err)
	}
	clientMAC := make([]byte, sha256.Size)
	if _, err := io.ReadFull(conn, clientMAC); err != nil {
		// A client with the wrong key rejects our MAC and hangs up here.
		return nil, fmt.Errorf("%w: peer aborted the handshake (wrong transport key on its side?): %v", ErrTransportAuth, err)
	}
	if !hmac.Equal(clientMAC, secureMAC(prk, []byte("client finished"), transcript)) {
		return nil, fmt.Errorf("%w: peer does not have the same transport key", ErrTransportAuth)
	}
	return &secureConn{Conn: conn, seal: s2c, open: c2s}, nil
}

// secureConn is a net.Conn whose payload is sealed frame by frame.
type secureConn struct {
	net.Conn
	seal, open cipher.AEAD

	wmu  sync.Mutex
	wctr uint64
	wbuf []byte

	rctr  uint64
	rbuf  []byte // ciphertext scratch
	plain []byte // decrypted, not yet returned
}

func secureNonce(ctr uint64) []byte {
	n := make([]byte, 12)
	binary.BigEndian.PutUint64(n[4:], ctr)
	return n
}

func (c *secureConn) Write(p []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	written := 0
	for len(p) > 0 {
		chunk := p
		if len(chunk) > secureMaxFrame {
			chunk = chunk[:secureMaxFrame]
		}
		need := 4 + len(chunk) + c.seal.Overhead()
		if cap(c.wbuf) < need {
			c.wbuf = make([]byte, 0, 4+secureMaxFrame+c.seal.Overhead())
		}
		buf := c.wbuf[:4]
		binary.BigEndian.PutUint32(buf, uint32(len(chunk)+c.seal.Overhead()))
		// The length prefix is authenticated too.
		buf = c.seal.Seal(buf, secureNonce(c.wctr), chunk, buf[:4])
		c.wctr++
		if _, err := c.Conn.Write(buf); err != nil {
			return written, err
		}
		written += len(chunk)
		p = p[len(chunk):]
	}
	return written, nil
}

func (c *secureConn) Read(p []byte) (int, error) {
	for len(c.plain) == 0 {
		var hdr [4]byte
		if _, err := io.ReadFull(c.Conn, hdr[:]); err != nil {
			return 0, err
		}
		n := int(binary.BigEndian.Uint32(hdr[:]))
		if n < c.open.Overhead() || n > secureMaxFrame+c.open.Overhead() {
			return 0, fmt.Errorf("%w: bad frame length %d", ErrTransportAuth, n)
		}
		if cap(c.rbuf) < n {
			c.rbuf = make([]byte, secureMaxFrame+c.open.Overhead())
		}
		buf := c.rbuf[:n]
		if _, err := io.ReadFull(c.Conn, buf); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return 0, err
		}
		plain, err := c.open.Open(buf[:0], secureNonce(c.rctr), buf, hdr[:])
		if err != nil {
			return 0, fmt.Errorf("%w: a frame failed authentication (tampered, replayed or out of order)", ErrTransportAuth)
		}
		c.rctr++
		c.plain = plain
	}
	n := copy(p, c.plain)
	c.plain = c.plain[n:]
	return n, nil
}
