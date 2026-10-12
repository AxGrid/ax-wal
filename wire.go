package axwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Wire protocol used by RemoteReader / Server. Designed to be readable in a
// hex dump and parseable in <30 lines without a schema compiler.
//
// Every frame on the wire:
//
//	[ 4B magic ] [ 4B body length, big-endian uint32 ] [ N bytes body ]
//
// Some frames have a fixed-size body (the length field is still there for
// uniform parsing). The magic distinguishes the frame kind:
//
//	"AXWH"  HELLO   client → server, JSON-encoded helloPayload
//	"AXOK"  OK      server → client, 8 bytes head_index (BE uint64)
//	"AXER"  ERR     server → client, JSON-encoded errPayload
//	"AXRB"  BATCH   server → client, packed records (see below)
//	"AXPI"  PING    server → client, empty body — keepalive when idle
//	"AXAC"  ACK     client → server, 8 bytes last_received_index (BE uint64)
//
// BATCH body layout:
//
//	[ 4B count ]
//	for each record:
//	    [ 8B index BE u64 ] [ 4B payload_len BE u32 ] [ payload bytes ]
//
// The protocol is plain TCP. Confidentiality and authentication are the
// transport's problem (SSH tunnel or TLS); see the Server / RemoteReader
// docs.
const wireMaxBodyBytes = 256 << 20 // 256 MiB sanity ceiling per frame

var (
	magicHello = [4]byte{'A', 'X', 'W', 'H'}
	magicOK    = [4]byte{'A', 'X', 'O', 'K'}
	magicErr   = [4]byte{'A', 'X', 'E', 'R'}
	magicBatch = [4]byte{'A', 'X', 'R', 'B'}
	magicPing  = [4]byte{'A', 'X', 'P', 'I'}
	magicAck   = [4]byte{'A', 'X', 'A', 'C'}
)

type helloPayload struct {
	Name       string `json:"name"`
	StartIndex uint64 `json:"start_index"`
	BatchSize  int    `json:"batch_size"`

	// Exact makes StartIndex authoritative: the server-side cursor is reset
	// to StartIndex-1 instead of resuming from wherever it was. A replica
	// sets it, because its own WAL — not the source's cursor — is the truth
	// about what it has.
	Exact bool `json:"exact,omitempty"`
}

type errPayload struct {
	Error string `json:"error"`
}

// errSecureHello: a plain endpoint received the opening of a secure handshake.
var errSecureHello = errors.New("axwal/wire: peer started a secure handshake, but no transport key is configured here")

// writeFrame emits a single frame. Caller is responsible for serializing
// concurrent writes on the same connection.
func writeFrame(w io.Writer, magic [4]byte, body []byte) error {
	if len(body) > wireMaxBodyBytes {
		return fmt.Errorf("axwal/wire: body too large (%d > %d)", len(body), wireMaxBodyBytes)
	}
	var hdr [8]byte
	copy(hdr[0:4], magic[:])
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(body)))
	// Small frames (ACK, PING, HELLO) go out in one write: one packet, and
	// on an encrypted connection one sealed frame instead of two.
	if len(body) <= 4096 {
		buf := make([]byte, 0, len(hdr)+len(body))
		buf = append(append(buf, hdr[:]...), body...)
		_, err := w.Write(buf)
		return err
	}
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

// readFrame returns the next frame's magic + body. On a clean connection
// close (EOF before any header byte) returns io.EOF.
func readFrame(r io.Reader) ([4]byte, []byte, error) {
	var hdr [8]byte
	n, err := io.ReadFull(r, hdr[:])
	var magic [4]byte
	if err == io.EOF && n == 0 {
		return magic, nil, io.EOF
	}
	if err != nil {
		return magic, nil, fmt.Errorf("axwal/wire: header: %w", err)
	}
	copy(magic[:], hdr[0:4])
	if magic == secureMagic {
		// The peer is starting an encrypted session and we are not
		// configured for one; what follows isn't a frame.
		return magic, nil, errSecureHello
	}
	bodyLen := binary.BigEndian.Uint32(hdr[4:8])
	if bodyLen > wireMaxBodyBytes {
		return magic, nil, fmt.Errorf("axwal/wire: oversized body %d", bodyLen)
	}
	if bodyLen == 0 {
		return magic, nil, nil
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return magic, nil, fmt.Errorf("axwal/wire: body: %w", err)
	}
	return magic, body, nil
}

// encodeBatch packs (indices, payloads) into the body of a BATCH frame.
// Caller passes them already aligned 1:1.
func encodeBatch(indices []uint64, payloads [][]byte) []byte {
	var size = 4
	for _, p := range payloads {
		size += 8 + 4 + len(p)
	}
	out := make([]byte, size)
	binary.BigEndian.PutUint32(out[0:4], uint32(len(indices)))
	off := 4
	for i := range indices {
		binary.BigEndian.PutUint64(out[off:off+8], indices[i])
		off += 8
		binary.BigEndian.PutUint32(out[off:off+4], uint32(len(payloads[i])))
		off += 4
		copy(out[off:off+len(payloads[i])], payloads[i])
		off += len(payloads[i])
	}
	return out
}

// decodeBatch parses a BATCH body. payloads[i] points into body — copy if
// you need to retain them past the next read.
func decodeBatch(body []byte) ([]uint64, [][]byte, error) {
	if len(body) < 4 {
		return nil, nil, errors.New("axwal/wire: short batch")
	}
	count := binary.BigEndian.Uint32(body[0:4])
	off := 4
	indices := make([]uint64, count)
	payloads := make([][]byte, count)
	for i := uint32(0); i < count; i++ {
		if off+12 > len(body) {
			return nil, nil, errors.New("axwal/wire: short record header")
		}
		indices[i] = binary.BigEndian.Uint64(body[off : off+8])
		off += 8
		plen := int(binary.BigEndian.Uint32(body[off : off+4]))
		off += 4
		if off+plen > len(body) {
			return nil, nil, errors.New("axwal/wire: short record payload")
		}
		// copy so caller can keep the slice across reads
		p := make([]byte, plen)
		copy(p, body[off:off+plen])
		payloads[i] = p
		off += plen
	}
	return indices, payloads, nil
}
