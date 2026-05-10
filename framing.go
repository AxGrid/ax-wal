package axwal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// Record on-disk frame: [len:u32 BE][crc32c(payload):u32 BE][payload].
const (
	headerSize    = 8
	maxRecordSize = 256 * 1024 * 1024 // 256 MiB safety cap
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrCorruptRecord   = errors.New("axwal: record CRC mismatch")
	ErrTruncatedRecord = errors.New("axwal: record truncated")
	ErrRecordTooLarge  = errors.New("axwal: record exceeds max size")
)

// encodeRecord builds a complete frame for payload. The returned slice is owned by the caller.
func encodeRecord(payload []byte) ([]byte, error) {
	if len(payload) > maxRecordSize {
		return nil, fmt.Errorf("%w: %d > %d", ErrRecordTooLarge, len(payload), maxRecordSize)
	}
	buf := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.Checksum(payload, crcTable))
	copy(buf[headerSize:], payload)
	return buf, nil
}

// readRecord pulls a single framed record from r.
// On a clean stream end returns io.EOF.
// On a partial header or partial payload returns ErrTruncatedRecord (so recovery can
// distinguish "trailing junk to truncate" from "good record").
// On CRC mismatch returns ErrCorruptRecord.
func readRecord(r io.Reader) ([]byte, error) {
	var hdr [headerSize]byte
	n, err := io.ReadFull(r, hdr[:])
	if err == io.EOF && n == 0 {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("%w: header (%d/%d): %v", ErrTruncatedRecord, n, headerSize, err)
	}

	length := binary.BigEndian.Uint32(hdr[0:4])
	want := binary.BigEndian.Uint32(hdr[4:8])

	if length > maxRecordSize {
		return nil, fmt.Errorf("%w: declared len %d", ErrCorruptRecord, length)
	}

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("%w: payload (want %d): %v", ErrTruncatedRecord, length, err)
		}
	}

	got := crc32.Checksum(payload, crcTable)
	if got != want {
		return nil, fmt.Errorf("%w: want %08x got %08x", ErrCorruptRecord, want, got)
	}
	return payload, nil
}
