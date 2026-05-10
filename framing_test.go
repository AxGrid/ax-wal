package axwal

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFraming_Roundtrip(t *testing.T) {
	cases := [][]byte{
		[]byte(""),
		[]byte("hello"),
		bytes.Repeat([]byte{0xAB}, 1024),
	}
	var buf bytes.Buffer
	for _, p := range cases {
		frame, err := encodeRecord(p)
		require.NoError(t, err)
		buf.Write(frame)
	}
	for _, p := range cases {
		got, err := readRecord(&buf)
		require.NoError(t, err)
		require.Equal(t, p, got)
	}
	_, err := readRecord(&buf)
	require.ErrorIs(t, err, io.EOF)
}

func TestFraming_TruncatedHeader(t *testing.T) {
	frame, err := encodeRecord([]byte("payload"))
	require.NoError(t, err)
	_, err = readRecord(bytes.NewReader(frame[:3]))
	require.ErrorIs(t, err, ErrTruncatedRecord)
}

func TestFraming_TruncatedPayload(t *testing.T) {
	frame, err := encodeRecord([]byte("payload"))
	require.NoError(t, err)
	_, err = readRecord(bytes.NewReader(frame[:headerSize+2]))
	require.ErrorIs(t, err, ErrTruncatedRecord)
}

func TestFraming_BitFlip(t *testing.T) {
	frame, err := encodeRecord([]byte("payload-data"))
	require.NoError(t, err)
	frame[headerSize+3] ^= 0x01
	_, err = readRecord(bytes.NewReader(frame))
	require.ErrorIs(t, err, ErrCorruptRecord)
}

func TestFraming_OversizedDeclared(t *testing.T) {
	var hdr [headerSize]byte
	hdr[0] = 0xFF
	hdr[1] = 0xFF
	hdr[2] = 0xFF
	hdr[3] = 0xFF
	_, err := readRecord(bytes.NewReader(hdr[:]))
	require.True(t, errors.Is(err, ErrCorruptRecord), "want ErrCorruptRecord, got %v", err)
}
