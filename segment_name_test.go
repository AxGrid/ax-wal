package axwal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSegName(t *testing.T) {
	cases := []struct {
		in   string
		want segName
		bad  bool
	}{
		{in: "wal-1-", want: segName{Name: "wal-1-", FirstIndex: 1}},
		{in: "wal-42-", want: segName{Name: "wal-42-", FirstIndex: 42}},
		{in: "wal-1-100", want: segName{Name: "wal-1-100", FirstIndex: 1, LastIndex: 100, Closed: true}},
		{in: "wal-1-100.zst", want: segName{Name: "wal-1-100.zst", FirstIndex: 1, LastIndex: 100, Closed: true, Compression: "zst"}},
		{in: "wal-5-4", want: segName{Name: "wal-5-4", FirstIndex: 5, LastIndex: 4, Closed: true}}, // empty segment
		{in: "wal-0-", bad: true},
		{in: "wal-0-0", bad: true},
		{in: "wal-1-0", want: segName{Name: "wal-1-0", FirstIndex: 1, LastIndex: 0, Closed: true}}, // empty: F-1=0=L
		{in: "junk", bad: true},
		{in: "wal-", bad: true},
		{in: ".writer.lock", bad: true},
	}
	for _, c := range cases {
		got, err := parseSegName(c.in)
		if c.bad {
			require.Error(t, err, c.in)
			continue
		}
		require.NoError(t, err, c.in)
		require.Equal(t, c.want, got, c.in)
	}
}

func TestFormatNames(t *testing.T) {
	require.Equal(t, "wal-1-", formatOpenName(1))
	require.Equal(t, "wal-42-100", formatClosedName(42, 100))
}
