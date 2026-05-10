package axwal

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriterLock_SecondAttemptFails(t *testing.T) {
	dir := t.TempDir()
	l1, err := acquireWriterLock(dir)
	require.NoError(t, err)
	defer l1.Release()

	_, err = acquireWriterLock(dir)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "another writer"), "got: %v", err)
}

func TestWriterLock_ReleaseAllowsSecond(t *testing.T) {
	dir := t.TempDir()
	l1, err := acquireWriterLock(dir)
	require.NoError(t, err)
	require.NoError(t, l1.Release())

	l2, err := acquireWriterLock(dir)
	require.NoError(t, err)
	require.NoError(t, l2.Release())
}
