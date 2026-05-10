package axwal

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// freePort returns an OS-picked free TCP port on 127.0.0.1.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func TestServer_RemoteReaderRoundtrip(t *testing.T) {
	dir := t.TempDir()

	// Pre-load the source WAL.
	w, err := OpenWriter(WriterOptions{Dir: dir, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("rec-%02d", i)))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	addr := freePort(t)
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()

	srvDone := make(chan error, 1)
	go func() {
		srvDone <- Serve(srvCtx, ServerOptions{
			Dir:               dir,
			Listen:            addr,
			KeepAliveInterval: 100 * time.Millisecond,
			AckBatchInterval:  20 * time.Millisecond,
		})
	}()

	// Wait for server to bind.
	require.Eventually(t, func() bool {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}, 2*time.Second, 20*time.Millisecond)

	// Connect a RemoteReader and drain all records.
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	rr, err := DialRemoteReader(dialCtx, RemoteReaderOptions{
		Addr:      addr,
		Name:      "follower-1",
		BatchSize: 16,
		AckEvery:  10,
	})
	require.NoError(t, err)
	defer rr.Close()

	require.Equal(t, uint64(50), rr.HeadIndex())

	readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer readCancel()
	for i := 0; i < 50; i++ {
		payload, idx, err := rr.Next(readCtx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), idx)
		require.Equal(t, fmt.Sprintf("rec-%02d", i), string(payload))
		require.NoError(t, rr.Ack(idx))
	}
	require.NoError(t, rr.Close())

	srvCancel()
	<-srvDone
}

func TestMirror_OverNetwork(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	// Pre-load source.
	w, err := OpenWriter(WriterOptions{Dir: src, MaxSegmentSize: 1 << 20})
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		_, err := w.Append([]byte(fmt.Sprintf("v-%03d", i)))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	addr := freePort(t)
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	go func() {
		_ = Serve(srvCtx, ServerOptions{Dir: src, Listen: addr})
	}()
	require.Eventually(t, func() bool {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}, 2*time.Second, 20*time.Millisecond)

	mirrorCtx, mirrorCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mirrorCancel()
	mirrorDone := make(chan error, 1)
	go func() {
		_, err := Mirror(mirrorCtx, MirrorOptions{
			SrcRemote: &RemoteReaderOptions{
				Addr:      addr,
				Name:      "mirror-net",
				BatchSize: 16,
			},
			DstDir:    dst,
			BatchSize: 16,
		})
		mirrorDone <- err
	}()

	// Wait for dst to catch up.
	require.Eventually(t, func() bool {
		r, err := OpenReader(ReaderOptions{
			Dir:       dst,
			Name:      "watcher-" + fmt.Sprint(time.Now().UnixNano()),
			StartFrom: StartLatest,
		})
		if err != nil {
			return false
		}
		head, _ := r.HeadIndex()
		r.Close()
		return head == 100
	}, 4*time.Second, 100*time.Millisecond)

	mirrorCancel()
	<-mirrorDone
	srvCancel()

	// Verify dst content.
	r, err := OpenReader(ReaderOptions{Dir: dst, Name: "verify", StartFrom: StartEarliest})
	require.NoError(t, err)
	defer r.Close()
	verifyCtx, vcancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer vcancel()
	for i := 0; i < 100; i++ {
		payload, idx, err := r.Next(verifyCtx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), idx)
		require.Equal(t, fmt.Sprintf("v-%03d", i), string(payload))
		require.NoError(t, r.Ack(idx))
	}
}

func TestServer_RejectsBadHello(t *testing.T) {
	dir := t.TempDir()
	addr := freePort(t)
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	go func() { _ = Serve(srvCtx, ServerOptions{Dir: dir, Listen: addr}) }()
	require.Eventually(t, func() bool {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			return false
		}
		c.Close()
		return true
	}, 2*time.Second, 20*time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	// Send garbage as the first frame.
	require.NoError(t, writeFrame(conn, [4]byte{'X', 'X', 'X', 'X'}, []byte("oops")))
	magic, body, err := readFrame(conn)
	require.NoError(t, err)
	require.Equal(t, magicErr, magic)
	require.Contains(t, string(body), "expected HELLO")
}
