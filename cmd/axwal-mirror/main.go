// axwal-mirror runs a foreground replication loop from a source WAL to a
// destination WAL. Resumable via a stable cursor name; designed to live under
// systemd / supervisord for hot-standby followers.
//
// Local source (both dirs visible from this process):
//
//	axwal-mirror -src /shared/leader -dst /local/follower -name follower-1
//
// Remote source over SSH tunnel (no separate `ssh -L` process — the mirror
// dials SSH itself):
//
//	axwal-mirror \
//	  -tunnel root@primary.example.com:22 \
//	  -ssh-key ~/.ssh/id_ed25519 \
//	  -ssh-known-hosts ~/.ssh/known_hosts \
//	  -addr 127.0.0.1:9001 \
//	  -dst /local/follower \
//	  -name follower-1
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	axwal "github.com/axgrid/ax-wal"
)

func main() {
	src := flag.String("src", "", "LOCAL source WAL directory (omit when using -tunnel/-addr)")
	dst := flag.String("dst", "", "destination WAL directory")
	name := flag.String("name", "", "cursor name in source — stable across restarts")
	from := flag.String("from", "earliest", "first-time start: earliest | latest | <index>")
	batch := flag.Int("batch", 256, "records per round trip")

	// Remote source flags.
	addr := flag.String("addr", "", "REMOTE: axwal-serve address. Without -tunnel: reachable directly. With -tunnel: address as seen from the SSH host (typically 127.0.0.1:9001)")
	tunnel := flag.String("tunnel", "", "REMOTE via SSH: user@host[:port] (port defaults to 22)")
	sshKey := flag.String("ssh-key", "", "path to SSH private key (PEM)")
	sshKeyPass := flag.String("ssh-key-pass", "", "passphrase for an encrypted SSH key")
	sshAgent := flag.Bool("ssh-agent", false, "use SSH_AUTH_SOCK as an additional auth method")
	sshKnownHosts := flag.String("ssh-known-hosts", "", "path to OpenSSH known_hosts (empty = INSECURE: skip host-key verification)")

	srcKey := flag.String("src-key-file", "", "32B AES key for local encrypted source")
	dstKey := flag.String("dst-key-file", "", "32B AES key for encrypted destination")
	statusEvery := flag.Duration("status", 5*time.Second, "status log interval; 0 disables")
	flag.Parse()

	if *dst == "" {
		fmt.Fprintln(os.Stderr, "axwal-mirror: -dst is required")
		flag.Usage()
		os.Exit(2)
	}
	if *src == "" && *addr == "" {
		fmt.Fprintln(os.Stderr, "axwal-mirror: provide either -src (local) or -addr (remote)")
		flag.Usage()
		os.Exit(2)
	}
	if *src != "" && *addr != "" {
		log.Fatalf("axwal-mirror: -src and -addr are mutually exclusive")
	}

	opts := axwal.MirrorOptions{
		DstDir:    *dst,
		Name:      *name,
		BatchSize: *batch,
	}
	switch *from {
	case "earliest":
		opts.StartFrom = axwal.StartEarliest
	case "latest":
		opts.StartFrom = axwal.StartLatest
	default:
		var idx uint64
		_, err := fmt.Sscanf(*from, "%d", &idx)
		if err != nil || idx == 0 {
			log.Fatalf("invalid -from %q", *from)
		}
		opts.StartFrom = axwal.StartAt
		opts.StartAtIndex = idx
	}

	if k := loadKeyFile(*srcKey); k != nil {
		opts.SrcReaderOptions.Encryption = k
	}
	if k := loadKeyFile(*dstKey); k != nil {
		opts.DstWriterOptions.Encryption = k
	}

	if *src != "" {
		opts.SrcDir = *src
		if *name == "" {
			log.Fatalf("axwal-mirror: -name is required for local source")
		}
	} else {
		// Remote source.
		if *name == "" {
			log.Fatalf("axwal-mirror: -name is required (used as the source-side cursor name)")
		}
		opts.SrcRemote = &axwal.RemoteReaderOptions{
			Addr:      *addr,
			Name:      *name,
			BatchSize: *batch,
		}
		if *tunnel != "" {
			user, host := parseTunnel(*tunnel)
			opts.SrcRemote.SSHTunnel = &axwal.SSHTunnelConfig{
				Host:           host,
				User:           user,
				KeyFile:        *sshKey,
				KeyPassphrase:  *sshKeyPass,
				Agent:          *sshAgent,
				KnownHostsFile: *sshKnownHosts,
			}
		}
	}

	ctx, cancel := signalContext()
	defer cancel()

	if *statusEvery > 0 {
		go statusLoop(ctx, opts, *statusEvery)
	}

	source := *src
	if source == "" {
		source = *addr
		if *tunnel != "" {
			source = *tunnel + " → " + *addr
		}
	}
	log.Printf("mirror: %s → %s (cursor=%s, batch=%d)", source, *dst, *name, *batch)

	mirrored, err := axwal.Mirror(ctx, opts)
	if err != nil {
		log.Fatalf("mirror: %v", err)
	}
	log.Printf("mirror: stopped after %d records", mirrored)
}

func parseTunnel(spec string) (user, host string) {
	at := strings.IndexByte(spec, '@')
	if at < 0 {
		log.Fatalf("axwal-mirror: -tunnel must be user@host[:port]")
	}
	user = spec[:at]
	host = spec[at+1:]
	if !strings.Contains(host, ":") {
		host += ":22"
	}
	return user, host
}

func loadKeyFile(path string) *axwal.EncryptionConfig {
	if path == "" {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read key %s: %v", path, err)
	}
	if len(b) != 32 {
		log.Fatalf("key file %s must be 32 bytes, got %d", path, len(b))
	}
	return &axwal.EncryptionConfig{Key: b}
}

// statusLoop logs mirror progress. For a remote source it just prints the
// destination's head index periodically (cheap). For a local source it
// additionally opens an extra read-only Reader to compute lag.
func statusLoop(ctx context.Context, opts axwal.MirrorOptions, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Open a brief Reader on dst to read its head — Writer is locked
			// by the mirror so we can't open it directly.
			r, err := axwal.OpenReader(axwal.ReaderOptions{
				Dir:               opts.DstDir,
				Name:              "axwal-mirror.status." + fmt.Sprint(time.Now().UnixNano()),
				StartFrom:         axwal.StartLatest,
				HeartbeatInterval: every,
				PollInterval:      time.Second,
			})
			if err != nil {
				continue
			}
			head, _ := r.HeadIndex()
			r.Close()
			log.Printf("status: dst.head=%d", head)
		}
	}
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	return ctx, cancel
}
