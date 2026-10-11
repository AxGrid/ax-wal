package axwal

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

// SSHTunnelConfig describes how to dial the primary host over SSH so the
// RemoteReader can establish its TCP connection through the tunnel — no
// external `ssh -L` process required.
//
// Auth precedence: KeyFile (if provided) → SSH agent (if Agent=true and
// SSH_AUTH_SOCK is set) → fail. Host-key verification reads KnownHostsFile
// if set; otherwise InsecureIgnoreHostKey is used (and a loud warning logged
// once per dial — the WAL package does not silently accept any key).
type SSHTunnelConfig struct {
	// Host of the SSH server: "primary.example.com:22". Port is required.
	Host string

	// User on the SSH server.
	User string

	// KeyFile is a PEM private key. Required unless Agent is true.
	KeyFile string

	// KeyPassphrase decrypts an encrypted private key. Empty for unencrypted.
	KeyPassphrase string

	// Agent, when true, pulls signers from $SSH_AUTH_SOCK (in addition to
	// KeyFile if also set).
	Agent bool

	// KnownHostsFile is the path of an OpenSSH-format known_hosts file. If
	// set, the server's host key must match an entry. If empty, host-key
	// verification is SKIPPED — usable for trusted-network deployments but
	// emits a one-line warning per dial. Production deployments should
	// always set this.
	KnownHostsFile string

	// DialTimeout caps the connect deadline. Defaults to 15 s.
	DialTimeout time.Duration
}

func dialSSH(ctx context.Context, cfg *SSHTunnelConfig) (*ssh.Client, error) {
	if cfg.Host == "" || cfg.User == "" {
		return nil, errors.New("axwal: SSHTunnelConfig requires Host and User")
	}
	timeout := cfg.DialTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}

	var auth []ssh.AuthMethod
	if cfg.Agent {
		sock := os.Getenv("SSH_AUTH_SOCK")
		if sock == "" {
			return nil, errors.New("axwal: SSHTunnelConfig.Agent set but SSH_AUTH_SOCK is empty")
		}
		ac, err := net.DialTimeout("unix", sock, timeout)
		if err != nil {
			return nil, fmt.Errorf("dial ssh-agent: %w", err)
		}
		// Only needed for the handshake; without this every reconnect would
		// leak one agent connection.
		defer ac.Close()
		ag := agent.NewClient(ac)
		auth = append(auth, ssh.PublicKeysCallback(ag.Signers))
	}
	if cfg.KeyFile != "" {
		raw, err := os.ReadFile(cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("read ssh key: %w", err)
		}
		var signer ssh.Signer
		if cfg.KeyPassphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(raw, []byte(cfg.KeyPassphrase))
		} else {
			signer, err = ssh.ParsePrivateKey(raw)
		}
		if err != nil {
			return nil, fmt.Errorf("parse ssh key: %w", err)
		}
		auth = append(auth, ssh.PublicKeys(signer))
	}
	if len(auth) == 0 {
		return nil, errors.New("axwal: SSHTunnelConfig has no auth method (set KeyFile or Agent)")
	}

	hostKeyCallback, err := makeHostKeyCallback(cfg.KnownHostsFile)
	if err != nil {
		return nil, err
	}

	clientCfg := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            auth,
		HostKeyCallback: hostKeyCallback,
		Timeout:         timeout,
	}

	d := net.Dialer{Timeout: timeout}
	tcpConn, err := d.DialContext(ctx, "tcp", cfg.Host)
	if err != nil {
		return nil, fmt.Errorf("dial ssh tcp: %w", err)
	}
	// Ask the server for a host key type we actually have in known_hosts.
	// Left to its defaults the client may negotiate e.g. ECDSA while the
	// file only lists the host's ED25519 key, and fail with "key mismatch"
	// on a host that plain `ssh` accepts.
	if algos := knownHostKeyAlgorithms(hostKeyCallback, cfg.Host, tcpConn.RemoteAddr()); len(algos) > 0 {
		clientCfg.HostKeyAlgorithms = algos
	}

	// ClientConfig.Timeout only covers ssh.Dial's TCP connect; bound the key
	// exchange and auth ourselves so a peer that accepts and then goes
	// silent can't hang the dial forever.
	_ = tcpConn.SetDeadline(time.Now().Add(timeout))
	c, chans, reqs, err := ssh.NewClientConn(tcpConn, cfg.Host, clientCfg)
	if err != nil {
		_ = tcpConn.Close()
		return nil, fmt.Errorf("ssh handshake: %w", err)
	}
	_ = tcpConn.SetDeadline(time.Time{})
	return ssh.NewClient(c, chans, reqs), nil
}

func makeHostKeyCallback(knownHostsFile string) (ssh.HostKeyCallback, error) {
	if knownHostsFile == "" {
		// Insecure-but-explicit. RemoteReader logs a warning at the top level.
		return ssh.InsecureIgnoreHostKey(), nil
	}
	cb, err := knownhosts.New(knownHostsFile)
	if err != nil {
		return nil, fmt.Errorf("read known_hosts %s: %w", knownHostsFile, err)
	}
	return cb, nil
}

// knownHostKeyAlgorithms returns the host key algorithms for which the
// known_hosts callback has an entry for this host (nil if none / not a
// known_hosts callback). It probes the callback with a key that can't match
// and reads the expected keys out of the resulting error.
func knownHostKeyAlgorithms(cb ssh.HostKeyCallback, host string, remote net.Addr) []string {
	var keyErr *knownhosts.KeyError
	if err := cb(host, remote, probeHostKey{}); !errors.As(err, &keyErr) {
		return nil
	}
	var algos []string
	seen := map[string]bool{}
	add := func(a string) {
		if !seen[a] {
			seen[a] = true
			algos = append(algos, a)
		}
	}
	for _, k := range keyErr.Want {
		switch t := k.Key.Type(); t {
		case ssh.KeyAlgoRSA:
			// One RSA key serves all three signature schemes.
			add(ssh.KeyAlgoRSASHA512)
			add(ssh.KeyAlgoRSASHA256)
			add(ssh.KeyAlgoRSA)
		default:
			add(t)
		}
	}
	return algos
}

// probeHostKey is a public key of a type no host has.
type probeHostKey struct{}

func (probeHostKey) Type() string                        { return "axwal-probe" }
func (probeHostKey) Marshal() []byte                     { return []byte("axwal-probe") }
func (probeHostKey) Verify([]byte, *ssh.Signature) error { return errors.New("probe key") }
