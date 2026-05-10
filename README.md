<p align="center">
  <img src="assets/logo.svg" width="220" alt="ax-wal" />
</p>

# ax-wal

A single-writer / many-reader, file-system-backed Write-Ahead Log for Go.
Append-only, durable, with at-least-once tail-and-follow semantics; meant as
a building block for queues, replication logs, audit trails, and metric
buffers.

```
producer ──► [writer fsync ─► segment file] ─► fsnotify ─► [reader → Ack]
                                            ╲                  ╱
                                             cursor file (per reader)
```

## What it is

- **Single writer, N readers.** Writer holds an exclusive `flock` on the
  directory; readers each register a unique cursor. Both can be in different
  processes or different machines on the same filesystem.
- **CRC32C-framed records.** Torn writes at crash time are detected and
  truncated by recovery; corruption inside a closed segment is reported, not
  silently swallowed.
- **Tail-and-follow.** `Reader.Next(ctx)` blocks until new records arrive;
  uses fsnotify with a polling fallback. `Reader.NextBatch(ctx, max)` for
  amortizing per-record overhead.
- **Auto-rotation.** By size, by age, by record count — any combination.
- **Cursor-aware retention** with optional time-based hard-purge
  (`MaxRetainAge`). Stale readers (heartbeat older than TTL) are excluded
  from the cursor floor so one dead consumer can't fill the disk.
- **Archive gate.** Optionally require an external uploader (S3 / B2 / cold
  storage) to drop a `wal-F-L.archived` sidecar before retention reclaims
  the local copy.
- **Tunable durability.** `SyncFull` / `SyncBarrier` / `SyncNone` — pick the
  trade-off between latency, throughput, and survivability of last-second
  writes. Pair with `GroupCommit` to amortize fsync cost across concurrent
  writers; on the read side `AckBatchInterval` does the same for cursor
  fsyncs (~1.7 M reads/s in our bench when both sides are batched).
- **Optional encryption-at-rest** via AES-256-GCM. Bring-your-own-key.
- **Optional `mmap` reads** for huge cold replays.
- **Built-in replication.** `axwal.Mirror(ctx, ...)` library + `axwal-mirror`
  binary copy a primary WAL to a follower in real time.
- **Operational tooling.** `axwal-cli ls / cursors / stats / verify / fsck /
  dump / restore / compact / tail / mark-archived`.
- **Stats and structured logging.** `Writer.Stats()` / `Reader.Stats()`
  return atomic counters; logs go through `*slog.Logger` with `dir` and
  reader `name` keys on every line.
- **Crash-tested.** Property-based test runs 30 randomized
  Append/AppendBatch/Rotate/SIGKILL sequences and asserts every committed
  record is readable after recovery.

## What it isn't

- Not a network-attached message broker. There's no consumer-group protocol,
  no JOIN/LEAVE handshake, no leader election — you give it a filesystem
  and a process, and it gives you a durable log.
- Not multi-writer. The `flock` on the directory is by design.
- Not Windows. The lock and per-OS sync helpers are POSIX (Linux + macOS
  tested).

## Quick start

```go
import axwal "github.com/axgrid/ax-wal"

// Producer
w, _ := axwal.OpenWriter(axwal.WriterOptions{
    Dir:            "/var/wal/orders",
    MaxSegmentSize: 64 << 20,
})
defer w.Close()
idx, _ := w.Append([]byte("order-42"))
fmt.Println("durable at index", idx)

// Consumer (any number of these, each in its own process)
r, _ := axwal.OpenReader(axwal.ReaderOptions{
    Dir:       "/var/wal/orders",
    Name:      "billing-worker-1",
    StartFrom: axwal.StartEarliest,
})
defer r.Close()
for {
    payload, idx, err := r.Next(ctx)
    if err != nil { break }
    process(payload)
    _ = r.Ack(idx)
}
```

## Configuration

### Writer

```go
type WriterOptions struct {
    Dir                 string
    MaxSegmentSize      int64
    MaxSegmentAge       time.Duration
    MaxSegmentRecords   uint64
    PreallocateSegments bool          // fallocate / F_PREALLOCATE
    CompressOnRotate    bool          // async zstd
    OnRotate            func(SegmentInfo)
    WriteMetaSidecar    bool          // wal-F-L.meta JSON with sha256
    SyncMode            SyncMode      // Full / Barrier / None
    GroupCommit         GroupCommitConfig
    Retention           RetentionConfig
    Encryption          *EncryptionConfig
    MaxPendingBytes     int           // backpressure ceiling
    BufferSize          int
    VerifyOnOpen        bool          // CRC-walk + dedupe + gap report on open
    Logger              *slog.Logger
    CloseTimeout        time.Duration // bounded shutdown
}
```

### Reader

```go
type ReaderOptions struct {
    Dir               string
    Name              string         // unique per active reader
    StartFrom         StartPosition  // Earliest | Latest | At
    StartAtIndex      uint64
    HeartbeatInterval time.Duration  // < StaleReaderTTL/2
    AckBatchInterval  time.Duration  // 0 = strict per-Ack fsync
    PollInterval      time.Duration  // fsnotify fallback
    UseMmap           bool           // mmap closed uncompressed segments
    Encryption        *EncryptionConfig
    Logger            *slog.Logger
}
```

### SyncMode — the durability dial

| Mode | Behavior | macOS cost | Linux cost | Loss on hard crash |
|---|---|---|---|---|
| `SyncFull` (default) | `*os.File.Sync()` | F_FULLFSYNC, ~5–15 ms | fsync, ~3–4 ms | nothing acked is lost |
| `SyncBarrier` | order-only barrier | F_BARRIERFSYNC, ~0.4 ms | fdatasync, ~3 ms | last few hundred ms |
| `SyncNone` | no flush | none | none | last few seconds |

`Writer.Sync()` always does a hard fsync regardless of `SyncMode` — use it
when you've opted into a weaker mode but need a guarantee right now (e.g.,
before reporting "transaction committed" upstream).

### GroupCommit — amortizing fsync cost

```go
type GroupCommitConfig struct {
    Enabled       bool
    MaxLatency    time.Duration  // default 1ms
    MaxBatchBytes int            // default 1 MiB
}
```

Concurrent `Append` calls share a single fsync. Each call still **blocks**
until its records are durable — semantics unchanged, throughput up. Larger
`MaxLatency` = more records per fsync = higher throughput, at the cost of
latency. `AppendContext(ctx, …)` and `AppendBatchContext(ctx, …)` respect
ctx cancellation while waiting on the cond.

**Anti-pattern**: GroupCommit + SyncBarrier or SyncNone — the per-fsync cost
is already cheap, so the 1 ms commit window becomes the dominant latency
and you give up most of the speed you came for.

### AckBatchInterval — the reader's group commit

Per-record `Ack()` does an fsync of the cursor file. On macOS that caps a
single reader at ~70 records/s; on Linux ~60. Setting `AckBatchInterval > 0`
makes `Ack()` lock-free and lets a background goroutine fsync the cursor at
that interval. A clean `Close()` always flushes the latest Ack first.

| AckBatchInterval | Ack call cost | Records re-delivered after hard kill |
|---|---|---|
| 0 (strict) | 1 fsync | 0 (after the last successful Ack) |
| 10 ms | atomic update | up to 10 ms × throughput |
| 100 ms | atomic update | up to 100 ms × throughput |

### Retention

```go
type RetentionConfig struct {
    Enabled              bool
    Interval             time.Duration  // default 30s
    MinRetainRecords     uint64
    StaleReaderTTL       time.Duration  // default 60s
    RequireArchivedMark  bool           // gate on wal-F-L.archived sidecar
    MaxRetainAge         time.Duration  // hard purge by age (compliance)
}
```

A closed segment is deleted iff:

1. **Cursor cutoff:** all non-stale cursors have advanced past it.
2. **`MinRetainRecords` floor** is satisfied (keep last N records always).
3. **OR `MaxRetainAge` expired** — segment older than this is deleted
   regardless of cursors. Pair with a documented consumer SLO.
4. **If `RequireArchivedMark`** is set, a sibling `wal-F-L.archived` must
   also exist. Use `Writer.MarkArchived(F, L)` from your in-process archiver,
   or `axwal-cli mark-archived` from a separate uploader.

The active open segment is never touched.

### OnRotate, .meta sidecars, archived sidecars

Three independent extension points for the rotation path:

| Hook | Trigger | Use case |
|---|---|---|
| `OnRotate(SegmentInfo)` callback | After rename + (optional) zstd | In-process upload, replication trigger |
| `wal-F-L.meta` sidecar | If `WriteMetaSidecar` is set | Audit, backup verification (sha256), cloud catalog |
| `wal-F-L.archived` sidecar | Created by `MarkArchived` | Retention gate for out-of-process uploaders |

Panics in `OnRotate` are recovered and logged — they can't tear down the
writer.

### Encryption at rest

```go
key := loadKeyFromKMS() // 32 bytes for AES-256
opts := axwal.WriterOptions{
    Dir:        "/var/wal/encrypted",
    Encryption: &axwal.EncryptionConfig{Key: key},
}
```

Every record's payload is sealed with AES-256-GCM before framing. On-disk
body becomes `[12B random nonce | gcm_seal(plaintext)]`. The frame's CRC32C
still covers the full sealed body, so framing-layer corruption is detected
before the AEAD runs.

A reader without the key can scan framing (`verify`, `ls` work fine) but
gets `ErrEncryption` when it tries to read a record. Mismatched key →
`ErrEncryption`.

CLI tools accept `-key-file path/to/key` (raw 32 bytes). No key rotation in
this version — re-key by running `axwal-mirror` from old to new with
different `-src-key-file` / `-dst-key-file`.

### Reader.Lag / Reader.HeadIndex

```go
records, bytes, _ := r.Lag()
// records: number of committed indices past your cursor
// bytes:   estimated on-disk bytes from the cursor onward
```

Cheap (one glob). Use for "consumer falling behind" alerts.

### Reader.NextBatch

```go
payloads, indices, err := r.NextBatch(ctx, 256)
```

Up to N records in one call; blocks for the first, drains best-effort for
the rest. Cuts per-record overhead for batched processors (DB inserts, S3
multipart uploads).

### Writer.TruncatePrefix

```go
removed, _ := w.TruncatePrefix(uptoIndex)
```

Synchronously deletes closed segments where `LastIndex < uptoIndex`,
including their `.archived` and `.meta` sidecars. Bypasses cursor protection
— call when you know no consumer will need that range again.

### VerifyOnOpen

`OpenWriter(opts).VerifyOnOpen = true` triggers a full CRC walk of every
closed segment at startup. Catches:

- Bit-flips inside closed segments → segment removed (cluster redundancy /
  backups must repopulate).
- Duplicate ranges (`wal-1-100` and `wal-1-100.zst` coexist) → uncompressed
  twin removed, .zst kept.
- Gaps in the index chain → logged as warnings (the data was already lost).

Costly — proportional to total WAL size. Run on operator-suspected damage
or after pulling a backup.

### Replication / Mirror

Three deployment patterns, same Mirror primitive in all of them:

**(1) Local: both directories visible from one process**

```go
axwal.Mirror(ctx, axwal.MirrorOptions{
    SrcDir: "/shared/leader-wal",
    DstDir: "/local/follower-wal",
    Name:   "follower-1",
    BatchSize: 256,
})
```

```bash
axwal-mirror -src /shared/leader -dst /local/follower -name follower-1
```

**(2) Remote: TCP between two hosts**

On the primary:

```bash
axwal-serve -dir /var/wal -listen 127.0.0.1:9001
```

On the follower:

```bash
axwal-mirror -addr primary.local:9001 -dst /local/follower -name follower-1
```

The server itself is plain TCP — bind it to a private interface or
firewall the port. For exposed deployments, use pattern (3).

**(3) Remote with in-process SSH tunnel (recommended for production)**

`axwal-mirror` dials SSH itself; no external `ssh -L` process required.
The TCP socket between the two ax-wal processes never leaves localhost on
either side.

```bash
# Primary listens on 127.0.0.1 only — outsiders cannot reach the WAL
# without first authenticating to SSH on the same host.
axwal-serve -dir /var/wal -listen 127.0.0.1:9001
```

```bash
# Follower
axwal-mirror \
  -tunnel root@primary.example.com:22 \
  -ssh-agent                              \  # or -ssh-key ~/.ssh/id_ed25519
  -ssh-known-hosts ~/.ssh/known_hosts     \  # strict host-key verification
  -addr 127.0.0.1:9001                    \  # as seen from inside the SSH host
  -dst /local/follower                    \
  -name follower-1
```

Library equivalent:

```go
axwal.Mirror(ctx, axwal.MirrorOptions{
    SrcRemote: &axwal.RemoteReaderOptions{
        Addr: "127.0.0.1:9001",
        Name: "follower-1",
        SSHTunnel: &axwal.SSHTunnelConfig{
            Host:           "primary.example.com:22",
            User:           "root",
            KeyFile:        "/home/follower/.ssh/id_ed25519",
            KnownHostsFile: "/home/follower/.ssh/known_hosts",
        },
    },
    DstDir:    "/local/follower",
    BatchSize: 256,
})
```

Why this design:

- **No PKI needed.** SSH keys + known_hosts are already managed on every
  host with sshd. No certificate authority, no rotation tooling.
- **Confidentiality + integrity + authentication**, all from SSH.
- **Operational visibility**: connections show up in `sshd` audit logs.
- **Fits into existing firewalls**: only port 22 needs to be open; the
  WAL port stays on localhost.

Wire protocol:

```
HELLO  (client → server)   {name, start_index, batch_size} as JSON
OK     (server → client)   8B head_index
ERR    (server → client)   {error} as JSON
RECBATCH  (server → client) packed [index|len|payload] tuples
PING   (server → client)   keepalive on idle
ACK    (client → server)   8B last_received_index, periodic
```

No protobuf, no schema compiler — readable in a hex dump. See `wire.go`.

Indices align iff dst started fresh. For HA you can encrypt+mirror across
trust zones via `-src-key-file` / `-dst-key-file`. The follower can be
queried as a normal local WAL — readers register cursors as usual; the
mirror process is the only writer.

## Benchmarks

`cmd/axwal-bench` flag matrix. 4 concurrent writer goroutines, 1 KiB
payloads, 5 s runs. macOS = M1 Max NVMe; Linux = Hetzner 16-core NVMe RAID.

### Writer throughput (4 writers, 1 KiB)

| OS | SyncMode | GroupCommit | writes/s | wp50 | wp99 |
|---|---|---|---|---|---|
| macOS | Full | off | 157 | 24 ms | 46 ms |
| macOS | Full | on (1 ms) | 511 | 6.4 ms | 21 ms |
| macOS | **Barrier** | off | **2,393** | 1.2 ms | 17 ms |
| macOS | None | off | 13,364 | 7 µs | 7 ms |
| Linux | Full | off | 279 | 13 ms | 27 ms |
| Linux | **Full** | **on (1 ms)** | **820** | 4.5 ms | 10 ms |
| Linux | None | off | **280,741** | 3 µs | 339 µs |

### Reader throughput (replay 10 K × 1 KiB, single reader)

| OS | ack-every | AckBatchInterval | rate | total |
|---|---|---|---|---|
| macOS | 1 | 0 (strict) | 70/s | 2 m 23 s |
| macOS | 1 | **100 ms** | **1,334,697/s** | 7 ms |
| Linux | 1 | 0 (strict) | 58/s | 2 m 50 s |
| Linux | 1 | **10 ms** | **1,846,663/s** | 5.4 ms |

### Combined high-throughput, low-durability (4 writers, 1 KiB, 5 s)

Writer `SyncNone` + Reader `AckBatchInterval=10ms`, both batched:

| | writes/s | MB/s | read p50 | read p99 |
|---|---|---|---|---|
| macOS | 15,327 | 14.9 | 32 µs | 5.7 ms |
| Linux | **192,593** | 188 | 5 ms | 31 ms |

### 100K-record cold replay (single reader, AckBatchInterval=0, ack-every=100)

| size | macOS rate | macOS p99 | Linux rate | Linux p99 |
|---|---|---|---|---|
| 100 B | 7,476/s | 6 µs | 5,813/s | 4 µs |
| 1 KB | 7,275/s | 27 µs | 5,948/s | 16 µs |
| 10 KB | 6,985/s | 78 µs | 5,604/s | 34 µs |
| 100 KB | 5,764/s | 397 µs | 4,851/s | 132 µs |
| 1 MB | 2,036/s | 1.25 ms | 2,256/s | 833 µs |

Hit `~6 K rec/s` with `ack-every=100` is the cursor-fsync ceiling. Switch
to `AckBatchInterval` for the millions/sec regime.

## Choosing a configuration

| Use case | SyncMode | GroupCommit | AckBatchInterval | Encryption | Why |
|---|---|---|---|---|---|
| Financial transactions, audit log | `Full` | on, MaxLatency=2-5ms | 0 (per-Ack fsync) | optional | survive hardware crash; consumer must not re-process |
| General durable queue, K8s pod | `Full` | on, 1-2 ms | 10 ms | optional | strong durability on writer; reader replay window of 10 ms is tolerable for at-least-once consumers |
| Replicated to S3, local is cache | `Barrier` | off | 50–100 ms | none | data is uploaded externally so power loss costs at most a few seconds; reader is downstream of a queue, not a database |
| Metrics / observability events | `None` | off | 100 ms | none | lossy is fine; throughput dominates |
| HA primary + warm standby | `Full` + `OnRotate` | on | 10 ms | optional | mirror via axwal-mirror; crash on primary → promote follower |
| Compliance buffer (purge after 30d) | `Full` | on | 10 ms | yes | `MaxRetainAge=30d`, `RequireArchivedMark=true`, monitored consumer SLO |

### Anti-patterns

- **`SyncNone` + `GroupCommit.Enabled`** — group commit's timer becomes the
  bottleneck and you give up most of the throughput.
- **`SyncFull` + `AckBatchInterval=0` + lots of Acks** — reader caps at
  ~70 rec/s while writer is wasting throughput it can't deliver.
- **`HeartbeatInterval > StaleReaderTTL / 2`** — janitor will declare your
  reader dead between heartbeats and start GC'ing past its position.
- **`MaxSegmentAge < GroupCommit.MaxLatency`** — pointless: rotation will
  happen on every commit window.
- **`MaxRetainAge` without a consumer SLO** — old segments will be purged
  even if a consumer hasn't drained them. Lagging readers will be
  fast-forwarded with a warning.

## Tools

```bash
# Demo producer / consumer (single binaries)
go run ./cmd/producer -dir /tmp/wal -n 1000
go run ./cmd/consumer -dir /tmp/wal -name worker-1

# Bench / load test
go run ./cmd/axwal-bench -duration 10s -writers 4 -size 1024 \
    -sync-mode barrier -ack-batch 10ms
go run ./cmd/axwal-bench -suite -duration 5s -writers 4   # 100B..1MB
go run ./cmd/axwal-bench -dir /tmp/q -size 1024 -replay-count 100000

# Replication — local (both dirs accessible)
go run ./cmd/axwal-mirror -src /shared/leader -dst /local/follower \
    -name follower-1 -batch 256

# Replication — over the network (primary)
go run ./cmd/axwal-serve -dir /var/wal -listen 127.0.0.1:9001

# Replication — over the network (follower) via SSH tunnel
go run ./cmd/axwal-mirror -tunnel root@primary:22 -ssh-agent \
    -ssh-known-hosts ~/.ssh/known_hosts \
    -addr 127.0.0.1:9001 -dst /local/follower -name follower-1

# Admin
go run ./cmd/axwal-cli ls -dir /tmp/wal
go run ./cmd/axwal-cli cursors -dir /tmp/wal
go run ./cmd/axwal-cli stats -dir /tmp/wal
go run ./cmd/axwal-cli verify -dir /tmp/wal
go run ./cmd/axwal-cli fsck -dir /tmp/wal --fix
go run ./cmd/axwal-cli tail -dir /tmp/wal -name dbg [-key-file key.bin]
go run ./cmd/axwal-cli dump -dir /tmp/wal -from 1 -to 100 > dump.json
go run ./cmd/axwal-cli restore -dir /tmp/wal2 -src dump.json
go run ./cmd/axwal-cli compact -dir /tmp/wal -range 1-1000
go run ./cmd/axwal-cli mark-archived -dir /tmp/wal -range 1-1000
```

## On-disk layout

```
/wal-dir/
  .writer.lock              flock + PID of the live writer (POSIX advisory lock)
  wal-1-1000                closed segment, indices 1..1000 inclusive
  wal-1001-2000.zst         closed + zstd-compressed
  wal-1001-2000.archived    sidecar dropped by an external uploader (optional)
  wal-1001-2000.meta        rotation metadata (sha256, num_records, …) optional
  wal-2001-                 active segment (writer is appending)
  cursors/
    billing-worker-1.cursor   [magic][crc32c][JSON cursor record]
    billing-worker-1.lock     flock on the cursor name
```

Record framing inside a segment file:

```
+--------+--------+----------------+
|  len   |  crc32c (Castagnoli)    |  payload (or AES-256-GCM body)
| 4B BE  |        4B BE            |  len bytes
+--------+--------+----------------+
```

A torn write (crash mid-Append) leaves a partial frame at the file tail.
Recovery on `OpenWriter` re-walks the open segment with CRC validation,
truncates everything from the first corrupt/short frame onwards, and
renames the file to its proper closed form.

## Stats / metrics

```go
ws := w.Stats()  // LastIndex, CommitIndex, FsyncCalls, FsyncErrors,
                 // Rotations, AppendCalls, AppendBytes,
                 // BackpressureRejects, RetentionDeletes, …
rs := r.Stats()  // NextIndex, AckedIndex, NextCalls, AckCalls,
                 // SegmentSwitches, ErrCorruptSegs, WaitWakeups
```

Expose via your Prometheus registry / OTLP exporter; counters are atomic
and cheap to scrape every second.

Logs go through `*slog.Logger` (configurable via `WriterOptions.Logger` /
`ReaderOptions.Logger`). Every line carries `dir` and (for readers) `name`
as keys.

## Limits

- 256 MiB hard cap per individual record (`maxRecordSize` in `framing.go`).
- A segment can grow up to `int64` bytes; in practice constrained by your
  rotation policy.
- The single-writer `flock` is *advisory*. Two processes that ignore the
  lock (e.g., one of them on a network filesystem with broken locking) can
  corrupt the WAL. On a local Linux/macOS filesystem the lock is honored.
- Reader is single-goroutine (`Next` is not safe for concurrent use).
  Spawn multiple Readers with distinct cursor names instead.
- Mirror preserves record ORDER, not necessarily INDICES across cycles —
  align by starting dst from a fresh empty directory.

## Tested on

- macOS (Apple M1 Max, Darwin 25.4, Go 1.23)
- Linux (Ubuntu 22.04, x86_64 16-core NVMe RAID, Go 1.23)
- CI: GitHub Actions matrix `linux/macos × go 1.23` running `go vet` +
  `go test -race` + a smoke benchmark.

## License

(unset)
