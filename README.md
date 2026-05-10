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
  uses fsnotify with a polling fallback.
- **Auto-rotation.** By size, by age, by record count — any combination.
- **Cursor-aware retention.** Old segments are deleted only when no live
  reader still needs them. Stale readers (heartbeat older than TTL) are
  excluded from the calculation.
- **Archive gate.** Optionally require an external uploader (S3 / B2 / cold
  storage) to drop a `wal-F-L.archived` sidecar before retention reclaims
  the local copy.

## What it isn't

- Not a network-attached message broker. There's no replication, no
  consumer groups, no protocol — you give it a filesystem and a process,
  and it gives you a durable log.
- Not multi-writer. The `flock` on the directory is by design.
- Not Windows. The lock and per-OS sync helpers are POSIX (Linux + macOS
  tested).

## Quick start

```go
import axwal "github.com/axgrid/ax-wal"

// Producer
w, _ := axwal.OpenWriter(axwal.WriterOptions{
    Dir:            "/var/wal/orders",
    MaxSegmentSize: 64 << 20, // 64 MiB
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
    _ = r.Ack(idx) // durably advance the cursor
}
```

## Configuration

### Writer

```go
type WriterOptions struct {
    Dir               string
    MaxSegmentSize    int64           // bytes, default 64 MiB
    MaxSegmentAge     time.Duration   // 0 = disabled
    MaxSegmentRecords uint64          // 0 = disabled
    CompressOnRotate  bool            // async zstd
    SyncMode          SyncMode        // see below
    BufferSize        int             // bufio.Writer size, default 64 KiB
    GroupCommit       GroupCommitConfig
    Retention         RetentionConfig
}
```

### Reader

```go
type ReaderOptions struct {
    Dir               string
    Name              string          // unique per active reader
    StartFrom         StartPosition   // Earliest | Latest | At
    StartAtIndex      uint64
    HeartbeatInterval time.Duration   // default 5s; should be < StaleReaderTTL/2
    AckBatchInterval  time.Duration   // 0 = strict per-Ack fsync; >0 = batched
    PollInterval      time.Duration   // fsnotify fallback, default 100ms
}
```

### SyncMode — the durability dial

| Mode | Behavior | macOS cost | Linux cost | Loss on hard crash |
|---|---|---|---|---|
| `SyncFull` (default) | `*os.File.Sync()` | F_FULLFSYNC, ~5–15 ms | fsync, ~3–4 ms | nothing acked is lost |
| `SyncBarrier` | order-only barrier | F_BARRIERFSYNC, ~0.4 ms | fdatasync, ~3 ms | last few hundred ms |
| `SyncNone` | no flush | none | none | last few seconds |

Hard truth: on Linux, `Barrier` and `Full` perform almost identically because
`fdatasync` still has to commit file-size metadata. The big win for `Barrier`
is on macOS, where `F_FULLFSYNC` is famously slow.

`Writer.Sync()` always does a hard fsync, regardless of `SyncMode`. Use it as
an explicit checkpoint when you've opted into a weaker mode but need a
guarantee right now (e.g., before reporting "transaction committed" upstream).

### GroupCommit — amortizing fsync cost

```go
type GroupCommitConfig struct {
    Enabled       bool
    MaxLatency    time.Duration  // default 1ms
    MaxBatchBytes int            // default 1 MiB
}
```

When enabled, concurrent `Append`/`AppendBatch` callers share a single fsync.
Each call **still blocks** until its records are durable — semantics are
unchanged, only throughput improves. Higher `MaxLatency` = more records per
fsync = higher throughput, at the cost of latency.

| MaxLatency | Effective fsyncs/s on disk | Tail latency |
|---|---|---|
| 1 ms | up to 1000 | + ~1 ms p99 |
| 5 ms | up to 200 | + ~5 ms p99 |
| 50 ms | up to 20 | + ~50 ms p99 |

Group-commit is **counter-productive when you've also chosen SyncBarrier or
SyncNone** — the per-fsync cost is already cheap enough that the 1ms timer
becomes the dominant latency. See benchmark below.

### AckBatchInterval — the reader's group commit

Per-record `Ack()` does an fsync of the cursor file. On macOS that caps a
single reader at ~70 records/s; on Linux ~60. Setting `AckBatchInterval > 0`
makes `Ack()` update an atomic counter and lets a background goroutine fsync
the cursor at that interval.

| AckBatchInterval | Ack call cost | Records re-delivered after hard kill |
|---|---|---|
| 0 (strict) | 1 fsync | 0 (after the last successful Ack) |
| 10 ms | atomic update | up to 10ms × throughput |
| 100 ms | atomic update | up to 100ms × throughput |

A clean `Close()` always flushes the latest Ack first.

### Retention

```go
type RetentionConfig struct {
    Enabled              bool
    Interval             time.Duration  // default 30s
    MinRetainRecords     uint64         // floor — keep at least this many recent records
    StaleReaderTTL       time.Duration  // default 60s
    RequireArchivedMark  bool           // gate on wal-F-L.archived sidecar
}
```

A closed segment is deleted iff:

1. **All non-stale cursor heartbeats** have advanced past it.
2. **`MinRetainRecords` floor** is satisfied (keep last N records always).
3. **If `RequireArchivedMark`,** a sibling file named `wal-F-L.archived`
   exists. Use `Writer.MarkArchived(F, L)` from your in-process archiver,
   or `axwal-cli mark-archived` from a separate uploader.

The active open segment is never touched.

## Benchmarks

All numbers below come from `cmd/axwal-bench`. Single payload size = 1 KiB,
4 concurrent writer goroutines, 5 second runs unless noted. Reader uses
`-ack-every 100` so the writer is the bottleneck (see the reader matrix
below for what changes when the reader is unconstrained).

### Hardware

| | macOS | Linux |
|---|---|---|
| CPU | Apple M1 Max, 10 cores | 16-core x86_64 (Hetzner) |
| Disk | Apple internal NVMe | NVMe RAID (md2) |
| OS | Darwin 25.4 | Ubuntu 22.04, kernel 5.15 |
| Go | 1.23.12 | 1.23.4 |

### Writer throughput vs. SyncMode × GroupCommit (4 writers, 1 KiB)

**macOS:**

| SyncMode | GroupCommit | writes/s | wp50 | wp95 | wp99 |
|---|---|---|---|---|---|
| Full | off | 157 | 24 ms | 37 ms | 46 ms |
| Full | on (1ms) | 511 | 6.4 ms | 16 ms | 21 ms |
| Barrier | off | **2,393** | 1.2 ms | 5.9 ms | 17 ms |
| Barrier | on (1ms) | 2,256 | 1.0 ms | 6.8 ms | 9.7 ms |
| None | off | **13,364** | 7 µs | 0.7 ms | 7 ms |
| None | on (1ms) | 2,576 | 1.0 ms | 5.6 ms | 9.8 ms |

**Linux:**

| SyncMode | GroupCommit | writes/s | wp50 | wp95 | wp99 |
|---|---|---|---|---|---|
| Full | off | 279 | 13 ms | 20 ms | 27 ms |
| Full | on (1ms) | 820 | 4.5 ms | 7.2 ms | 10 ms |
| Barrier | off | 280 | 13 ms | 19 ms | 24 ms |
| Barrier | on (1ms) | 820 | 4.5 ms | 7.2 ms | 10 ms |
| None | off | **280,741** | 3 µs | 10 µs | 339 µs |
| None | on (1ms) | 3,781 | 1.1 ms | 1.1 ms | 1.2 ms |

Two takeaways:

- On Linux, `Barrier` and `Full` are within noise. The asymmetry exists on
  macOS because `F_FULLFSYNC` is unique to that platform.
- **Group-commit pessimizes the cheap modes.** With `SyncNone`, every Append
  is a memcpy — adding a 1 ms commit window halves macOS throughput and
  destroys Linux's by 70×. Use `GroupCommit.Enabled = true` only when you
  also want `SyncFull`.

### Reader throughput vs. AckBatchInterval (replay 10K × 1 KiB, single reader)

**macOS:**

| ack-every | AckBatchInterval | rate | Next() p99 | total |
|---|---|---|---|---|
| 1 | 0 | 70/s | 108 µs | 2 m 23 s |
| 100 | 0 | 6,417/s | 25 µs | 1.56 s |
| 10000 | 0 | 485,874/s | 12 µs | 21 ms |
| 1 | 10 ms | 663,450/s | 9 µs | 15 ms |
| 1 | 100 ms | **1,334,697/s** | 9 µs | 7 ms |

**Linux:**

| ack-every | AckBatchInterval | rate | Next() p99 | total |
|---|---|---|---|---|
| 1 | 0 | 58/s | 24 µs | 2 m 50 s |
| 100 | 0 | 5,904/s | 16 µs | 1.69 s |
| 10000 | 0 | 462,376/s | 12 µs | 22 ms |
| 1 | 10 ms | **1,846,663/s** | 11 µs | 5.4 ms |
| 1 | 100 ms | 1,730,532/s | 12 µs | 5.8 ms |

`AckBatchInterval` is essentially free in API terms — you keep calling
`Ack()` on every record like a strict at-least-once consumer should — but
turns reader throughput from ~70 rec/s into ~1.7 M rec/s.

### Combined: SyncNone writer + AckBatchInterval reader (4 writers, 1 KiB, 5s)

This is the "high throughput, replay-on-crash-allowed" sweet spot.

| | writes/s | MB/s | read p50 | read p99 |
|---|---|---|---|---|
| macOS | 15,327 | 14.9 | 32 µs | 5.7 ms |
| Linux | **192,593** | 188 | 5 ms | 31 ms |

### 100K-record cold replay (single reader, ack-every=100, AckBatch off)

How fast a brand-new reader catches up after a backlog of 100K records.

**macOS:**

| size | rate | MB/s | Next() p99 | total |
|---|---|---|---|---|
| 100 B | 7,476/s | 0.7 | 6 µs | 13 s |
| 1 KB | 7,275/s | 7.1 | 27 µs | 14 s |
| 10 KB | 6,985/s | 68 | 78 µs | 14 s |
| 100 KB | 5,764/s | 563 | 397 µs | 17 s |
| 1 MB | 2,036/s | 2,036 | 1.25 ms | 49 s |

**Linux:**

| size | rate | MB/s | Next() p99 | total |
|---|---|---|---|---|
| 100 B | 5,813/s | 0.5 | 4 µs | 17 s |
| 1 KB | 5,948/s | 5.8 | 16 µs | 17 s |
| 10 KB | 5,604/s | 55 | 34 µs | 18 s |
| 100 KB | 4,851/s | 474 | 132 µs | 21 s |
| 1 MB | 2,256/s | 2,256 | 833 µs | 4.4 s (10K records) |

The plateau at ~6 K rec/s is the cursor-fsync ceiling at `ack-every=100`.
With `AckBatchInterval = 100ms` it disappears — see the matrix above.

## Choosing a configuration

| Use case | SyncMode | GroupCommit | AckBatchInterval | Why |
|---|---|---|---|---|
| Financial transactions, audit log | `Full` | on, MaxLatency=2-5ms | 0 (per-Ack fsync) | survive hardware crash; consumer must not re-process; hot writers benefit from group commit |
| General durable queue, K8s pod | `Full` | on, MaxLatency=1-2ms | 10 ms | strong durability on writer; reader replay window of 10ms is tolerable for at-least-once consumers |
| Replicated to S3, local is cache | `Barrier` | off | 50–100 ms | data is uploaded externally so power loss costs at most a few seconds; reader is downstream of a queue, not a database |
| Metrics / observability events | `None` | off | 100 ms | lossy is fine; throughput dominates; reader is downstream pipeline that re-aggregates anyway |
| Single-process in-memory bus persisted to disk | `None` | off | 100 ms | restart implies full reload from external truth source |

### Anti-patterns

- **`SyncNone` + `GroupCommit.Enabled`** — group commit's timer becomes the
  bottleneck and you give up most of the throughput you came for.
- **`SyncFull` + `AckBatchInterval=0` + lots of Acks** — reader caps at
  ~70 rec/s while writer is wasting throughput it can't deliver.
- **`HeartbeatInterval > StaleReaderTTL / 2`** — janitor will declare your
  reader dead between heartbeats and start GC'ing past its position.
- **`MaxSegmentAge < MaxLatency`** — pointless: rotation will happen on every
  commit window.

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

# Admin
go run ./cmd/axwal-cli ls -dir /tmp/wal
go run ./cmd/axwal-cli cursors -dir /tmp/wal
go run ./cmd/axwal-cli verify -dir /tmp/wal
go run ./cmd/axwal-cli tail -dir /tmp/wal -name dbg
go run ./cmd/axwal-cli mark-archived -dir /tmp/wal -range 1-1000
```

## On-disk layout

```
/wal-dir/
  .writer.lock              flock + PID of the live writer (per OS, may not exist on Windows etc.)
  wal-1-1000                closed segment, indices 1..1000 inclusive
  wal-1001-2000.zst         closed + zstd-compressed
  wal-1001-2000.archived    sidecar dropped by an external uploader (optional)
  wal-2001-                 active segment (writer is appending)
  cursors/
    billing-worker-1.cursor   {name, index, pid, started_at, last_heartbeat}
    billing-worker-1.lock     flock on the cursor name
```

Record framing inside a segment file:

```
+--------+--------+----------------+
|  len   |  crc32c (Castagnoli)    |  payload
| 4B BE  |        4B BE            |  len bytes
+--------+--------+----------------+
```

A torn write (crash mid-Append) leaves a partial frame at the file tail.
Recovery on `OpenWriter` re-walks the open segment with CRC validation,
truncates everything from the first corrupt/short frame onwards, and
renames the file to its proper closed form.

## Limits

- 256 MiB hard cap per individual record (`maxRecordSize` in `framing.go`).
- A segment can grow up to `int64` bytes; in practice constrained by your
  rotation policy.
- The single-writer `flock` is *advisory*. Two processes that ignore the lock
  (e.g., one of them on a network filesystem with broken locking) can corrupt
  the WAL. On a local Linux/macOS filesystem the lock is honored by the kernel.

## License

(unset)
