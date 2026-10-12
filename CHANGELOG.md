# Changelog

## v0.4.0 — 2026-10-12

Encrypted archives that can live in a public bucket. Guide (Russian):
[docs/ARCHIVE_ENCRYPTION.md](docs/ARCHIVE_ENCRYPTION.md).

### Added

- **`ArchiveConfig.Encryption`** (`ArchiveEncryption{Key}`): uploaded
  segments and the manifest are sealed with AES-256-GCM in 64 KiB chunks
  (streamable; tampering, truncation and chunk reordering are detected),
  and object names become `HMAC-SHA256(key, first index)`, so nothing in
  the bucket reveals contents, index ranges, record counts or order.
  Encryption is deterministic per file, which keeps upload retries
  idempotent. `Layout` is ignored in this mode.
- **`ArchiveReaderOptions.ArchiveEncryption`**: reads such an archive as a
  stream — no temp files — using only `Get`. Works from the encrypted
  manifest, or, if there is none, by walking the chain of derived object
  names from index 1.
- **`store/httpstore`**: read-only `ArchiveStore` over plain HTTP(S) GET for
  public buckets; `storeurl` accepts `https://…` URLs.
- `LoadEncryptedArchiveManifest`, `ArchiveObjectName`,
  `ArchiveManifestName`, `NewArchiveDecrypter`.
- CLI: `-archive-key-file` on `axwal-cli dump / restore / archive` and
  `axwal-mirror -store`.

## v0.3.4 — 2026-10-12

Reader configuration guide (Russian): [docs/READER.md](docs/READER.md).

Reader robustness. A new scenario suite (`reader_scenarios_test.go`) walks a
reader through deleted, truncated, archived, compressed and damaged segments;
the previous reader stalled or mislabeled records in 9 of its 14 scenarios.

### Fixed

- **Stall on a hole.** When the records a running reader needed next were
  gone (`TruncatePrefix`, `MaxRetainAge`, a segment removed by hand, a log
  that starts later than the reader's position), `Next` waited forever.
  It now jumps to the first index that exists once the hole has persisted
  for 1 s, logs the missing range and counts it in
  `ReaderStats.SkippedRecords`.
- **Spurious "no such file" errors.** A segment compressed, rotated or
  deleted between directory listing and open made `Next` fail; the reader
  now looks again.
- **Mislabeled records after a CRC error.** After `ErrCorruptRecord` in a
  closed segment the next call returned the following record under the
  failed record's index. A truncated closed segment produced one error and
  then a silent stall. Both are now a sticky error, or — with the new
  `ReaderOptions.SkipCorruptSegments` — a logged, counted skip to the next
  segment. A closed segment holding fewer records than its name says is
  detected the same way (it used to stall).
- **Torn tail after a writer crash.** A complete frame with a bad CRC at the
  tail of the open segment made the reader fail and lose its place; it now
  waits for the writer's recovery, like it already did for a partial frame.
  If the writer keeps appending past the bad frame, the reader reports
  `ErrCorruptRecord`.
- **Writer behind the reader** (tail lost in a crash without fsync): `Next`
  failed with "skipping to index …: EOF" on every call; it now waits for
  the log to reach the reader's position.
- `NextBatch` no longer leaves the reader mid-frame after an error in its
  non-blocking part.
- `ReaderStats.SegmentSwitches`, `WaitWakeups` and `ErrCorruptSegs` were
  never incremented.

### Added

- `ReaderOptions.SkipCorruptSegments`, `ReaderStats.SkippedRecords`.

## v0.3.3 — 2026-10-11

### Fixed

- `Reader` on the writer's tail: after reading a record that was only partly
  written yet, it reopened the segment and walked every record from the
  start to get back to its position — on each such hit. Records larger than
  a few KB make that the common case (they reach the file in several
  writes), and a reader that keeps up with the writer, like the server side
  of a synchronous replica, paid it constantly. It now resumes at the saved
  byte offset, also across a rotation. Synchronous replication, `SyncFull` +
  `GroupCommit`, 64 writers: 2 KB records 6 578 → 35 735 rec/s, 8 KB
  2 870 → 24 990, 32 KB 1 921 → 9 806.

## v0.3.2 — 2026-10-11

### Changed

- **Group commit no longer waits for a timer.** The commit goroutine starts
  an fsync as soon as records are pending and runs it without holding the
  writer lock, so appends keep buffering meanwhile and ride on the next
  fsync. Batches size themselves to the disk. Before, every commit waited
  up to `MaxLatency` (1 ms) first, which on a fast disk (fsync ≈ 0.1 ms)
  made `GroupCommit` slower than plain appends. Measured on a server with a
  RAID controller cache, `SyncFull`, 512-byte records: 1 writer 722 → 7 909
  rec/s, 8 writers 5 087 → 27 102, 64 writers 45 085 → 96 670.
  `GroupCommitConfig.MaxLatency` is now only the period of a fallback timer;
  `MaxBatchBytes` is unused.
- `Reader`: when caught up with the writer, a wakeup no longer lists the
  directory (one `stat` instead).

## v0.3.1 — 2026-10-11

### Fixed

- SSH tunnel: the client now asks the server for a host key type that is
  present in `known_hosts`. Before, it could negotiate e.g. ECDSA while the
  file listed only the host's ED25519 key and fail with
  `knownhosts: key mismatch` on a host that plain `ssh` accepts. Found by
  running a replica against a real sshd.

## v0.3.0 — 2026-10-11

Replication: read-only replica over SSH with automatic reconnect, optional
synchronous mode. Operator guide (Russian): [docs/REPLICATION.md](docs/REPLICATION.md).

### Added

- **Read-only replica** — `RunReplica` / `axwal-mirror -replica`: an exact
  copy of a remote WAL (same record indices) that resumes from its own last
  index, reconnects with backoff, and stops with `ErrReplicaDiverged` rather
  than leave a hole. The directory is marked read-only: `OpenWriter` returns
  `ErrReplicaDir` until `PromoteReplica` / `axwal-cli promote`.
- **Synchronous replication** — `WriterOptions.SyncReplication`
  (`Timeout`, `RecoverLag`, `Cooldown`) with `ServerOptions.Writer`: appends
  wait for a replica's on-disk acknowledgement; on timeout or replica
  disconnect the writer falls back to asynchronous mode and returns to
  synchronous by itself once the replica has caught up. Stats:
  `SyncReplicationActive`, `SyncReplicationFallbacks`,
  `SyncReplicationWaits`, `ReplicasConnected`, `ReplicaAckedIndex`.
- `RemoteReaderOptions.IdleTimeout` (default 60 s): a connection on which
  the server has gone silent is dropped, so followers notice a link that
  died without a FIN/RST.
- Wire protocol: HELLO gained `exact` (server resets the follower's cursor
  to the requested start index). Older servers ignore it; a replica then
  still verifies indices and refuses to continue on a mismatch.

### Fixed

- `Serve`: a follower reconnecting under the same name was rejected
  ("cursor is held by another reader") for as long as its dead previous
  connection lingered; the new connection now evicts the old one.
- `RemoteReader`: the connect / handshake phase is bounded by `DialTimeout`
  end to end (SSH key exchange, tunnel dial, HELLO/OK) instead of only the
  TCP connect.
- SSH tunnel: the ssh-agent connection was leaked on every dial.

### Changed behavior

- `RemoteReader.Next` now returns an error (wrapping
  `io.ErrUnexpectedEOF`) when the server closes the connection; it used to
  return a bare `io.EOF`.

## v0.2.0 — 2026-10-11

Built-in archiving, disk protection, non-blocking append, calendar rotation,
key rotation. No on-disk format change: v0.1.0 directories open as is.

### Added

- **Built-in archiver** — `WriterOptions.Archive` (`ArchiveConfig`: `Store`,
  `Prefix`, `Layout`, `Concurrency`, `Retry`, `AttemptTimeout`,
  `ScanInterval`, `DrainOnClose`, `DisableManifest`).
  - Uploads every closed segment after compression; writes `.archived` only
    once the store reports the object with the expected size (and SHA-1,
    where the backend exposes one).
  - Retries forever with exponential backoff; every step is idempotent, so a
    retry neither loses a segment nor re-sends one that already arrived.
  - On open, picks up every closed segment without `.archived`, including one
    finalized by crash recovery.
  - Streams from the file; above 128 MiB uses B2 large-file / S3 multipart
    with parts read directly from the file.
- **Stores** (no SDK dependencies): `store/b2store` (Backblaze B2 native
  API), `store/s3store` (S3 and compatibles, SigV4), `store/fsstore` (local
  directory), `store/storeurl` (`b2://`, `s3://`, `file://` URLs with
  credentials from the environment). Custom backends implement
  `ArchiveStore` (`Put`, `Get`, `List`).
- **Archive manifest** — `<prefix>/manifest.json` with index ranges, record
  counts, sizes, SHA-1 / SHA-256, compression, first/last record time and
  key fingerprint per segment. `LoadArchiveManifest` reads it, or rebuilds a
  catalog from a listing when there is none.
- **Archive reader** — `OpenArchiveReader` reads records straight from a
  store by index range or by time range, with optional `Follow`.
- **Mirror from an archive** — `MirrorOptions.SrcArchive`,
  `axwal-mirror -store URL`; position kept in `<dst>/archive-mirror.pos`.
- **Retention**: `MinRetainAge` (keep locally at least this long),
  `MaxTotalBytes` / `MinFreeBytes` (evict oldest archived segments under
  disk pressure), `OnDiskFull` policy (`DiskFullReject` → `ErrDiskFull`,
  `DiskFullDrop`, `DiskFullIgnore`).
- **Calendar rotation** — `RotateAligned` + `RotateLocation` (every hour at
  :00, daily at 00:00). A segment only holds records of one window.
- **Non-blocking append** — `Writer.AppendAsync` with `AsyncQueueSize`,
  `AsyncQueueBytes`, `AsyncBatchSize`; overflow returns `ErrQueueFull` and is
  counted in `Stats().Dropped`.
- **`RotateOnClose`** — `Close()` runs the full rotation pipeline
  (compression, `.meta`, `OnRotate`) on the last segment.
- **Key rotation** — `Writer.RotateKey`, `EncryptionConfig.OldKeys` for
  readers, `KeyFingerprint`.
- **Segment metadata** — `.meta` gained `first_record_unix`,
  `last_record_unix`, `key_fingerprint`.
- **Stats** — `Dropped`, `AsyncErrors`, `AsyncQueued`, `DiskFull`,
  `DiskFullRejects`, `ArchivePending`, `ArchivePendingBytes`,
  `ArchiveOldestPendingAge`, `ArchiveLastSuccess`, `ArchiveUploads`,
  `ArchiveErrors`.
- **axwal-cli** — `archive -store URL [-json]`; `dump -raw`, `-jsonl`,
  `-follow`, `-store URL -since/-until`, `-old-key-files`;
  `restore -store URL`.
- Exported errors: `ErrWriterClosed`, `ErrDiskFull`, `ErrQueueFull`,
  `ErrAsyncDisabled`, `ErrObjectNotFound`.

### Fixed

- `CompressOnRotate` + `WriteMetaSidecar`: the sidecar was hashed after
  compression had already removed the uncompressed segment
  (`open …/wal-1-6 for meta: no such file or directory`).
- `axwal-cli dump -from 1 -to 100` no longer hangs when fewer records exist;
  it stops at the last record present (`-follow` restores tailing).
- `Reader.HeadIndex()` / `Lag()` ignored the open segment whenever a closed
  one existed.
- Compression now writes to `wal-F-L.zst.tmp` and renames, so a half-written
  `.zst` is never visible; recovery removes leftovers and the uncompressed
  twin of a completed `.zst`.

### Changed behavior

- `StartLatest` now skips records in the open segment too (consequence of
  the `HeadIndex` fix; matches its documentation).
- `Archive` implies `WriteMetaSidecar`, `RotateOnClose` and
  `Retention.RequireArchivedMark`.
- With `RotateOnClose` and no `CloseTimeout`, `Close()` gives the pipeline of
  the last segment up to 30 s.
- With retention enabled, the janitor also runs after each rotation and
  upload, not only on its interval.

### Known limitations

- `b2store` and `s3store` are tested against in-process fake servers (S3
  signing also against the AWS SigV4 reference examples), not against live
  buckets.
- Time filters on an archive select whole segments: records carry no
  timestamps.
- A segment finalized by crash recovery has no recorded record times or key
  fingerprint; the manifest falls back to the file's modification time.

## v0.1.0

Initial release.
