# Changelog

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
