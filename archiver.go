package axwal

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// archiver uploads closed segments to an ArchiveStore. Its source of truth is
// the directory itself: "pending" means a closed segment without a .archived
// sidecar. That makes restarts, crash-recovered segments and failed attempts
// all the same case — whatever is still unmarked gets (re)tried.
type archiver struct {
	w   *Writer
	cfg ArchiveConfig
	log *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	kick   chan struct{}
	sem    chan struct{} // upload slots
	wg     sync.WaitGroup

	mu       sync.Mutex
	inflight map[segRange]struct{}
	fails    map[segRange]*archiveFail

	// manMu serializes manifest read-modify-write cycles.
	manMu    sync.Mutex
	manifest *ArchiveManifest // nil until loaded from the store

	pending      atomic.Int64
	pendingBytes atomic.Int64
	oldestUnixNs atomic.Int64 // ModTime of the oldest pending segment; 0 = none
	lastOKUnixNs atomic.Int64
	uploads      atomic.Uint64
	errors       atomic.Uint64
}

type archiveFail struct {
	attempts int
	next     time.Time
}

// pendingSeg is a closed segment waiting for upload.
type pendingSeg struct {
	seg  segName
	path string
	size int64
	mod  time.Time
}

func newArchiver(w *Writer, cfg ArchiveConfig) *archiver {
	ctx, cancel := context.WithCancel(context.Background())
	return &archiver{
		w:        w,
		cfg:      cfg,
		log:      w.log.With("component", "axwal.archiver"),
		ctx:      ctx,
		cancel:   cancel,
		kick:     make(chan struct{}, 1),
		sem:      make(chan struct{}, cfg.Concurrency),
		inflight: map[segRange]struct{}{},
		fails:    map[segRange]*archiveFail{},
	}
}

func (a *archiver) start() {
	// Encrypted upload temp files of a previous run that was killed mid-way.
	if leftovers, _ := filepath.Glob(filepath.Join(a.w.dir, archiveUploadTmpPrefix+"*.tmp")); len(leftovers) > 0 {
		for _, p := range leftovers {
			_ = os.Remove(p)
		}
	}
	a.wg.Add(1)
	go a.loop()
}

// stop aborts in-flight uploads and waits for the goroutines. Anything not
// uploaded stays pending for the next OpenWriter.
func (a *archiver) stop() {
	a.cancel()
	a.wg.Wait()
}

// wake asks the dispatcher to rescan soon.
func (a *archiver) wake() {
	select {
	case a.kick <- struct{}{}:
	default:
	}
}

func (a *archiver) loop() {
	defer a.wg.Done()
	t := time.NewTicker(a.cfg.ScanInterval)
	defer t.Stop()
	for {
		// The first scan picks up leftovers of a previous run.
		next := a.scan()
		var retry <-chan time.Time
		var timer *time.Timer
		if !next.IsZero() {
			timer = time.NewTimer(time.Until(next))
			retry = timer.C
		}
		select {
		case <-a.ctx.Done():
		case <-a.kick:
		case <-t.C:
		case <-retry:
		}
		if timer != nil {
			timer.Stop()
		}
		if a.ctx.Err() != nil {
			return
		}
	}
}

// pendingSegments lists closed, fully finalized segments without a .archived
// mark, oldest first. When both wal-F-L and wal-F-L.zst exist, the .zst wins.
func (a *archiver) pendingSegments() []pendingSeg {
	matches, err := filepath.Glob(filepath.Join(a.w.dir, "wal-*"))
	if err != nil {
		return nil
	}
	byRange := map[segRange]pendingSeg{}
	for _, p := range matches {
		s, err := parseSegName(filepath.Base(p))
		if err != nil || !s.Closed || s.LastIndex < s.FirstIndex {
			continue
		}
		if isArchived(a.w.dir, s) || a.w.isFinalizing(s.FirstIndex, s.LastIndex) {
			continue
		}
		fi, err := os.Stat(p)
		if err != nil {
			continue
		}
		k := segRange{s.FirstIndex, s.LastIndex}
		if prev, dup := byRange[k]; dup && prev.seg.Compression != "" {
			continue
		}
		byRange[k] = pendingSeg{seg: s, path: p, size: fi.Size(), mod: fi.ModTime()}
	}
	out := make([]pendingSeg, 0, len(byRange))
	for _, ps := range byRange {
		out = append(out, ps)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].seg.FirstIndex < out[j].seg.FirstIndex })
	return out
}

// scan refreshes the gauges and starts uploads for as many pending segments
// as there are free slots. Returns when the earliest backed-off segment
// becomes due (zero if none).
func (a *archiver) scan() (next time.Time) {
	segs := a.pendingSegments()

	var bytes int64
	var oldest time.Time
	for _, ps := range segs {
		bytes += ps.size
		if oldest.IsZero() || ps.mod.Before(oldest) {
			oldest = ps.mod
		}
	}
	a.pending.Store(int64(len(segs)))
	a.pendingBytes.Store(bytes)
	if oldest.IsZero() {
		a.oldestUnixNs.Store(0)
	} else {
		a.oldestUnixNs.Store(oldest.UnixNano())
	}

	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()

	// Forget failure state of segments that are gone (archived by hand,
	// truncated away).
	live := make(map[segRange]struct{}, len(segs))
	for _, ps := range segs {
		live[segRange{ps.seg.FirstIndex, ps.seg.LastIndex}] = struct{}{}
	}
	for k := range a.fails {
		if _, ok := live[k]; !ok {
			delete(a.fails, k)
		}
	}

	for _, ps := range segs {
		k := segRange{ps.seg.FirstIndex, ps.seg.LastIndex}
		if _, busy := a.inflight[k]; busy {
			continue
		}
		if f := a.fails[k]; f != nil && now.Before(f.next) {
			if next.IsZero() || f.next.Before(next) {
				next = f.next
			}
			continue
		}
		select {
		case a.sem <- struct{}{}:
		default:
			return next // all slots busy; a finishing worker wakes us
		}
		a.inflight[k] = struct{}{}
		a.wg.Add(1)
		go a.work(ps)
	}
	return next
}

func (a *archiver) work(ps pendingSeg) {
	defer a.wg.Done()
	k := segRange{ps.seg.FirstIndex, ps.seg.LastIndex}

	ctx, cancel := context.WithTimeout(a.ctx, a.cfg.AttemptTimeout)
	key, err := a.upload(ctx, ps)
	cancel()

	a.mu.Lock()
	delete(a.inflight, k)
	switch {
	case err == nil:
		delete(a.fails, k)
		a.uploads.Add(1)
		a.lastOKUnixNs.Store(time.Now().UnixNano())
		a.log.Info("segment archived", "name", ps.seg.Name, "key", key)
	case a.ctx.Err() != nil:
		// Shutting down: not a failure, the segment stays pending.
	default:
		f := a.fails[k]
		if f == nil {
			f = &archiveFail{}
			a.fails[k] = f
		}
		f.attempts++
		delay := a.cfg.Retry.delay(f.attempts)
		f.next = time.Now().Add(delay)
		a.errors.Add(1)
		a.log.Warn("segment upload failed; will retry",
			"name", ps.seg.Name, "attempt", f.attempts, "retry_in", delay, "err", err)
	}
	a.mu.Unlock()

	<-a.sem
	a.wake()
	if err == nil {
		a.w.kickJanitor() // the segment just became deletable
	}
}

// upload pushes one segment and marks it archived. Every step is idempotent,
// so a retry after a failure at any point neither loses the segment nor
// leaves a duplicate object.
func (a *archiver) upload(ctx context.Context, ps pendingSeg) (key string, err error) {
	seg, path := ps.seg, ps.path

	// A closed segment that never went through the rotation pipeline (the
	// writer was closed without RotateOnClose, or crashed mid-compression)
	// is still uncompressed. Compress it now, keeping its mtime so retention
	// age and the day-based layout don't shift.
	if a.w.opts.CompressOnRotate && seg.Compression == "" {
		zst := path + ".zst"
		if err := compressOnceZstd(path, zst); err != nil {
			return "", fmt.Errorf("compress: %w", err)
		}
		_ = os.Chtimes(zst, ps.mod, ps.mod)
		_ = os.Remove(path)
		path, seg.Name, seg.Compression = zst, seg.Name+".zst", "zst"
	}

	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", err
	}

	// One streaming pass for the checksums; the upload itself streams too.
	h1, h256 := sha1.New(), sha256.New()
	if _, err := io.Copy(io.MultiWriter(h1, h256), f); err != nil {
		return "", fmt.Errorf("hash: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	info := ArchivePutInfo{
		Size:   fi.Size(),
		SHA1:   hex.EncodeToString(h1.Sum(nil)),
		SHA256: hex.EncodeToString(h256.Sum(nil)),
	}

	// With archive encryption the object is the ciphertext: encrypt into a
	// temp file (streaming, nothing held in memory) and upload that. The
	// salt is derived from the plaintext hash, so a retry produces the very
	// same bytes and is recognized below as already uploaded.
	var body io.Reader = f
	if c := a.cfg.crypto; c != nil {
		tmpPath := filepath.Join(a.w.dir, fmt.Sprintf("%s%d-%d.tmp", archiveUploadTmpPrefix, seg.FirstIndex, seg.LastIndex))
		enc, encInfo, err := encryptToTemp(c, f, tmpPath, c.saltFor("segment", h256.Sum(nil)))
		if err != nil {
			return "", fmt.Errorf("encrypt: %w", err)
		}
		defer os.Remove(tmpPath)
		defer enc.Close()
		body, info = enc, encInfo
	}

	entry := ArchiveSegment{
		FirstIndex:      seg.FirstIndex,
		LastIndex:       seg.LastIndex,
		NumRecords:      seg.LastIndex - seg.FirstIndex + 1,
		SizeBytes:       info.Size,
		SHA1:            info.SHA1,
		SHA256:          info.SHA256,
		Compression:     seg.Compression,
		FirstRecordUnix: ps.mod.Unix(),
		LastRecordUnix:  ps.mod.Unix(),
	}
	if meta, _ := readMetaSidecar(a.w.dir, seg.FirstIndex, seg.LastIndex); meta != nil {
		if meta.Compression != seg.Compression {
			meta.Compression = seg.Compression
			_ = writeMetaSidecar(a.w.dir, *meta)
		}
		if meta.FirstRecordUnix > 0 {
			entry.FirstRecordUnix, entry.LastRecordUnix = meta.FirstRecordUnix, meta.LastRecordUnix
		}
		entry.KeyFingerprint = meta.KeyFingerprint
	}
	key = segmentObjectKey(a.cfg.Prefix, a.cfg.Layout, seg.Name, time.Unix(entry.FirstRecordUnix, 0))
	if c := a.cfg.crypto; c != nil {
		// The name says nothing about the content, and a reader can derive
		// it from the key and the index it is looking for.
		key = archiveKey(a.cfg.Prefix, c.segmentName(seg.FirstIndex))
		entry.Encryption = archiveEncAlgo
	}
	entry.Key = key

	// An earlier attempt may have uploaded the object and then failed on a
	// later step; don't send it again.
	existing, found, err := statObject(ctx, a.cfg.Store, key)
	if err != nil {
		return key, fmt.Errorf("stat %s: %w", key, err)
	}
	if !found || !objectMatches(existing, info) {
		if err := a.cfg.Store.Put(ctx, key, body, info); err != nil {
			return key, fmt.Errorf("put %s: %w", key, err)
		}
		// Trust nothing: the mark is only written once the store itself
		// reports the object with the expected size (and SHA-1, where the
		// backend exposes it).
		stored, found, err := statObject(ctx, a.cfg.Store, key)
		if err != nil {
			return key, fmt.Errorf("verify %s: %w", key, err)
		}
		if !found {
			return key, fmt.Errorf("verify %s: object not listed after upload", key)
		}
		if !objectMatches(stored, info) {
			return key, fmt.Errorf("verify %s: stored size=%d sha1=%q, want size=%d sha1=%q",
				key, stored.Size, stored.SHA1, info.Size, info.SHA1)
		}
	}

	if !a.cfg.DisableManifest {
		entry.UploadedAtUnix = time.Now().Unix()
		if err := a.recordInManifest(ctx, entry); err != nil {
			return key, fmt.Errorf("manifest: %w", err)
		}
	}

	if err := MarkArchived(a.w.dir, seg.FirstIndex, seg.LastIndex); err != nil {
		return key, fmt.Errorf("mark archived: %w", err)
	}
	return key, nil
}

// archiveUploadTmpPrefix names the temp files holding encrypted segments on
// their way to the store. Deliberately not "wal-…": nothing that scans for
// segments should ever look at them.
const archiveUploadTmpPrefix = ".upload-"

// encryptToTemp writes the encrypted form of src to path and returns the file
// positioned at its start, with the size and checksums of the ciphertext.
func encryptToTemp(c *archiveCrypto, src io.Reader, path string, salt []byte) (*os.File, ArchivePutInfo, error) {
	out, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return nil, ArchivePutInfo{}, err
	}
	h1, h256 := sha1.New(), sha256.New()
	cw := &countingWriter{w: io.MultiWriter(out, h1, h256)}
	err = c.encrypt(cw, src, salt)
	if err == nil {
		_, err = out.Seek(0, io.SeekStart)
	}
	if err != nil {
		out.Close()
		os.Remove(path)
		return nil, ArchivePutInfo{}, err
	}
	return out, ArchivePutInfo{
		Size:   cw.n,
		SHA1:   hex.EncodeToString(h1.Sum(nil)),
		SHA256: hex.EncodeToString(h256.Sum(nil)),
	}, nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func objectMatches(o ArchiveObject, info ArchivePutInfo) bool {
	if o.Size != info.Size {
		return false
	}
	return o.SHA1 == "" || strings.EqualFold(o.SHA1, info.SHA1)
}

func (a *archiver) recordInManifest(ctx context.Context, entry ArchiveSegment) error {
	a.manMu.Lock()
	defer a.manMu.Unlock()
	if a.manifest == nil {
		m, err := getManifest(ctx, a.cfg.Store, a.cfg.Prefix, a.cfg.crypto)
		switch {
		case err == nil:
		case errors.Is(err, ErrObjectNotFound):
			m = &ArchiveManifest{}
		default:
			// Don't guess: starting from an empty manifest here would wipe
			// the catalog of everything uploaded before.
			return fmt.Errorf("load: %w", err)
		}
		a.manifest = m
	}
	a.manifest.upsert(entry)
	return putManifest(ctx, a.cfg.Store, a.cfg.Prefix, a.manifest, a.cfg.crypto)
}

// drain waits until nothing is pending, for at most d (d <= 0: no limit).
// Segments in backoff get an immediate extra attempt.
func (a *archiver) drain(d time.Duration) {
	a.mu.Lock()
	a.fails = map[segRange]*archiveFail{}
	a.mu.Unlock()

	var deadline <-chan time.Time
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		deadline = t.C
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		a.w.finMu.Lock()
		finalizing := len(a.w.finalizing)
		a.w.finMu.Unlock()
		if finalizing == 0 && len(a.pendingSegments()) == 0 {
			return
		}
		a.wake()
		select {
		case <-deadline:
			a.log.Warn("close: archive drain timed out; remaining segments upload on next open",
				"pending", len(a.pendingSegments()))
			return
		case <-tick.C:
		}
	}
}

func (a *archiver) fillStats(s *WriterStats) {
	s.ArchivePending = int(a.pending.Load())
	s.ArchivePendingBytes = a.pendingBytes.Load()
	if ns := a.oldestUnixNs.Load(); ns != 0 {
		s.ArchiveOldestPendingAge = time.Since(time.Unix(0, ns))
	}
	if ns := a.lastOKUnixNs.Load(); ns != 0 {
		s.ArchiveLastSuccess = time.Unix(0, ns)
	}
	s.ArchiveUploads = a.uploads.Load()
	s.ArchiveErrors = a.errors.Load()
}
