package axwal

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// ErrArchiveGap is returned by an ArchiveReader when records it was asked for
// are not in the archive although later ones are: a segment is absent from
// the catalog, or listed there but its object is gone from the store. The
// error names the missing index range. With ArchiveReaderOptions.SkipMissing
// the reader jumps over such holes instead.
var ErrArchiveGap = errors.New("axwal: records are missing from the archive")

// ArchiveReaderOptions configures an ArchiveReader.
type ArchiveReaderOptions struct {
	// Store / Prefix locate the archive (same values as in ArchiveConfig).
	Store  ArchiveStore
	Prefix string

	// FromIndex / ToIndex bound the records returned, inclusive. Zero means
	// unbounded on that side.
	FromIndex uint64
	ToIndex   uint64

	// FromTime / ToTime select by date instead of (or on top of) indices.
	// Records carry no timestamps, so the filter works on whole segments:
	// every segment whose [first record, last record] time range overlaps
	// [FromTime, ToTime] is returned in full. With RotateAligned on the
	// writer the granularity is exactly one rotation window.
	FromTime time.Time
	ToTime   time.Time

	// Encryption must be set for an encrypted archive. Put the current key
	// in Key and earlier ones in OldKeys; the manifest's key_fingerprint
	// tells which key a segment needs (see KeyFingerprint).
	Encryption *EncryptionConfig

	// ArchiveEncryption must be set for an archive written with
	// ArchiveConfig.Encryption (same key). Such an archive is read as a
	// stream — objects are decrypted and decoded while they download,
	// nothing is written to disk — and only ArchiveStore.Get is used, so a
	// public bucket reached over plain HTTPS (store/httpstore) is enough.
	//
	// The reader normally works from the encrypted manifest. If the archive
	// has none, it walks the chain instead: the object name of a segment
	// follows from the key and its first index, and each segment tells
	// where the next one starts. That walk begins at index 1 and can't
	// filter by time.
	ArchiveEncryption *ArchiveEncryption

	// TempDir is where segments are downloaded to, one at a time. Defaults
	// to os.TempDir(). Unused for encrypted archives.
	TempDir string

	// Follow keeps the reader alive at the end of the archive: Next blocks
	// and polls for newly uploaded segments instead of returning io.EOF.
	Follow bool

	// PollInterval is the catalog refresh period in Follow mode. Default 30s.
	PollInterval time.Duration

	// GapTimeout: in Follow mode segments are consumed strictly in index
	// order, waiting for a missing one (parallel uploads can land out of
	// order). If it still hasn't appeared after GapTimeout the reader logs a
	// warning and skips ahead. Default 10m.
	GapTimeout time.Duration

	// SkipMissing decides what happens at a hole in the archive.
	//
	// false (default): Next returns ErrArchiveGap, and keeps returning it.
	// A reader that walks the log to recount or search something must not
	// silently produce a result with records missing.
	//
	// true: the reader logs a warning with the missing range, adds it to
	// Stats().SkippedRecords and continues with the next records that exist.
	// (Not possible without a manifest: there the reader can't know where
	// the segment after a missing one starts, so it always stops.)
	SkipMissing bool

	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// ArchiveReaderStats is a snapshot of an ArchiveReader's progress.
type ArchiveReaderStats struct {
	// NextIndex is the lowest index not yet returned.
	NextIndex uint64
	// SkippedRecords counts records jumped over because of SkipMissing.
	SkippedRecords uint64
}

// ArchiveReader reads records straight from an archive, downloading the
// segments it needs — no access to the WAL directory or the producing host
// required. Not safe for concurrent use.
type ArchiveReader struct {
	opts   ArchiveReaderOptions
	log    *slog.Logger
	cipher *cipherAEAD

	segs    []ArchiveSegment
	nextIdx uint64 // lowest index still to be returned; 0 = wherever the selection starts
	started bool   // at least one segment has been opened
	done    bool
	gapAt   time.Time // when the current gap was first seen

	crypto *archiveCrypto
	// chain: no manifest; segments are found by deriving the name of the
	// one that starts at chainNext.
	chain       bool
	chainNext   uint64
	warnedEmpty bool
	skipped     uint64

	// Currently open segment.
	body   io.Closer // HTTP body of a streamed (encrypted) segment
	file   *os.File
	dec    *zstd.Decoder
	br     *bufio.Reader
	cur    ArchiveSegment
	curIdx uint64 // index of the next record in the open segment
}

// OpenArchiveReader loads the archive catalog and positions the reader at the
// start of the requested range.
func OpenArchiveReader(ctx context.Context, opts ArchiveReaderOptions) (*ArchiveReader, error) {
	if opts.Store == nil {
		return nil, errors.New("axwal: ArchiveReaderOptions.Store is required")
	}
	opts.Prefix = strings.Trim(opts.Prefix, "/")
	if opts.PollInterval <= 0 {
		opts.PollInterval = 30 * time.Second
	}
	if opts.GapTimeout <= 0 {
		opts.GapTimeout = 10 * time.Minute
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	c, err := newCipherFromConfig(opts.Encryption)
	if err != nil {
		return nil, err
	}
	crypto, err := newArchiveCrypto(opts.ArchiveEncryption)
	if err != nil {
		return nil, err
	}
	r := &ArchiveReader{
		opts:    opts,
		log:     opts.Logger.With("component", "axwal.archive-reader", "prefix", opts.Prefix),
		cipher:  c,
		crypto:  crypto,
		nextIdx: opts.FromIndex,
	}
	err = r.reload(ctx)
	if crypto != nil && errors.Is(err, ErrObjectNotFound) {
		if !opts.FromTime.IsZero() || !opts.ToTime.IsZero() {
			return nil, errors.New("axwal: this encrypted archive has no manifest; FromTime / ToTime need one")
		}
		r.log.Info("encrypted archive has no manifest; walking the segment chain from index 1")
		r.chain, r.chainNext, err = true, 1, nil
		// An archive written without a manifest still has a small "head"
		// object saying where it starts and how far it goes.
		if head, herr := getArchiveHead(ctx, opts.Store, opts.Prefix, crypto); herr == nil && head.FirstIndex > 1 {
			r.chainNext = head.FirstIndex
		}
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *ArchiveReader) reload(ctx context.Context) error {
	if r.chain {
		return nil
	}
	m, err := loadManifest(ctx, r.opts.Store, r.opts.Prefix, r.crypto)
	if err != nil {
		return err
	}
	r.segs = m.Segments
	// Reading "from the beginning" means from where the archive is declared
	// to begin, so that a first segment that hasn't been uploaded yet (or is
	// gone) shows up as a hole instead of being silently left out.
	if r.nextIdx == 0 && !r.started && m.FirstIndex > 0 &&
		r.opts.FromTime.IsZero() && r.opts.ToTime.IsZero() {
		r.nextIdx = m.FirstIndex
	}
	return nil
}

// Stats returns the reader's position and how much it had to skip.
func (r *ArchiveReader) Stats() ArchiveReaderStats {
	return ArchiveReaderStats{NextIndex: r.nextIdx, SkippedRecords: r.skipped}
}

// missing handles a hole covering indices from..to: an error by default, a
// counted jump with SkipMissing.
func (r *ArchiveReader) missing(from, to uint64, why string) error {
	if !r.opts.SkipMissing {
		return fmt.Errorf("%w: indices %d..%d (%s)", ErrArchiveGap, from, to, why)
	}
	r.skipped += to - from + 1
	r.log.Warn("records are missing from the archive; skipping (SkipMissing)",
		"missing_from", from, "missing_to", to, "reason", why)
	r.nextIdx = to + 1
	return nil
}

// Segments returns the archive catalog as last loaded (all segments, not just
// the selected range).
func (r *ArchiveReader) Segments() []ArchiveSegment {
	return append([]ArchiveSegment(nil), r.segs...)
}

// Next returns the next record and its index. At the end of the requested
// range (or of the archive, unless Follow is set) it returns io.EOF.
func (r *ArchiveReader) Next(ctx context.Context) ([]byte, uint64, error) {
	return r.next(ctx, true)
}

// NextBatch returns up to max records: it blocks for the first one like Next,
// then takes whatever else the already-downloaded segment holds.
func (r *ArchiveReader) NextBatch(ctx context.Context, max int) ([][]byte, []uint64, error) {
	if max <= 0 {
		return nil, nil, errors.New("axwal: NextBatch max must be > 0")
	}
	p, idx, err := r.next(ctx, true)
	if err != nil {
		return nil, nil, err
	}
	payloads, indices := [][]byte{p}, []uint64{idx}
	for len(payloads) < max {
		p, idx, err := r.next(ctx, false)
		if errors.Is(err, errNoRecordReady) || errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return payloads, indices, err
		}
		payloads = append(payloads, p)
		indices = append(indices, idx)
	}
	return payloads, indices, nil
}

// Ack is a no-op: an archive has no cursors. It exists so an ArchiveReader
// can stand in for a Reader.
func (r *ArchiveReader) Ack(uint64) error { return nil }

// Close releases the downloaded segment.
func (r *ArchiveReader) Close() error {
	r.closeSegment()
	return nil
}

var errNoRecordReady = errors.New("no record ready")

// next returns the next record. With block=false it never downloads or
// waits: it only drains the segment that is already open.
func (r *ArchiveReader) next(ctx context.Context, block bool) ([]byte, uint64, error) {
	for {
		if r.done {
			return nil, 0, io.EOF
		}
		if r.br != nil {
			payload, err := readRecord(r.br)
			if err == io.EOF {
				if r.chain {
					// The segment itself told us where the next one starts.
					r.chainNext = r.curIdx
				} else if r.curIdx != r.cur.LastIndex+1 {
					return nil, 0, fmt.Errorf("axwal: archive segment %s ended at index %d, expected %d",
						r.cur.Key, r.curIdx-1, r.cur.LastIndex)
				}
				r.closeSegment()
				continue
			}
			if err != nil {
				return nil, 0, fmt.Errorf("archive segment %s: %w", r.cur.Key, err)
			}
			idx := r.curIdx
			r.curIdx++
			if idx < r.nextIdx {
				continue // before FromIndex
			}
			if r.opts.ToIndex > 0 && idx > r.opts.ToIndex {
				r.finish()
				return nil, 0, io.EOF
			}
			r.nextIdx = idx + 1
			if r.cipher != nil {
				pt, derr := r.cipher.open(payload)
				if derr != nil {
					return nil, idx, derr
				}
				payload = pt
			}
			return payload, idx, nil
		}

		if !block {
			return nil, 0, errNoRecordReady
		}
		if r.opts.ToIndex > 0 && r.nextIdx > r.opts.ToIndex {
			r.finish()
			return nil, 0, io.EOF
		}
		seg, ok, err := r.pickSegment()
		if err != nil {
			return nil, 0, err
		}
		if r.chain {
			seg, ok = ArchiveSegment{
				Key:        archiveKey(r.opts.Prefix, r.crypto.segmentName(r.chainNext)),
				FirstIndex: r.chainNext,
				Encryption: archiveEncAlgo,
			}, true
		}
		if ok {
			err := r.openSegment(ctx, seg)
			switch {
			case err == nil:
				r.gapAt = time.Time{}
				continue
			case r.chain && errors.Is(err, ErrObjectNotFound):
				// Either the chain ends here, or this one object is missing
				// and the log goes on behind it. Only the head object can
				// tell the two apart.
				if herr := r.chainHole(ctx); herr != nil {
					return nil, 0, herr
				}
				// The end (for now). If the chain never even started,
				// "empty archive" is the least likely explanation — say so
				// rather than report a clean end of nothing.
				if !r.started && !r.opts.Follow {
					return nil, 0, fmt.Errorf("axwal: no manifest and no segment starting at index %d under this key: "+
						"wrong archive key, wrong prefix, or an empty archive: %w", r.chainNext, ErrObjectNotFound)
				}
				if !r.started && !r.warnedEmpty {
					r.warnedEmpty = true
					r.log.Warn("nothing found under this archive key yet (wrong key or prefix, or the archive is still empty); waiting",
						"first_index", r.chainNext)
				}
			case errors.Is(err, ErrObjectNotFound):
				// Listed in the manifest, absent from the store.
				from := max(r.nextIdx, seg.FirstIndex)
				if merr := r.missing(from, seg.LastIndex,
					"object "+seg.Key+" is in the manifest but not in the store"); merr != nil {
					return nil, 0, merr
				}
				continue
			default:
				return nil, 0, err
			}
		}
		if !r.opts.Follow {
			r.finish()
			return nil, 0, io.EOF
		}
		if err := sleepCtx(ctx, r.opts.PollInterval); err != nil {
			return nil, 0, err
		}
		if err := r.reload(ctx); err != nil {
			// The archive being briefly unreachable shouldn't kill a follower.
			r.log.Warn("catalog refresh failed; will retry", "err", err)
		}
	}
}

func (r *ArchiveReader) finish() {
	r.closeSegment()
	r.done = true
}

// chainHole is called in chain mode when the segment starting at chainNext
// doesn't exist. It returns ErrArchiveGap if the archive's head object says
// the log continues beyond that point; nil if this is (as far as anyone can
// tell) the end.
func (r *ArchiveReader) chainHole(ctx context.Context) error {
	head, err := getArchiveHead(ctx, r.opts.Store, r.opts.Prefix, r.crypto)
	if err == nil && !r.started && head.FirstIndex > r.chainNext {
		// The archive (which didn't exist yet when we opened it) starts
		// later than index 1.
		r.chainNext = head.FirstIndex
	}
	if err != nil || head.LastIndex < r.chainNext {
		r.gapAt = time.Time{}
		return nil
	}
	// With parallel uploads a later segment (and the head) can land before
	// an earlier one; a follower gives it time.
	if r.gapAt.IsZero() {
		r.gapAt = time.Now()
	}
	if r.opts.Follow && time.Since(r.gapAt) < r.opts.GapTimeout {
		return nil
	}
	return fmt.Errorf("%w: the segment starting at index %d is not in the store, but the archive goes on to index %d; "+
		"without a manifest the reader can't tell where the next segment starts, so it can't skip",
		ErrArchiveGap, r.chainNext, head.LastIndex)
}

// pickSegment selects the next segment to read from the catalog; ok=false if
// there is none (yet). A hole before it is an error or a counted skip,
// depending on SkipMissing.
func (r *ArchiveReader) pickSegment() (ArchiveSegment, bool, error) {
	if r.chain {
		return ArchiveSegment{}, false, nil
	}
	timeFilter := !r.opts.FromTime.IsZero() || !r.opts.ToTime.IsZero()
	for _, s := range r.segs {
		if s.LastIndex < r.nextIdx || s.LastIndex < s.FirstIndex {
			continue
		}
		if !r.opts.FromTime.IsZero() && s.LastRecordUnix < r.opts.FromTime.Unix() {
			continue
		}
		if !r.opts.ToTime.IsZero() && s.FirstRecordUnix > r.opts.ToTime.Unix() {
			break
		}
		// s is the lowest remaining segment. Is something missing before it?
		// (Before the first segment that only counts if a start index was
		// asked for explicitly.)
		if s.FirstIndex > r.nextIdx && r.nextIdx > 0 && (r.started || !timeFilter) {
			if r.gapAt.IsZero() {
				r.gapAt = time.Now()
			}
			if r.opts.Follow && time.Since(r.gapAt) < r.opts.GapTimeout {
				// Parallel uploads can land out of order: give it time.
				return ArchiveSegment{}, false, nil
			}
			to := s.FirstIndex - 1
			if r.opts.ToIndex > 0 && to > r.opts.ToIndex {
				to = r.opts.ToIndex
			}
			if err := r.missing(r.nextIdx, to, "no segment in the archive catalog covers them"); err != nil {
				return ArchiveSegment{}, false, err
			}
		}
		r.gapAt = time.Time{}
		if r.opts.ToIndex > 0 && s.FirstIndex > r.opts.ToIndex {
			break
		}
		return s, true, nil
	}
	return ArchiveSegment{}, false, nil
}

// openSegment downloads seg to a temp file, verifies it against the catalog
// and opens it for reading.
func (r *ArchiveReader) openSegment(ctx context.Context, seg ArchiveSegment) error {
	if seg.Encryption != "" && r.crypto != nil {
		// Derive the name instead of trusting the manifest's path: the
		// reader may reach the archive under a different prefix than the
		// writer used (a public URL that already points into it, a CDN).
		seg.Key = archiveKey(r.opts.Prefix, r.crypto.segmentName(seg.FirstIndex))
	}
	rc, err := r.opts.Store.Get(ctx, seg.Key)
	if err != nil {
		return fmt.Errorf("get %s: %w", seg.Key, err)
	}
	if seg.Encryption != "" {
		return r.openStream(rc, seg)
	}
	defer rc.Close()

	f, err := os.CreateTemp(r.opts.TempDir, "axwal-archive-*")
	if err != nil {
		return err
	}
	// Unlink right away: the data lives as long as the handle and can't be
	// left behind by a crash.
	_ = os.Remove(f.Name())

	h := sha1.New()
	n, err := io.Copy(io.MultiWriter(f, h), rc)
	if err == nil && seg.SizeBytes > 0 && n != seg.SizeBytes {
		err = fmt.Errorf("size %d, catalog says %d", n, seg.SizeBytes)
	}
	if got := hex.EncodeToString(h.Sum(nil)); err == nil && seg.SHA1 != "" && !strings.EqualFold(got, seg.SHA1) {
		err = fmt.Errorf("sha1 %s, catalog says %s", got, seg.SHA1)
	}
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		f.Close()
		return fmt.Errorf("download %s: %w", seg.Key, err)
	}

	var src io.Reader = f
	if seg.Compression == "zst" {
		dec, err := zstd.NewReader(f)
		if err != nil {
			f.Close()
			return fmt.Errorf("zstd reader: %w", err)
		}
		r.dec = dec
		src = dec
	}
	r.file = f
	r.br = bufio.NewReaderSize(src, 64*1024)
	r.cur = seg
	r.curIdx = seg.FirstIndex
	if !r.started || seg.FirstIndex > r.nextIdx {
		r.nextIdx = max(r.nextIdx, seg.FirstIndex)
	}
	r.started = true
	return nil
}

// openStream reads an encrypted segment as it downloads: decrypt, then
// decompress if the plaintext turns out to be zstd, then decode records.
// Every chunk is authenticated by the cipher, so no separate checksum pass
// (and no temp file) is needed.
func (r *ArchiveReader) openStream(rc io.ReadCloser, seg ArchiveSegment) error {
	if r.crypto == nil {
		rc.Close()
		return fmt.Errorf("%w: archive segment %s is encrypted; set ArchiveReaderOptions.ArchiveEncryption",
			ErrEncryption, seg.Key)
	}
	plain := bufio.NewReaderSize(r.crypto.decryptReader(rc), 64*1024)
	var src io.Reader = plain
	// The manifest says whether the segment is compressed, but in chain mode
	// there is no manifest — the zstd frame magic settles it either way.
	if magic, err := plain.Peek(4); err == nil && magic[0] == 0x28 && magic[1] == 0xB5 && magic[2] == 0x2F && magic[3] == 0xFD {
		dec, err := zstd.NewReader(plain)
		if err != nil {
			rc.Close()
			return fmt.Errorf("zstd reader: %w", err)
		}
		r.dec = dec
		src = dec
	} else if err != nil && !errors.Is(err, io.EOF) {
		rc.Close()
		return fmt.Errorf("archive segment %s: %w", seg.Key, err)
	}
	r.body = rc
	r.br = bufio.NewReaderSize(src, 64*1024)
	r.cur = seg
	r.curIdx = seg.FirstIndex
	if !r.started || seg.FirstIndex > r.nextIdx {
		r.nextIdx = max(r.nextIdx, seg.FirstIndex)
	}
	r.started = true
	return nil
}

func (r *ArchiveReader) closeSegment() {
	if r.body != nil {
		_ = r.body.Close()
		r.body = nil
	}
	if r.dec != nil {
		r.dec.Close()
		r.dec = nil
	}
	if r.file != nil {
		_ = r.file.Close()
		r.file = nil
	}
	r.br = nil
}
