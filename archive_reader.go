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

	// TempDir is where segments are downloaded to, one at a time. Defaults
	// to os.TempDir().
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

	// Logger defaults to slog.Default().
	Logger *slog.Logger
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

	// Currently open segment.
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
	r := &ArchiveReader{
		opts:    opts,
		log:     opts.Logger.With("component", "axwal.archive-reader", "prefix", opts.Prefix),
		cipher:  c,
		nextIdx: opts.FromIndex,
	}
	if err := r.reload(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *ArchiveReader) reload(ctx context.Context) error {
	m, err := LoadArchiveManifest(ctx, r.opts.Store, r.opts.Prefix)
	if err != nil {
		return err
	}
	r.segs = m.Segments
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
				if r.curIdx != r.cur.LastIndex+1 {
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
		seg, ok := r.pickSegment()
		if ok {
			if err := r.openSegment(ctx, seg); err != nil {
				return nil, 0, err
			}
			continue
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

// pickSegment selects the next segment to read, or ok=false if there is none
// (yet).
func (r *ArchiveReader) pickSegment() (ArchiveSegment, bool) {
	for _, s := range r.segs {
		if s.LastIndex < r.nextIdx || s.LastIndex < s.FirstIndex {
			continue
		}
		if r.opts.ToIndex > 0 && s.FirstIndex > r.opts.ToIndex {
			break
		}
		if !r.opts.FromTime.IsZero() && s.LastRecordUnix < r.opts.FromTime.Unix() {
			continue
		}
		if !r.opts.ToTime.IsZero() && s.FirstRecordUnix > r.opts.ToTime.Unix() {
			break
		}
		// s is the lowest remaining segment. Is something missing before it?
		if r.started && s.FirstIndex > r.nextIdx {
			if r.gapAt.IsZero() {
				r.gapAt = time.Now()
			}
			if r.opts.Follow && time.Since(r.gapAt) < r.opts.GapTimeout {
				return ArchiveSegment{}, false // probably still uploading
			}
			r.log.Warn("archive gap: records are missing; skipping ahead",
				"missing_from", r.nextIdx, "missing_to", s.FirstIndex-1)
		}
		r.gapAt = time.Time{}
		return s, true
	}
	return ArchiveSegment{}, false
}

// openSegment downloads seg to a temp file, verifies it against the catalog
// and opens it for reading.
func (r *ArchiveReader) openSegment(ctx context.Context, seg ArchiveSegment) error {
	rc, err := r.opts.Store.Get(ctx, seg.Key)
	if err != nil {
		return fmt.Errorf("get %s: %w", seg.Key, err)
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

func (r *ArchiveReader) closeSegment() {
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
