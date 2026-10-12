package axwal

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// ArchiveCheckOptions configures CheckArchive.
type ArchiveCheckOptions struct {
	// Store / Prefix / ArchiveEncryption locate the archive, as in
	// ArchiveConfig.
	Store             ArchiveStore
	Prefix            string
	ArchiveEncryption *ArchiveEncryption

	// Dir, if set, is the writer's WAL directory. The check then also
	// compares the archive against what the writer believes it has
	// uploaded: every local segment carrying the .archived mark must really
	// be there. That works even for an archive without a manifest.
	Dir string

	// Deep downloads every object and verifies it end to end: size and
	// SHA-1 against the manifest, decryption, decompression, record CRCs
	// and the record count. Without it only the existence of each object is
	// checked.
	Deep bool

	// Repair (needs Dir): for every local segment that is marked archived
	// but missing or damaged in the archive, remove the mark, so the
	// writer's archiver uploads it again.
	Repair bool
}

// IndexRange is an inclusive range of record indices.
type IndexRange struct {
	First, Last uint64
}

func (r IndexRange) String() string { return fmt.Sprintf("%d..%d", r.First, r.Last) }

// ArchiveProblem is one object that is missing or damaged.
type ArchiveProblem struct {
	IndexRange
	Key string
	// Missing: the object doesn't exist. Otherwise it exists but is wrong.
	Missing bool
	Detail  string
}

// ArchiveCheckResult is what CheckArchive found.
type ArchiveCheckResult struct {
	// HasManifest is false for an archive without a catalog; then only the
	// comparison with Dir was possible.
	HasManifest bool
	Segments    int
	FirstIndex  uint64
	LastIndex   uint64
	Bytes       int64

	// Gaps are index ranges inside FirstIndex..LastIndex that no segment in
	// the manifest covers.
	Gaps []IndexRange
	// Problems are objects that are missing from the store or damaged.
	Problems []ArchiveProblem

	// The remaining fields are filled when Dir was given.
	//
	// LocalPending: closed segments not yet uploaded (normal while the
	// archiver is catching up).
	// LocalLost: segments marked .archived on disk that are NOT intact in
	// the archive — the case that must never be left alone, because
	// retention deletes marked segments.
	// Repaired: the subset of LocalLost whose mark was removed.
	LocalPending []IndexRange
	LocalLost    []IndexRange
	Repaired     []IndexRange
	// LocalLastIndex is the last record in a closed local segment.
	LocalLastIndex uint64
}

// OK reports whether the archive is complete and consistent as far as it was
// checked. Pending uploads don't count against it.
func (r *ArchiveCheckResult) OK() bool {
	return len(r.Gaps) == 0 && len(r.Problems) == 0 && len(r.LocalLost) == len(r.Repaired)
}

// CheckArchive verifies that an archive holds what it should: no holes in
// the index sequence, every cataloged object present (and, with Deep,
// intact), and — given the writer's directory — everything the writer marked
// as uploaded really there.
func CheckArchive(ctx context.Context, opts ArchiveCheckOptions) (*ArchiveCheckResult, error) {
	if opts.Store == nil {
		return nil, errors.New("axwal: ArchiveCheckOptions.Store is required")
	}
	if opts.Repair && opts.Dir == "" {
		return nil, errors.New("axwal: ArchiveCheckOptions.Repair needs Dir")
	}
	prefix := strings.Trim(opts.Prefix, "/")
	crypto, err := newArchiveCrypto(opts.ArchiveEncryption)
	if err != nil {
		return nil, err
	}
	res := &ArchiveCheckResult{}

	m, err := loadManifest(ctx, opts.Store, prefix, crypto)
	switch {
	case err == nil:
		res.HasManifest = true
	case errors.Is(err, ErrObjectNotFound) && opts.Dir != "":
		m = &ArchiveManifest{} // compare against the directory only
	default:
		return nil, err
	}

	// bad[first] is set for segments found missing or damaged.
	bad := map[uint64]bool{}
	inCatalog := map[segRange]ArchiveSegment{}
	check := func(seg ArchiveSegment) {
		p := checkArchiveObject(ctx, opts.Store, crypto, seg, opts.Deep)
		if p != nil {
			res.Problems = append(res.Problems, *p)
			bad[seg.FirstIndex] = true
		}
	}

	segs := append([]ArchiveSegment(nil), m.Segments...)
	sort.Slice(segs, func(i, j int) bool { return segs[i].FirstIndex < segs[j].FirstIndex })
	for i, s := range segs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		inCatalog[segRange{s.FirstIndex, s.LastIndex}] = s
		res.Segments++
		res.Bytes += s.SizeBytes
		if i == 0 {
			res.FirstIndex = s.FirstIndex
			if m.FirstIndex > 0 && s.FirstIndex > m.FirstIndex {
				// The beginning of the log never made it.
				res.Gaps = append(res.Gaps, IndexRange{m.FirstIndex, s.FirstIndex - 1})
				res.FirstIndex = m.FirstIndex
			}
		} else if prev := segs[i-1].LastIndex; s.FirstIndex > prev+1 {
			res.Gaps = append(res.Gaps, IndexRange{prev + 1, s.FirstIndex - 1})
		}
		if s.LastIndex > res.LastIndex {
			res.LastIndex = s.LastIndex
		}
		if crypto != nil && s.Encryption != "" {
			s.Key = archiveKey(prefix, crypto.segmentName(s.FirstIndex))
		}
		check(s)
	}

	if opts.Dir == "" {
		return res, nil
	}

	// What does the writer's directory say?
	matches, err := filepath.Glob(filepath.Join(opts.Dir, "wal-*"))
	if err != nil {
		return nil, err
	}
	local := map[segRange]segName{}
	for _, p := range matches {
		s, perr := parseSegName(filepath.Base(p))
		if perr != nil || !s.Closed || s.LastIndex < s.FirstIndex {
			continue
		}
		k := segRange{s.FirstIndex, s.LastIndex}
		if prev, dup := local[k]; dup && prev.Compression != "" {
			continue
		}
		local[k] = s
	}
	keys := make([]segRange, 0, len(local))
	for k := range local {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].first < keys[j].first })
	for _, k := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s := local[k]
		rng := IndexRange{k.first, k.last}
		if k.last > res.LocalLastIndex {
			res.LocalLastIndex = k.last
		}
		if !isArchived(opts.Dir, s) {
			res.LocalPending = append(res.LocalPending, rng)
			continue
		}
		lost := false
		if entry, ok := inCatalog[k]; ok {
			lost = bad[entry.FirstIndex]
		} else if res.HasManifest {
			lost = true // marked on disk, unknown to the catalog
			res.Problems = append(res.Problems, ArchiveProblem{IndexRange: rng, Missing: true,
				Detail: "marked .archived locally but not in the manifest"})
		} else if crypto != nil {
			// No manifest: the object name follows from the key.
			seg := ArchiveSegment{Key: archiveKey(prefix, crypto.segmentName(k.first)),
				FirstIndex: k.first, LastIndex: k.last, Encryption: archiveEncAlgo}
			if p := checkArchiveObject(ctx, opts.Store, crypto, seg, opts.Deep); p != nil {
				res.Problems = append(res.Problems, *p)
				lost = true
			}
		}
		if !lost {
			continue
		}
		res.LocalLost = append(res.LocalLost, rng)
		if opts.Repair {
			if err := removeArchivedSidecar(opts.Dir, s); err != nil {
				return res, fmt.Errorf("repair %s: %w", rng, err)
			}
			res.Repaired = append(res.Repaired, rng)
		}
	}
	if len(res.Repaired) > 0 {
		_ = fsyncDir(opts.Dir)
	}
	return res, nil
}

// checkArchiveObject fetches one object and returns what is wrong with it,
// or nil.
func checkArchiveObject(ctx context.Context, store ArchiveStore, crypto *archiveCrypto, seg ArchiveSegment, deep bool) *ArchiveProblem {
	problem := func(missing bool, format string, a ...any) *ArchiveProblem {
		return &ArchiveProblem{IndexRange: IndexRange{seg.FirstIndex, seg.LastIndex}, Key: seg.Key,
			Missing: missing, Detail: fmt.Sprintf(format, a...)}
	}
	rc, err := store.Get(ctx, seg.Key)
	if errors.Is(err, ErrObjectNotFound) {
		return problem(true, "object is not in the store")
	}
	if err != nil {
		return problem(false, "cannot fetch: %v", err)
	}
	defer rc.Close()
	if !deep {
		return nil
	}

	h := sha1.New()
	cr := &countingReader{r: io.TeeReader(rc, h)}
	var src io.Reader = cr
	if seg.Encryption != "" {
		if crypto == nil {
			return problem(false, "object is encrypted and no archive key was given")
		}
		src = crypto.decryptReader(cr)
	}
	plain := bufio.NewReaderSize(src, 64*1024)
	src = plain
	if magic, perr := plain.Peek(4); perr == nil && magic[0] == 0x28 && magic[1] == 0xB5 && magic[2] == 0x2F && magic[3] == 0xFD {
		dec, derr := zstd.NewReader(plain)
		if derr != nil {
			return problem(false, "zstd: %v", derr)
		}
		defer dec.Close()
		src = dec
	}
	br := bufio.NewReaderSize(src, 64*1024)
	var records uint64
	for {
		_, rerr := readRecord(br)
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return problem(false, "record %d: %v", seg.FirstIndex+records, rerr)
		}
		records++
	}
	if want := seg.LastIndex - seg.FirstIndex + 1; records != want {
		return problem(false, "holds %d records, expected %d", records, want)
	}
	// Drain whatever framing is left so the byte count and hash are complete.
	_, _ = io.Copy(io.Discard, cr)
	if seg.SizeBytes > 0 && cr.n != seg.SizeBytes {
		return problem(false, "size %d, manifest says %d", cr.n, seg.SizeBytes)
	}
	if got := hex.EncodeToString(h.Sum(nil)); seg.SHA1 != "" && !strings.EqualFold(got, seg.SHA1) {
		return problem(false, "sha1 %s, manifest says %s", got, seg.SHA1)
	}
	return nil
}
