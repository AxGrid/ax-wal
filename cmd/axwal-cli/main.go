// axwal-cli is the admin tool for inspecting and manipulating a WAL directory.
//
//	axwal-cli ls -dir /path/to/wal              # list segments + sizes + archived?
//	axwal-cli cursors -dir /path/to/wal         # list reader cursors + heartbeat age
//	axwal-cli verify -dir /path/to/wal          # CRC-walk every segment
//	axwal-cli tail -dir /path/to/wal -name dbg  # follow live (Ctrl-C to stop)
//	axwal-cli mark-archived -dir X -range 1-100 # touch wal-1-100.archived
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	axwal "github.com/axgrid/ax-wal"
	"github.com/klauspost/compress/zstd"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	switch cmd {
	case "ls":
		cmdLs(args)
	case "cursors":
		cmdCursors(args)
	case "verify":
		cmdVerify(args)
	case "tail":
		cmdTail(args)
	case "mark-archived":
		cmdMarkArchived(args)
	case "stats":
		cmdStats(args)
	case "dump":
		cmdDump(args)
	case "restore":
		cmdRestore(args)
	case "compact":
		cmdCompact(args)
	case "fsck":
		cmdFsck(args)
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n\n", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println(`axwal-cli — WAL admin tool

Subcommands:
  ls              list segments in a WAL directory
  cursors         list reader cursors and their heartbeat age
  stats           summary table: head index, segments, total bytes, per-cursor lag
  verify          CRC-scan every segment, report bad records
  fsck            verify + remove bad segments and dedupe twins (--fix to apply)
  tail            follow the WAL live (registers a temporary cursor)
  dump            print records in a range as JSON {idx, len, payload_base64}
  restore         append records from a JSON dump file into a target dir
  compact         merge a contiguous range of closed segments into a single one
  mark-archived   touch the .archived sidecar so retention can delete the segment

Run "axwal-cli <subcommand> -h" for flags.`)
}

// ---------------- ls ----------------

func cmdLs(args []string) {
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	dir := fs.String("dir", "", "WAL directory")
	fs.Parse(args)
	if *dir == "" {
		fatal("ls: -dir required")
	}

	matches, err := filepath.Glob(filepath.Join(*dir, "wal-*"))
	if err != nil {
		fatal("glob: %v", err)
	}

	type row struct {
		name      string
		size      int64
		first     uint64
		last      uint64
		records   int64
		closed    bool
		zst       bool
		archived  bool
		isSidecar bool
	}
	var rows []row
	archivedSet := map[string]bool{}
	for _, p := range matches {
		base := filepath.Base(p)
		if strings.HasSuffix(base, ".archived") {
			archivedSet[strings.TrimSuffix(base, ".archived")] = true
		}
	}

	for _, p := range matches {
		base := filepath.Base(p)
		fi, err := os.Stat(p)
		if err != nil {
			continue
		}
		if strings.HasSuffix(base, ".archived") {
			continue
		}
		r := row{name: base, size: fi.Size()}

		// Try to parse name. We replicate the parser inline since it's
		// internal to the package — but accept both compressed and open forms.
		if first, last, closed, zst, ok := parseSeg(base); ok {
			r.first = first
			r.last = last
			r.closed = closed
			r.zst = zst
			if closed {
				r.records = int64(last) - int64(first) + 1
				if r.records < 0 {
					r.records = 0
				}
				key := fmt.Sprintf("wal-%d-%d", first, last)
				r.archived = archivedSet[key]
			}
		}
		rows = append(rows, r)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].first != rows[j].first {
			return rows[i].first < rows[j].first
		}
		return rows[i].name < rows[j].name
	})

	if len(rows) == 0 {
		fmt.Println("(empty)")
		return
	}
	fmt.Printf("%-32s %10s %12s %12s %10s %5s %9s\n",
		"name", "size", "first", "last", "records", "zst", "archived")
	var totalSize int64
	var totalRecs int64
	for _, r := range rows {
		zstFlag := ""
		if r.zst {
			zstFlag = "yes"
		}
		archFlag := ""
		if r.archived {
			archFlag = "yes"
		}
		closed := "open"
		if r.closed {
			closed = ""
		}
		_ = closed
		first := ""
		last := ""
		records := ""
		if r.first > 0 {
			first = strconv.FormatUint(r.first, 10)
		}
		if r.closed {
			last = strconv.FormatUint(r.last, 10)
			records = strconv.FormatInt(r.records, 10)
		} else {
			last = "(open)"
		}
		fmt.Printf("%-32s %10s %12s %12s %10s %5s %9s\n",
			r.name, humanBytes(r.size), first, last, records, zstFlag, archFlag)
		totalSize += r.size
		totalRecs += r.records
	}
	fmt.Printf("\n%d segment(s), total size %s, %d closed records\n", len(rows), humanBytes(totalSize), totalRecs)
}

// ---------------- cursors ----------------

func cmdCursors(args []string) {
	fs := flag.NewFlagSet("cursors", flag.ExitOnError)
	dir := fs.String("dir", "", "WAL directory")
	fs.Parse(args)
	if *dir == "" {
		fatal("cursors: -dir required")
	}

	cdir := filepath.Join(*dir, "cursors")
	entries, err := os.ReadDir(cdir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Println("(no cursors directory)")
			return
		}
		fatal("readdir: %v", err)
	}

	type rec struct {
		Name              string `json:"name"`
		Index             uint64 `json:"index"`
		PID               int    `json:"pid"`
		StartedAtUnix     int64  `json:"started_at_unix"`
		LastHeartbeatUnix int64  `json:"last_heartbeat_unix"`
	}
	var rows []rec
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".cursor") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(cdir, e.Name()))
		if err != nil {
			continue
		}
		var r rec
		if err := json.Unmarshal(body, &r); err != nil {
			continue
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })

	if len(rows) == 0 {
		fmt.Println("(no cursors)")
		return
	}
	now := time.Now().Unix()
	fmt.Printf("%-20s %12s %8s %16s %12s\n", "name", "index", "pid", "heartbeat-age", "status")
	for _, r := range rows {
		age := now - r.LastHeartbeatUnix
		ageStr := formatAge(age)
		status := "alive"
		if age > 60 {
			status = "stale"
		}
		fmt.Printf("%-20s %12d %8d %16s %12s\n", r.Name, r.Index, r.PID, ageStr, status)
	}
}

// ---------------- verify ----------------

func cmdVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	dir := fs.String("dir", "", "WAL directory")
	fs.Parse(args)
	if *dir == "" {
		fatal("verify: -dir required")
	}

	matches, err := filepath.Glob(filepath.Join(*dir, "wal-*"))
	if err != nil {
		fatal("glob: %v", err)
	}
	var paths []string
	for _, p := range matches {
		b := filepath.Base(p)
		if strings.HasSuffix(b, ".archived") || b == ".writer.lock" {
			continue
		}
		if _, _, _, _, ok := parseSeg(b); ok {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)

	totalRecs := 0
	totalErrs := 0
	for _, p := range paths {
		recs, err := walkSegment(p)
		base := filepath.Base(p)
		if err != nil {
			totalErrs++
			fmt.Printf("%-32s  %d records, ERROR: %v\n", base, recs, err)
		} else {
			fmt.Printf("%-32s  %d records, OK\n", base, recs)
		}
		totalRecs += recs
	}
	fmt.Printf("\n%d file(s), %d records total, %d errors\n", len(paths), totalRecs, totalErrs)
	if totalErrs > 0 {
		os.Exit(1)
	}
}

// walkSegment opens p (with .zst decompression if needed) and counts records,
// validating CRC. Returns the number of OK records read before any error.
func walkSegment(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var src io.Reader = f
	if strings.HasSuffix(path, ".zst") {
		dec, err := zstd.NewReader(f)
		if err != nil {
			return 0, err
		}
		defer dec.Close()
		src = dec
	}
	br := bufio.NewReaderSize(src, 64*1024)

	count := 0
	for {
		_, err := readFrame(br)
		if err == io.EOF {
			return count, nil
		}
		if err != nil {
			return count, err
		}
		count++
	}
}

// ---------------- tail ----------------

func cmdTail(args []string) {
	fs := flag.NewFlagSet("tail", flag.ExitOnError)
	dir := fs.String("dir", "", "WAL directory")
	name := fs.String("name", fmt.Sprintf("axwal-cli-tail-%d", os.Getpid()), "cursor name (temporary)")
	from := fs.String("from", "earliest", "earliest | latest | <index>")
	hex := fs.Bool("hex", false, "print payload as hex instead of string")
	fs.Parse(args)
	if *dir == "" {
		fatal("tail: -dir required")
	}

	opts := axwal.ReaderOptions{
		Dir:               *dir,
		Name:              *name,
		HeartbeatInterval: time.Second,
		PollInterval:      50 * time.Millisecond,
	}
	switch *from {
	case "earliest":
		opts.StartFrom = axwal.StartEarliest
	case "latest":
		opts.StartFrom = axwal.StartLatest
	default:
		idx, err := strconv.ParseUint(*from, 10, 64)
		if err != nil || idx == 0 {
			fatal("tail: -from must be 'earliest', 'latest', or a positive uint64")
		}
		opts.StartFrom = axwal.StartAt
		opts.StartAtIndex = idx
	}

	r, err := axwal.OpenReader(opts)
	if err != nil {
		fatal("open reader: %v", err)
	}
	defer r.Close()

	ctx, cancel := signalContext()
	defer cancel()

	for {
		payload, idx, err := r.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			fatal("next: %v", err)
		}
		if *hex {
			fmt.Printf("%d: %x\n", idx, payload)
		} else {
			fmt.Printf("%d: %s\n", idx, string(payload))
		}
		_ = r.Ack(idx)
	}
}

// ---------------- mark-archived ----------------

func cmdMarkArchived(args []string) {
	fs := flag.NewFlagSet("mark-archived", flag.ExitOnError)
	dir := fs.String("dir", "", "WAL directory")
	rng := fs.String("range", "", "segment index range as F-L (e.g. 1-100)")
	fs.Parse(args)
	if *dir == "" || *rng == "" {
		fatal("mark-archived: both -dir and -range required")
	}
	parts := strings.SplitN(*rng, "-", 2)
	if len(parts) != 2 {
		fatal("mark-archived: -range must be F-L")
	}
	first, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		fatal("mark-archived: bad first index: %v", err)
	}
	last, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		fatal("mark-archived: bad last index: %v", err)
	}
	if err := axwal.MarkArchived(*dir, first, last); err != nil {
		fatal("%v", err)
	}
	fmt.Printf("marked wal-%d-%d as archived\n", first, last)
}

// ---------------- stats ----------------

func cmdStats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	dir := fs.String("dir", "", "WAL directory")
	fs.Parse(args)
	if *dir == "" {
		fatal("stats: -dir required")
	}

	matches, err := filepath.Glob(filepath.Join(*dir, "wal-*"))
	if err != nil {
		fatal("glob: %v", err)
	}
	var (
		totalBytes int64
		closedCnt  int
		openCnt    int
		head       uint64
		first      uint64
	)
	for _, p := range matches {
		base := filepath.Base(p)
		if strings.HasSuffix(base, ".archived") || strings.HasSuffix(base, ".meta") {
			continue
		}
		fi, err := os.Stat(p)
		if err != nil {
			continue
		}
		totalBytes += fi.Size()
		f, l, closed, _, ok := parseSeg(base)
		if !ok {
			continue
		}
		if first == 0 || f < first {
			first = f
		}
		if closed {
			closedCnt++
			if l > head {
				head = l
			}
		} else {
			openCnt++
		}
	}

	fmt.Printf("dir:           %s\n", *dir)
	fmt.Printf("segments:      %d closed + %d open = %d total\n", closedCnt, openCnt, closedCnt+openCnt)
	fmt.Printf("disk:          %s\n", humanBytes(totalBytes))
	fmt.Printf("first index:   %d\n", first)
	fmt.Printf("head index:    %d (closed-segment max; open-segment may extend further)\n", head)

	// Cursors with lag.
	cdir := filepath.Join(*dir, "cursors")
	entries, err := os.ReadDir(cdir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			fatal("readdir cursors: %v", err)
		}
		fmt.Println("(no cursors)")
		return
	}
	type rec struct {
		Name              string `json:"name"`
		Index             uint64 `json:"index"`
		PID               int    `json:"pid"`
		LastHeartbeatUnix int64  `json:"last_heartbeat_unix"`
	}
	type row struct {
		name string
		idx  uint64
		lag  int64 // signed: -1 if cursor > head (shouldn't happen)
		age  int64
		pid  int
	}
	var rows []row
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".cursor") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(cdir, e.Name()))
		if err != nil {
			continue
		}
		// Strip framed header if present.
		jsonBody := body
		if len(body) >= 8 && body[0] == 'A' && body[1] == 'X' && body[2] == 'C' && body[3] == '1' {
			jsonBody = body[8:]
		}
		var r rec
		if err := json.Unmarshal(jsonBody, &r); err != nil {
			continue
		}
		now := time.Now().Unix()
		var lag int64
		if r.Index <= head {
			lag = int64(head - r.Index)
		} else {
			lag = -int64(r.Index - head)
		}
		rows = append(rows, row{r.Name, r.Index, lag, now - r.LastHeartbeatUnix, r.PID})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
	if len(rows) == 0 {
		fmt.Println("(no cursors)")
		return
	}
	fmt.Printf("\n%-20s %12s %12s %14s %8s\n", "cursor", "index", "lag (rec)", "heartbeat-age", "pid")
	for _, r := range rows {
		fmt.Printf("%-20s %12d %12d %14s %8d\n", r.name, r.idx, r.lag, formatAge(r.age), r.pid)
	}
}

// ---------------- dump ----------------

func cmdDump(args []string) {
	fs := flag.NewFlagSet("dump", flag.ExitOnError)
	dir := fs.String("dir", "", "WAL directory")
	from := fs.Uint64("from", 0, "first index to dump (inclusive). 0=earliest")
	to := fs.Uint64("to", 0, "last index to dump (inclusive). 0=read until ctx-stop")
	fs.Parse(args)
	if *dir == "" {
		fatal("dump: -dir required")
	}

	opts := axwal.ReaderOptions{
		Dir:               *dir,
		Name:              fmt.Sprintf("axwal-cli-dump-%d", os.Getpid()),
		HeartbeatInterval: time.Second,
		PollInterval:      50 * time.Millisecond,
	}
	if *from == 0 {
		opts.StartFrom = axwal.StartEarliest
	} else {
		opts.StartFrom = axwal.StartAt
		opts.StartAtIndex = *from
	}

	r, err := axwal.OpenReader(opts)
	if err != nil {
		fatal("open reader: %v", err)
	}
	defer r.Close()

	ctx, cancel := signalContext()
	defer cancel()

	enc := json.NewEncoder(os.Stdout)
	for {
		payload, idx, err := r.Next(ctx)
		if err != nil {
			return
		}
		if err := enc.Encode(map[string]any{
			"idx":     idx,
			"len":     len(payload),
			"payload": payload, // base64 in JSON
		}); err != nil {
			fatal("encode: %v", err)
		}
		if *to != 0 && idx >= *to {
			return
		}
	}
}

// ---------------- restore ----------------

func cmdRestore(args []string) {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	dir := fs.String("dir", "", "TARGET WAL directory (writer will append here)")
	src := fs.String("src", "", "JSON dump file (one record per line, format: {idx, len, payload})")
	fs.Parse(args)
	if *dir == "" || *src == "" {
		fatal("restore: -dir and -src required")
	}

	w, err := axwal.OpenWriter(axwal.WriterOptions{Dir: *dir})
	if err != nil {
		fatal("open writer: %v", err)
	}
	defer w.Close()

	f, err := os.Open(*src)
	if err != nil {
		fatal("open src: %v", err)
	}
	defer f.Close()

	type entry struct {
		Idx     uint64 `json:"idx"`
		Payload []byte `json:"payload"`
	}
	dec := json.NewDecoder(f)
	count := 0
	for dec.More() {
		var e entry
		if err := dec.Decode(&e); err != nil {
			fatal("decode line %d: %v", count+1, err)
		}
		if _, err := w.Append(e.Payload); err != nil {
			fatal("append: %v", err)
		}
		count++
	}
	fmt.Printf("restored %d records into %s (lastIndex=%d)\n", count, *dir, w.LastIndex())
}

// ---------------- compact ----------------

func cmdCompact(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	dir := fs.String("dir", "", "WAL directory")
	rng := fs.String("range", "", "compact this range, e.g. 1-1000 (must align to existing segment boundaries)")
	dryRun := fs.Bool("dry-run", false, "show what would happen, don't modify")
	fs.Parse(args)
	if *dir == "" || *rng == "" {
		fatal("compact: both -dir and -range required")
	}
	parts := strings.SplitN(*rng, "-", 2)
	if len(parts) != 2 {
		fatal("compact: -range must be F-L")
	}
	wantFirst, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		fatal("bad first: %v", err)
	}
	wantLast, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		fatal("bad last: %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(*dir, "wal-*"))
	if err != nil {
		fatal("glob: %v", err)
	}
	type seg struct {
		path  string
		first uint64
		last  uint64
		zst   bool
	}
	var contained []seg
	for _, p := range matches {
		base := filepath.Base(p)
		if strings.HasSuffix(base, ".archived") || strings.HasSuffix(base, ".meta") {
			continue
		}
		f, l, closed, zst, ok := parseSeg(base)
		if !ok || !closed {
			continue
		}
		if f >= wantFirst && l <= wantLast {
			contained = append(contained, seg{p, f, l, zst})
		}
	}
	if len(contained) < 2 {
		fatal("compact: need at least 2 contained segments in %s; got %d", *rng, len(contained))
	}
	sort.Slice(contained, func(i, j int) bool { return contained[i].first < contained[j].first })
	// Verify chain has no gaps.
	for i := 1; i < len(contained); i++ {
		if contained[i].first != contained[i-1].last+1 {
			fatal("compact: gap between %d-%d and %d-%d (refusing)",
				contained[i-1].first, contained[i-1].last, contained[i].first, contained[i].last)
		}
	}
	// Verify range matches exactly.
	if contained[0].first != wantFirst || contained[len(contained)-1].last != wantLast {
		fatal("compact: contained segments cover %d-%d, not %d-%d (refusing)",
			contained[0].first, contained[len(contained)-1].last, wantFirst, wantLast)
	}

	fmt.Printf("would merge %d segments into wal-%d-%d:\n", len(contained), wantFirst, wantLast)
	for _, s := range contained {
		fmt.Printf("  %s\n", filepath.Base(s.path))
	}
	if *dryRun {
		return
	}

	// Read all records from contained segments and write into a temp file in
	// the same dir. After the temp is durable, atomically rename it over the
	// final name and remove the source segments.
	tmpPath := filepath.Join(*dir, fmt.Sprintf(".compact-%d-%d.tmp", wantFirst, wantLast))
	out, err := os.Create(tmpPath)
	if err != nil {
		fatal("create tmp: %v", err)
	}
	for _, s := range contained {
		buf, err := readSegmentRaw(s.path)
		if err != nil {
			out.Close()
			os.Remove(tmpPath)
			fatal("read %s: %v", s.path, err)
		}
		if _, err := out.Write(buf); err != nil {
			out.Close()
			os.Remove(tmpPath)
			fatal("write tmp: %v", err)
		}
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(tmpPath)
		fatal("sync tmp: %v", err)
	}
	if err := out.Close(); err != nil {
		os.Remove(tmpPath)
		fatal("close tmp: %v", err)
	}

	finalName := fmt.Sprintf("wal-%d-%d", wantFirst, wantLast)
	finalPath := filepath.Join(*dir, finalName)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		fatal("rename: %v", err)
	}
	for _, s := range contained {
		if filepath.Base(s.path) == finalName {
			continue // we just wrote into one of the source paths; keep
		}
		if err := os.Remove(s.path); err != nil {
			fmt.Fprintf(os.Stderr, "warn: remove %s: %v\n", s.path, err)
		}
		// Drop sidecars too.
		base := fmt.Sprintf("wal-%d-%d", s.first, s.last)
		_ = os.Remove(filepath.Join(*dir, base+".archived"))
		_ = os.Remove(filepath.Join(*dir, base+".meta"))
	}
	fmt.Printf("compacted into %s\n", finalName)
}

// readSegmentRaw returns the BYTES of all valid frames in a closed segment,
// transparently decompressing .zst. Used by compact to rebuild a merged file.
func readSegmentRaw(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if strings.HasSuffix(path, ".zst") {
		dec, err := zstd.NewReader(f)
		if err != nil {
			return nil, err
		}
		defer dec.Close()
		return io.ReadAll(dec)
	}
	return io.ReadAll(f)
}

// ---------------- fsck ----------------

func cmdFsck(args []string) {
	fs := flag.NewFlagSet("fsck", flag.ExitOnError)
	dir := fs.String("dir", "", "WAL directory")
	fix := fs.Bool("fix", false, "actually delete bad segments / dedupe twins (otherwise dry-run)")
	fs.Parse(args)
	if *dir == "" {
		fatal("fsck: -dir required")
	}

	matches, err := filepath.Glob(filepath.Join(*dir, "wal-*"))
	if err != nil {
		fatal("glob: %v", err)
	}
	type key struct{ f, l uint64 }
	pairs := map[key][]string{}
	type seg struct {
		path string
		f, l uint64
	}
	var allClosed []seg

	for _, p := range matches {
		b := filepath.Base(p)
		if strings.HasSuffix(b, ".archived") || strings.HasSuffix(b, ".meta") {
			continue
		}
		f, l, closed, _, ok := parseSeg(b)
		if !ok || !closed {
			continue
		}
		pairs[key{f, l}] = append(pairs[key{f, l}], p)
		allClosed = append(allClosed, seg{p, f, l})
	}

	// Twin detection.
	for k, ps := range pairs {
		if len(ps) <= 1 {
			continue
		}
		// Prefer .zst.
		keep := ps[0]
		for _, p := range ps {
			if strings.HasSuffix(p, ".zst") {
				keep = p
				break
			}
		}
		for _, p := range ps {
			if p == keep {
				continue
			}
			fmt.Printf("DUP   %s (twin of %s)", filepath.Base(p), filepath.Base(keep))
			if *fix {
				if err := os.Remove(p); err != nil {
					fmt.Printf("  remove FAILED: %v\n", err)
				} else {
					fmt.Printf("  removed\n")
				}
			} else {
				fmt.Printf("  (would remove with --fix; range %d-%d)\n", k.f, k.l)
			}
		}
	}

	// CRC walk.
	bad := 0
	for _, s := range allClosed {
		count, err := walkSegment(s.path)
		if err != nil {
			bad++
			fmt.Printf("BAD   %-30s %d records before %v", filepath.Base(s.path), count, err)
			if *fix {
				if rmErr := os.Remove(s.path); rmErr != nil {
					fmt.Printf("  remove FAILED: %v\n", rmErr)
				} else {
					fmt.Printf("  removed\n")
				}
			} else {
				fmt.Printf("  (would remove with --fix)\n")
			}
		}
	}

	// Gap detection.
	sort.Slice(allClosed, func(i, j int) bool { return allClosed[i].f < allClosed[j].f })
	var prev uint64
	for i, s := range allClosed {
		if i == 0 {
			if s.f != 1 {
				fmt.Printf("GAP   chain doesn't start at 1 (first segment %s)\n", filepath.Base(s.path))
			}
		} else if s.f != prev+1 {
			fmt.Printf("GAP   between %d and %d (missing %d..%d)\n", prev, s.f, prev+1, s.f-1)
		}
		prev = s.l
	}

	if bad == 0 {
		fmt.Println("ok")
	} else {
		fmt.Printf("%d bad segment(s)\n", bad)
		if !*fix {
			os.Exit(1)
		}
	}
}

// ---------------- shared helpers ----------------

func parseSeg(name string) (first, last uint64, closed, zst, ok bool) {
	// name pattern: wal-F-[L][.zst]
	if !strings.HasPrefix(name, "wal-") {
		return 0, 0, false, false, false
	}
	rest := strings.TrimPrefix(name, "wal-")
	zst = strings.HasSuffix(rest, ".zst")
	rest = strings.TrimSuffix(rest, ".zst")
	parts := strings.SplitN(rest, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false, false, false
	}
	first, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || first == 0 {
		return 0, 0, false, false, false
	}
	if parts[1] == "" {
		return first, 0, false, zst, true
	}
	last, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false, false, false
	}
	return first, last, true, zst, true
}

// readFrame mirrors the package-internal record framing for verify.
func readFrame(r io.Reader) (int, error) {
	var hdr [8]byte
	n, err := io.ReadFull(r, hdr[:])
	if err == io.EOF && n == 0 {
		return 0, io.EOF
	}
	if err != nil {
		return 0, fmt.Errorf("truncated header: %w", err)
	}
	length := uint32(hdr[0])<<24 | uint32(hdr[1])<<16 | uint32(hdr[2])<<8 | uint32(hdr[3])
	wantCRC := uint32(hdr[4])<<24 | uint32(hdr[5])<<16 | uint32(hdr[6])<<8 | uint32(hdr[7])
	const maxRecord = 256 << 20
	if length > maxRecord {
		return 0, fmt.Errorf("oversized declared length %d", length)
	}
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, fmt.Errorf("truncated payload: %w", err)
		}
	}
	got := crc32cChecksum(payload)
	if got != wantCRC {
		return 0, fmt.Errorf("CRC mismatch: want %08x got %08x", wantCRC, got)
	}
	return int(length), nil
}

// crc32cChecksum has its own helper to avoid importing the package's internal
// table (it's idiomatic to use hash/crc32.MakeTable(crc32.Castagnoli)).
var crc32cTable = makeCrc32cTable()

func makeCrc32cTable() *[256]uint32 {
	const poly = 0x82f63b78
	var t [256]uint32
	for i := 0; i < 256; i++ {
		c := uint32(i)
		for j := 0; j < 8; j++ {
			if c&1 != 0 {
				c = (c >> 1) ^ poly
			} else {
				c >>= 1
			}
		}
		t[i] = c
	}
	return &t
}

func crc32cChecksum(b []byte) uint32 {
	c := uint32(0xFFFFFFFF)
	for _, x := range b {
		c = (c >> 8) ^ crc32cTable[byte(c)^x]
	}
	return c ^ 0xFFFFFFFF
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(n)/(1<<20))
	case n >= 1024:
		return fmt.Sprintf("%.1fKB", float64(n)/1024)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func formatAge(secs int64) string {
	if secs < 60 {
		return fmt.Sprintf("%ds", secs)
	}
	if secs < 3600 {
		return fmt.Sprintf("%dm%ds", secs/60, secs%60)
	}
	return fmt.Sprintf("%dh%dm", secs/3600, (secs%3600)/60)
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

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}
