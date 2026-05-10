// axwal-bench is a load tester / benchmark harness for the WAL.
//
// Single mode (one payload size):
//
//	axwal-bench -dir /tmp/wal -duration 10s -writers 4 -size 1024
//
// Suite mode (100B / 1KB / 10KB / 100KB / 1MB sequentially):
//
//	axwal-bench -suite -duration 5s -writers 4
//
// Each record carries [u64 seq | i64 unix_nanos | filler]. The reader extracts
// the timestamp and computes end-to-end (write → read) latency.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	axwal "github.com/axgrid/ax-wal"
)

const (
	headerSize = 16 // 8 bytes seq + 8 bytes nanos
)

type runConfig struct {
	dir              string
	duration         time.Duration
	writers          int
	payloadSize      int
	groupCommit      bool
	gcLatency        time.Duration
	compress         bool
	maxSegmentSize   int64
	maxSegmentRecs   uint64
	requireArchived  bool
	ackEvery         uint64        // call Ack only every N records (1 = per record)
	ackBatch         time.Duration // ReaderOptions.AckBatchInterval
	syncMode         axwal.SyncMode
}

type result struct {
	cfg          runConfig
	totalWritten uint64
	totalRead    uint64
	bytesOnDisk  int64
	writeLat     []int64 // ns
	readLat      []int64 // ns (end-to-end)
	rotations    int
	elapsed      time.Duration
}

func main() {
	dir := flag.String("dir", "", "WAL directory (default: temp)")
	duration := flag.Duration("duration", 10*time.Second, "run length")
	writers := flag.Int("writers", 4, "concurrent writer goroutines")
	size := flag.Int("size", 1024, "payload size in bytes (single mode)")
	suite := flag.Bool("suite", false, "run 100B/1KB/10KB/100KB/1MB sequentially")
	groupCommit := flag.Bool("group-commit", false, "enable group commit (batched fsync)")
	gcLatency := flag.Duration("gc-latency", time.Millisecond, "group-commit max latency")
	compress := flag.Bool("compress", false, "zstd-compress rotated segments")
	maxSegSize := flag.Int64("max-segment-size", 64<<20, "max segment size before rotation (bytes)")
	maxSegRecs := flag.Uint64("max-segment-records", 0, "max records per segment (0=disabled)")
	requireArchived := flag.Bool("require-archived", false, "block deletion until .archived sidecar present")
	ackEvery := flag.Uint64("ack-every", 1, "Ack the cursor only every N records (1 = strict at-least-once, higher = faster reads at risk of replay window)")
	ackBatch := flag.Duration("ack-batch", 0, "ReaderOptions.AckBatchInterval (0=disabled, e.g. 10ms)")
	syncMode := flag.String("sync-mode", "full", "writer durability: full | barrier | none")
	replayCount := flag.Uint64("replay-count", 0, "REPLAY MODE: pre-populate this many records, then time a fresh reader draining them all (no concurrent writer)")
	flag.Parse()

	var sm axwal.SyncMode
	switch *syncMode {
	case "full":
		sm = axwal.SyncFull
	case "barrier":
		sm = axwal.SyncBarrier
	case "none":
		sm = axwal.SyncNone
	default:
		log.Fatalf("invalid -sync-mode %q (full|barrier|none)", *syncMode)
	}

	if *size < headerSize {
		log.Fatalf("payload size must be >= %d to fit header", headerSize)
	}

	sizes := []int{*size}
	if *suite {
		sizes = []int{100, 1024, 10 * 1024, 100 * 1024, 1024 * 1024}
	}

	var results []result
	for _, sz := range sizes {
		runDir := *dir
		if runDir == "" {
			runDir = filepath.Join(os.TempDir(), fmt.Sprintf("axwal-bench-%d-%d", os.Getpid(), time.Now().UnixNano()))
		} else if *suite {
			runDir = filepath.Join(*dir, fmt.Sprintf("size-%d", sz))
		}
		if err := os.MkdirAll(runDir, 0755); err != nil {
			log.Fatalf("mkdir %s: %v", runDir, err)
		}

		cfg := runConfig{
			dir:             runDir,
			duration:        *duration,
			writers:         *writers,
			payloadSize:     sz,
			groupCommit:     *groupCommit,
			gcLatency:       *gcLatency,
			compress:        *compress,
			maxSegmentSize:  *maxSegSize,
			maxSegmentRecs:  *maxSegRecs,
			requireArchived: *requireArchived,
			ackEvery:        *ackEvery,
			ackBatch:        *ackBatch,
			syncMode:        sm,
		}
		var r result
		if *replayCount > 0 {
			log.Printf("=== replay: size=%s count=%d ack-every=%d dir=%s",
				humanBytes(int64(sz)), *replayCount, cfg.ackEvery, cfg.dir)
			r = runReplay(cfg, *replayCount)
			printReplayResult(r)
		} else {
			log.Printf("=== run: size=%s writers=%d group-commit=%v duration=%s dir=%s",
				humanBytes(int64(sz)), cfg.writers, cfg.groupCommit, cfg.duration, cfg.dir)
			r = runOne(cfg)
			printResult(r)
		}
		results = append(results, r)
	}

	if *suite {
		fmt.Println()
		fmt.Println(suiteSummary(results))
	}
}

func runOne(cfg runConfig) result {
	w, err := axwal.OpenWriter(axwal.WriterOptions{
		Dir:               cfg.dir,
		MaxSegmentSize:    cfg.maxSegmentSize,
		MaxSegmentRecords: cfg.maxSegmentRecs,
		CompressOnRotate:  cfg.compress,
		SyncMode:          cfg.syncMode,
		GroupCommit: axwal.GroupCommitConfig{
			Enabled:    cfg.groupCommit,
			MaxLatency: cfg.gcLatency,
		},
		Retention: axwal.RetentionConfig{
			Enabled:             cfg.requireArchived,
			Interval:            5 * time.Second,
			RequireArchivedMark: cfg.requireArchived,
		},
	})
	if err != nil {
		log.Fatalf("open writer: %v", err)
	}
	defer w.Close()

	r, err := axwal.OpenReader(axwal.ReaderOptions{
		Dir:               cfg.dir,
		Name:              "bench",
		StartFrom:         axwal.StartEarliest,
		HeartbeatInterval: time.Second,
		AckBatchInterval:  cfg.ackBatch,
		PollInterval:      5 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("open reader: %v", err)
	}
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	var (
		totalWritten atomic.Uint64
		totalRead    atomic.Uint64
		writeLatMu   sync.Mutex
		writeLat     []int64
		readLatMu    sync.Mutex
		readLat      []int64
	)

	// ----- writers -----
	var writersWG sync.WaitGroup
	for i := 0; i < cfg.writers; i++ {
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			payload := make([]byte, cfg.payloadSize)
			myLat := make([]int64, 0, 4096)
			for ctx.Err() == nil {
				seq := totalWritten.Add(1)
				binary.BigEndian.PutUint64(payload[0:8], seq)
				ts := time.Now().UnixNano()
				binary.BigEndian.PutUint64(payload[8:16], uint64(ts))

				start := time.Now()
				_, err := w.Append(payload)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("append: %v", err)
					return
				}
				myLat = append(myLat, time.Since(start).Nanoseconds())
			}
			writeLatMu.Lock()
			writeLat = append(writeLat, myLat...)
			writeLatMu.Unlock()
		}()
	}

	// ----- reader -----
	readerDone := make(chan struct{})
	readCtx, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()
	go func() {
		defer close(readerDone)
		myLat := make([]int64, 0, 65536)
		var lastAcked uint64
		readSince := uint64(0)
		ackEvery := cfg.ackEvery
		if ackEvery == 0 {
			ackEvery = 1
		}
		for {
			payload, idx, err := r.Next(readCtx)
			if err != nil {
				if lastAcked > 0 {
					_ = r.Ack(lastAcked) // best-effort flush of last index
				}
				readLatMu.Lock()
				readLat = append(readLat, myLat...)
				readLatMu.Unlock()
				return
			}
			if len(payload) >= headerSize {
				ts := int64(binary.BigEndian.Uint64(payload[8:16]))
				myLat = append(myLat, time.Now().UnixNano()-ts)
			}
			lastAcked = idx
			readSince++
			if readSince >= ackEvery {
				if err := r.Ack(idx); err != nil {
					log.Printf("ack: %v", err)
					return
				}
				readSince = 0
			}
			totalRead.Add(1)
		}
	}()

	// ----- live stats -----
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		var prevW, prevR uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				w := totalWritten.Load()
				r := totalRead.Load()
				log.Printf("  ... w=%d (+%d/s)  r=%d (+%d/s)  lag=%d", w, w-prevW, r, r-prevR, w-r)
				prevW, prevR = w, r
			}
		}
	}()

	start := time.Now()
	writersWG.Wait()
	activeElapsed := time.Since(start)
	<-statsDone

	// Drain phase: let reader catch up briefly, but cap. Latencies recorded
	// during drain count toward end-to-end latency stats; rates do not.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	drainTick := time.NewTicker(50 * time.Millisecond)
	defer drainTick.Stop()
DrainLoop:
	for {
		select {
		case <-drainCtx.Done():
			break DrainLoop
		case <-drainTick.C:
			if totalRead.Load() >= totalWritten.Load() {
				break DrainLoop
			}
		}
	}
	cancelRead()
	<-readerDone

	res := result{
		cfg:          cfg,
		totalWritten: totalWritten.Load(),
		totalRead:    totalRead.Load(),
		writeLat:     writeLat,
		readLat:      readLat,
		elapsed:      activeElapsed,
	}
	res.bytesOnDisk, res.rotations = inspectDir(cfg.dir)
	return res
}

func printResult(r result) {
	wRate := float64(r.totalWritten) / r.elapsed.Seconds()
	rRate := float64(r.totalRead) / r.elapsed.Seconds()
	throughput := wRate * float64(r.cfg.payloadSize) / (1 << 20)

	wp50, wp95, wp99 := percentiles(r.writeLat)
	rp50, rp95, rp99 := percentiles(r.readLat)

	fmt.Printf("\n--- size=%s writers=%d group-commit=%v ---\n",
		humanBytes(int64(r.cfg.payloadSize)), r.cfg.writers, r.cfg.groupCommit)
	fmt.Printf("writes:        %10s/s  (%.2f MB/s, %d total)\n", commaSep(int64(wRate)), throughput, r.totalWritten)
	fmt.Printf("reads:         %10s/s  (%d total, lag at end = %d)\n", commaSep(int64(rRate)), r.totalRead, int64(r.totalWritten)-int64(r.totalRead))
	fmt.Printf("write latency: p50=%9s  p95=%9s  p99=%9s\n", humanDur(wp50), humanDur(wp95), humanDur(wp99))
	fmt.Printf("read latency:  p50=%9s  p95=%9s  p99=%9s   (write→read)\n", humanDur(rp50), humanDur(rp95), humanDur(rp99))
	fmt.Printf("disk:          %s across %d closed segments\n", humanBytes(r.bytesOnDisk), r.rotations)
}

// runReplay pre-populates the WAL with `count` records and then measures how
// fast a fresh reader can drain them all from Earliest.
func runReplay(cfg runConfig, count uint64) result {
	// Phase 1: produce N records as fast as we can. Use AppendBatch to amortize
	// fsync — replay benchmarks the READ path, not the write path.
	w, err := axwal.OpenWriter(axwal.WriterOptions{
		Dir:              cfg.dir,
		MaxSegmentSize:   cfg.maxSegmentSize,
		CompressOnRotate: cfg.compress,
		SyncMode:         cfg.syncMode,
		// No group commit for the prepopulate phase: we're doing one big
		// AppendBatch, which is already a single fsync.
	})
	if err != nil {
		log.Fatalf("open writer: %v", err)
	}

	const batchSize = 1024
	payload := make([]byte, cfg.payloadSize)
	prepStart := time.Now()
	for produced := uint64(0); produced < count; {
		take := batchSize
		if uint64(take) > count-produced {
			take = int(count - produced)
		}
		batch := make([][]byte, take)
		for i := 0; i < take; i++ {
			seq := produced + uint64(i) + 1
			binary.BigEndian.PutUint64(payload[0:8], seq)
			ts := time.Now().UnixNano()
			binary.BigEndian.PutUint64(payload[8:16], uint64(ts))
			rec := make([]byte, cfg.payloadSize)
			copy(rec, payload)
			batch[i] = rec
		}
		if _, err := w.AppendBatch(batch); err != nil {
			log.Fatalf("prepopulate AppendBatch: %v", err)
		}
		produced += uint64(take)
	}
	if err := w.Close(); err != nil {
		log.Fatalf("close writer: %v", err)
	}
	prepElapsed := time.Since(prepStart)
	log.Printf("  prepopulated %d records (%s) in %s = %.0f rec/s",
		count, humanBytes(int64(count)*int64(cfg.payloadSize)), prepElapsed,
		float64(count)/prepElapsed.Seconds())

	// Phase 2: open a FRESH reader from earliest, time draining everything.
	r, err := axwal.OpenReader(axwal.ReaderOptions{
		Dir:               cfg.dir,
		Name:              "replay",
		StartFrom:         axwal.StartEarliest,
		HeartbeatInterval: time.Second,
		AckBatchInterval:  cfg.ackBatch,
		PollInterval:      5 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("open reader: %v", err)
	}
	defer r.Close()

	ackEvery := cfg.ackEvery
	if ackEvery == 0 {
		ackEvery = 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	readLat := make([]int64, 0, count)
	readStart := time.Now()
	var lastAck uint64
	since := uint64(0)
	for read := uint64(0); read < count; read++ {
		recvStart := time.Now()
		_, idx, err := r.Next(ctx)
		if err != nil {
			log.Fatalf("Next at %d: %v", read, err)
		}
		readLat = append(readLat, time.Since(recvStart).Nanoseconds())
		lastAck = idx
		since++
		if since >= ackEvery {
			if err := r.Ack(idx); err != nil {
				log.Fatalf("Ack: %v", err)
			}
			since = 0
		}
	}
	if since > 0 {
		_ = r.Ack(lastAck)
	}
	readElapsed := time.Since(readStart)

	res := result{
		cfg:          cfg,
		totalWritten: count,
		totalRead:    count,
		readLat:      readLat,
		elapsed:      readElapsed,
	}
	res.bytesOnDisk, res.rotations = inspectDir(cfg.dir)
	return res
}

func printReplayResult(r result) {
	rate := float64(r.totalRead) / r.elapsed.Seconds()
	mb := rate * float64(r.cfg.payloadSize) / (1 << 20)
	rp50, rp95, rp99 := percentiles(r.readLat)
	fmt.Printf("\n--- REPLAY size=%s count=%d ack-every=%d ---\n",
		humanBytes(int64(r.cfg.payloadSize)), r.totalRead, r.cfg.ackEvery)
	fmt.Printf("drained:       %d records in %s\n", r.totalRead, r.elapsed)
	fmt.Printf("rate:          %s rec/s   (%.2f MB/s)\n", commaSep(int64(rate)), mb)
	fmt.Printf("Next() latency: p50=%s  p95=%s  p99=%s\n", humanDur(rp50), humanDur(rp95), humanDur(rp99))
	fmt.Printf("disk:          %s across %d closed segments\n", humanBytes(r.bytesOnDisk), r.rotations)
}

func suiteSummary(rs []result) string {
	out := "=== Suite summary ===\n"
	out += fmt.Sprintf("%-7s %12s %12s %10s %10s %10s %10s %10s %10s\n",
		"size", "writes/s", "MB/s", "wp50", "wp95", "wp99", "rp50", "rp95", "rp99")
	out += "----------------------------------------------------------------------------------------------------------------\n"
	for _, r := range rs {
		wRate := float64(r.totalWritten) / r.elapsed.Seconds()
		mb := wRate * float64(r.cfg.payloadSize) / (1 << 20)
		wp50, wp95, wp99 := percentiles(r.writeLat)
		rp50, rp95, rp99 := percentiles(r.readLat)
		out += fmt.Sprintf("%-7s %12s %12.2f %10s %10s %10s %10s %10s %10s\n",
			humanBytes(int64(r.cfg.payloadSize)),
			commaSep(int64(wRate)), mb,
			humanDur(wp50), humanDur(wp95), humanDur(wp99),
			humanDur(rp50), humanDur(rp95), humanDur(rp99))
	}
	return out
}

func percentiles(samples []int64) (p50, p95, p99 int64) {
	if len(samples) == 0 {
		return 0, 0, 0
	}
	sorted := make([]int64, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pick := func(p float64) int64 {
		idx := int(float64(len(sorted)-1) * p)
		return sorted[idx]
	}
	return pick(0.50), pick(0.95), pick(0.99)
}

func inspectDir(dir string) (int64, int) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0
	}
	var bytes int64
	var segs int
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		bytes += fi.Size()
		name := e.Name()
		if len(name) > 4 && name[:4] == "wal-" {
			segs++
		}
	}
	return bytes, segs
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(n)/(1<<30))
	case n >= 10<<20:
		return fmt.Sprintf("%.0fMB", float64(n)/(1<<20))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(n)/(1<<20))
	case n >= 1024:
		if n%1024 == 0 {
			return fmt.Sprintf("%dKB", n/1024)
		}
		return fmt.Sprintf("%.1fKB", float64(n)/1024)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func humanDur(ns int64) string {
	d := time.Duration(ns)
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	case d >= time.Microsecond:
		return fmt.Sprintf("%.0fµs", float64(d)/float64(time.Microsecond))
	default:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
}

func commaSep(n int64) string {
	if n < 0 {
		return "-" + commaSep(-n)
	}
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	return commaSep(n/1000) + fmt.Sprintf(",%03d", n%1000)
}
