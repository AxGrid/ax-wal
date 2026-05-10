// producer is a tiny demo: appends N messages to a WAL with fsync per record.
//
//	go run ./cmd/producer -dir /tmp/axwal -n 1000
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	axwal "github.com/axgrid/ax-wal"
)

func main() {
	dir := flag.String("dir", "/tmp/axwal", "WAL directory")
	n := flag.Int("n", 100, "number of records to append")
	delay := flag.Duration("delay", 0, "sleep between appends")
	maxSeg := flag.Int64("max-segment", 1<<20, "max segment size bytes")
	compress := flag.Bool("compress", false, "zstd-compress rotated segments")
	flag.Parse()

	w, err := axwal.OpenWriter(axwal.WriterOptions{
		Dir:              *dir,
		MaxSegmentSize:   *maxSeg,
		CompressOnRotate: *compress,
		Retention: axwal.RetentionConfig{
			Enabled:          true,
			Interval:         5 * time.Second,
			MinRetainRecords: 1000,
			StaleReaderTTL:   60 * time.Second,
		},
	})
	if err != nil {
		log.Fatalf("open writer: %v", err)
	}
	defer w.Close()

	t0 := time.Now()
	for i := 0; i < *n; i++ {
		payload := []byte(fmt.Sprintf("msg-%d ts=%d", i, time.Now().UnixNano()))
		idx, err := w.Append(payload)
		if err != nil {
			log.Fatalf("append: %v", err)
		}
		if i%100 == 0 {
			log.Printf("appended idx=%d", idx)
		}
		if *delay > 0 {
			time.Sleep(*delay)
		}
	}
	log.Printf("done: %d records in %s (last index %d)", *n, time.Since(t0), w.LastIndex())
}
