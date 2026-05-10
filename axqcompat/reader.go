package axqcompat

import (
	"context"
	"errors"
	"log/slog"

	axwal "github.com/axgrid/ax-wal"
	"github.com/axgrid/axq/domain"
)

// ReaderOptions wires an already-opened ax-wal Reader into the axq surface.
type ReaderOptions struct {
	// Reader is the live ax-wal reader. Required.
	Reader *axwal.Reader

	// Dir is the WAL directory the reader opens against. Required for
	// LastFID() (we list segments to find the newest F).
	Dir string

	// Name is the axq reader name (returned by GetName).
	Name string

	// WriterName is the axq producer name returned by GetWriterName().
	WriterName string

	// BufferSize sizes the chan used by C() / Pop(). Default 1024.
	BufferSize int

	// Ctx, when set, scopes the reader's lifetime — Pop returns nil and
	// C() closes once Ctx is cancelled.
	Ctx context.Context

	// Logger receives slog records. Defaults to slog.Default.
	Logger *slog.Logger

	// CloseUnderlying controls whether Reader.Close() (the adapter's)
	// also closes the wrapped ax-wal Reader. Default true.
	CloseUnderlying bool
}

// Reader satisfies github.com/axgrid/axq.Reader. Construct via NewReader.
type Reader struct {
	r          *axwal.Reader
	dir        string
	name       string
	writerName string
	opts       domain.ReaderOptions
	perf       *rateCounter
	log        *slog.Logger

	pumpCtx    context.Context
	pumpCancel context.CancelFunc

	ch       chan domain.Message
	pumpDone chan struct{}

	closeUnderlying bool
}

// NewReader wraps an existing ax-wal Reader and starts the pump goroutine.
func NewReader(opts ReaderOptions) (*Reader, error) {
	if opts.Reader == nil {
		return nil, errors.New("axqcompat: ReaderOptions.Reader required")
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = 1024
	}
	if opts.Name == "" {
		opts.Name = "axwal-reader"
	}
	if opts.WriterName == "" {
		opts.WriterName = "axwal"
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	parent := opts.Ctx
	if parent == nil {
		parent = context.Background()
	}
	pumpCtx, pumpCancel := context.WithCancel(parent)

	a := &Reader{
		r:          opts.Reader,
		dir:        opts.Dir,
		name:       opts.Name,
		writerName: opts.WriterName,
		opts: domain.ReaderOptions{
			BaseOptions: domain.BaseOptions{Name: opts.Name},
			ReaderName:  opts.WriterName,
		},
		perf:            newRateCounter(),
		log:             opts.Logger,
		pumpCtx:         pumpCtx,
		pumpCancel:      pumpCancel,
		ch:              make(chan domain.Message, opts.BufferSize),
		pumpDone:        make(chan struct{}),
		closeUnderlying: opts.CloseUnderlying,
	}
	go a.pump()
	return a, nil
}

// pump is the only goroutine that calls the underlying Reader.Next.
func (a *Reader) pump() {
	defer close(a.pumpDone)
	defer close(a.ch)
	for {
		payload, idx, err := a.r.Next(a.pumpCtx)
		if err != nil {
			return
		}
		msg := &message{
			id:      idx,
			payload: payload,
			ack:     a.r.Ack,
			log:     a.log,
		}
		select {
		case <-a.pumpCtx.Done():
			return
		case a.ch <- msg:
			a.perf.Inc()
		}
	}
}

// Pop blocks until the next message is available; returns nil on shutdown.
func (a *Reader) Pop() domain.Message {
	msg, ok := <-a.ch
	if !ok {
		return nil
	}
	return msg
}

// C returns the underlying message channel. Closed when the reader stops.
func (a *Reader) C() <-chan domain.Message { return a.ch }

// GetOpts returns a domain.ReaderOptions reflecting our adapter's name.
func (a *Reader) GetOpts() domain.ServiceOpts { return &a.opts }

// GetName returns the axq reader name.
func (a *Reader) GetName() string { return a.name }

// GetWriterName returns the configured producer name.
func (a *Reader) GetWriterName() string { return a.writerName }

// Counter returns the cumulative records delivered. ax-wal exposes
// NextCalls (one per Reader.Next) which is the closest analogue.
func (a *Reader) Counter() (uint64, error) {
	return a.r.Stats().NextCalls, nil
}

// LastFID returns the largest F across all segments in the WAL dir (i.e.
// the F of the currently-open segment, or — if there's no open segment —
// the F of the most recent closed one).
func (a *Reader) LastFID() (uint64, error) {
	if a.dir == "" {
		return 0, nil
	}
	_, max, err := minMaxSegmentFID(a.dir)
	return max, err
}

// LastID returns the head index of the WAL.
func (a *Reader) LastID() (uint64, error) {
	return a.r.HeadIndex()
}

// Performance returns the rec/s the adapter has handed out in the last
// 1-second window.
func (a *Reader) Performance() uint64 {
	return a.perf.Rate()
}

// Close stops the pump, drains the perf counter, and (if configured)
// closes the wrapped ax-wal Reader.
func (a *Reader) Close() error {
	a.pumpCancel()
	<-a.pumpDone
	a.perf.Stop()
	if a.closeUnderlying {
		return a.r.Close()
	}
	return nil
}
