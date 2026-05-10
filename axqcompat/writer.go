package axqcompat

import (
	"errors"
	"fmt"

	axwal "github.com/axgrid/ax-wal"
	"github.com/axgrid/axq/domain"
	"github.com/golang/protobuf/proto"
)

// WriterOptions wires an already-opened ax-wal Writer into the axq surface.
type WriterOptions struct {
	// Writer is the live ax-wal writer. Required.
	Writer *axwal.Writer

	// Dir is the WAL directory (matches the dir the underlying Writer was
	// opened on). Required for LastFID() / MinimalFID() — we read the dir
	// at call time to identify segment boundaries. Pass "" to disable
	// (those methods will return 0 with no error).
	Dir string

	// Name is the axq writer name, returned by GetName() and reflected in
	// GetOpts(). Defaults to "axwal" when empty.
	Name string

	// CloseUnderlying controls whether the adapter's Close() also closes
	// the wrapped ax-wal Writer. Default true.
	CloseUnderlying bool
}

// Writer satisfies github.com/axgrid/axq.Writer. Construct via NewWriter.
type Writer struct {
	w               *axwal.Writer
	dir             string
	name            string
	opts            domain.WriterOptions
	perf            *rateCounter
	closeUnderlying bool
}

// NewWriter wraps an existing ax-wal Writer.
func NewWriter(opts WriterOptions) (*Writer, error) {
	if opts.Writer == nil {
		return nil, errors.New("axqcompat: WriterOptions.Writer required")
	}
	if opts.Name == "" {
		opts.Name = "axwal"
	}
	return &Writer{
		w:    opts.Writer,
		dir:  opts.Dir,
		name: opts.Name,
		opts: domain.WriterOptions{
			BaseOptions: domain.BaseOptions{Name: opts.Name},
		},
		perf:            newRateCounter(),
		closeUnderlying: opts.CloseUnderlying,
	}, nil
}

// Push appends one record.
func (a *Writer) Push(message []byte) error {
	if _, err := a.w.Append(message); err != nil {
		return err
	}
	a.perf.Inc()
	return nil
}

// PushMany appends N records as one atomic batch.
func (a *Writer) PushMany(messages [][]byte) error {
	if len(messages) == 0 {
		return nil
	}
	if _, err := a.w.AppendBatch(messages); err != nil {
		return err
	}
	a.perf.Add(uint64(len(messages)))
	return nil
}

// PushProto marshals v (legacy proto package) and pushes it.
func (a *Writer) PushProto(message proto.Message) error {
	b, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("axqcompat: marshal proto: %w", err)
	}
	return a.Push(b)
}

// PushProtoMany marshals all and submits as one batch.
func (a *Writer) PushProtoMany(messages []proto.Message) error {
	if len(messages) == 0 {
		return nil
	}
	out := make([][]byte, len(messages))
	for i, m := range messages {
		b, err := proto.Marshal(m)
		if err != nil {
			return fmt.Errorf("axqcompat: marshal proto[%d]: %w", i, err)
		}
		out[i] = b
	}
	return a.PushMany(out)
}

// Close stops the perf counter and (if CloseUnderlying was true) closes the
// wrapped ax-wal Writer.
func (a *Writer) Close() {
	a.perf.Stop()
	if a.closeUnderlying {
		_ = a.w.Close()
	}
}

// GetOpts returns a domain.WriterOptions whose ServiceOpts methods reflect
// this adapter's name. Other fields are zero-valued.
func (a *Writer) GetOpts() domain.ServiceOpts { return &a.opts }

// GetName returns the configured axq writer name.
func (a *Writer) GetName() string { return a.name }

// Counter returns the cumulative records appended.
func (a *Writer) Counter() (uint64, error) {
	return a.w.Stats().AppendRecords, nil
}

// LastFID returns the F (first index) of the currently-open segment in the
// WAL dir. When no segments exist yet (fresh writer that hasn't rotated)
// the largest closed segment's F is returned, or 0 if the dir is empty.
func (a *Writer) LastFID() (uint64, error) {
	if a.dir == "" {
		return 0, nil
	}
	_, max, err := minMaxSegmentFID(a.dir)
	return max, err
}

// LastID returns the index of the most recent record.
func (a *Writer) LastID() (uint64, error) {
	return a.w.LastIndex(), nil
}

// MinimalFID returns the F of the OLDEST segment still on disk (after
// retention has trimmed earlier segments).
func (a *Writer) MinimalFID() (uint64, error) {
	if a.dir == "" {
		return 0, nil
	}
	min, _, err := minMaxSegmentFID(a.dir)
	return min, err
}

// MinimalID returns the smallest record index still on disk. With ax-wal's
// segment naming, this is identical to MinimalFID — F is the first index
// of its segment, and the oldest segment's F is the smallest live index.
func (a *Writer) MinimalID() (uint64, error) {
	return a.MinimalFID()
}

// Performance returns the running rec/s rate.
func (a *Writer) Performance() uint64 {
	return a.perf.Rate()
}
