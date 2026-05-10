package axqcompat

import (
	"log/slog"

	"github.com/golang/protobuf/proto"
)

// message is the adapter type that satisfies github.com/axgrid/axq/domain.Message.
//
// Done() routes back to the wrapped ax-mysql-wal Reader's Ack — at-least-once
// semantics are preserved. Error() is a logging hook only: ax-mysql-wal has
// no built-in dead-letter / retry queue, so a record that the consumer
// failed to process simply isn't ack'd; on the next reader run, the cursor
// resumes at the failed index and re-delivers it.
type message struct {
	id      uint64
	payload []byte
	ack     func(uint64) error // bound to Reader.Ack at construction
	log     *slog.Logger       // used by Error(); nil falls back to slog.Default
}

// Id returns the record's monotonic index.
func (m *message) Id() uint64 { return m.id }

// Message returns the raw record payload bytes (zero-copy view of the
// reader's prefetch buffer; do NOT retain past the next Pop).
func (m *message) Message() []byte { return m.payload }

// Done ack's the record. axq's API ignores the result; we mirror that, but
// log Ack errors so they're visible to operators.
func (m *message) Done() {
	if m.ack == nil {
		return
	}
	if err := m.ack(m.id); err != nil {
		log := m.log
		if log == nil {
			log = slog.Default()
		}
		log.Warn("axqcompat: ack failed", "id", m.id, "err", err)
	}
}

// Error records that the consumer failed to process this message. Because
// we do NOT call ack(), the underlying ax-mysql-wal cursor stays put and
// the next Reader will replay the record. The supplied err is logged.
func (m *message) Error(err error) {
	log := m.log
	if log == nil {
		log = slog.Default()
	}
	log.Warn("axqcompat: message error (will be re-delivered on next reader run)",
		"id", m.id, "err", err)
}

// UnmarshalProto decodes the payload into v using the legacy proto package
// (matching what axq itself uses; modern google.golang.org/protobuf is
// wire-compatible if your generated types target both).
func (m *message) UnmarshalProto(v proto.Message) error {
	return proto.Unmarshal(m.payload, v)
}
