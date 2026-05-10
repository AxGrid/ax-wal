// Package axqcompat lets a github.com/axgrid/ax-wal Writer/Reader pair be
// used wherever the legacy github.com/axgrid/axq Writer/Reader interfaces
// are expected. Old axq consumers can drop their wiring code in front of an
// ax-wal pair without touching call sites.
//
// Field semantics:
//
//	axq concept             ax-wal mapping
//	-------                 --------------
//	domain.Message.Id()     record index (sequential, monotonic)
//	domain.Message.Done()   Reader.Ack(idx)
//	domain.Message.Error()  no-op (logs); the record stays un-acked and is
//	                        re-delivered after the cursor's AckBatchInterval
//	Writer.LastFID()        first index F of the currently-open wal-F- segment
//	Writer.LastID()         Writer.LastIndex()
//	Writer.MinimalFID()     smallest F across segments still on disk
//	Writer.MinimalID()      first index of the oldest segment (= MinimalFID)
//	Writer.Counter()        Writer.Stats().RecordsAppended
//	Writer.Performance()    rolling 1-second rec/s rate
//	Reader.LastFID()        F of newest open/closed segment in the dir
//	Reader.LastID()         Reader.HeadIndex()
//	Reader.Counter()        Reader.Stats().RecordsDelivered
//
// FID semantics in ax-wal: ax-wal segments are named wal-F-L (closed) or
// wal-F- (open). F is the first record index in that segment file, so it
// uniquely identifies a segment AND tells you where its records start.
// That's the closest analogue to axq's BlobFID, which keys on a row id but
// also bounds an index range — same role, different storage.
//
// This package lives in its own sub-module so that the heavy axq deps
// (gorm, zerolog, uuid, b2) don't pollute the lean ax-wal core.
package axqcompat
