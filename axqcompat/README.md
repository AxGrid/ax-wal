# axqcompat for ax-wal

Adapter that lets a `github.com/axgrid/ax-wal` Writer/Reader pair be used wherever the legacy `github.com/axgrid/axq` Writer/Reader interfaces are expected.

```go
import (
    axwal "github.com/axgrid/ax-wal"
    "github.com/axgrid/ax-wal/axqcompat"
    "github.com/axgrid/axq"
)

rawW, _ := axwal.OpenWriter(axwal.WriterOptions{Dir: "/var/lib/wal"})
var w axq.Writer = mustWriter(axqcompat.NewWriter(axqcompat.WriterOptions{
    Writer:          rawW,
    Dir:             "/var/lib/wal",   // needed for FID lookups
    Name:            "tx-producer",
    CloseUnderlying: true,
}))

w.Push(payload)
w.LastFID()        // F of the open segment (wal-{F}-)
w.MinimalFID()     // F of the oldest segment still on disk
w.Performance()    // rec/s rolling 1-second window
```

Reader side mirrors:

```go
rawR, _ := axwal.OpenReader(axwal.ReaderOptions{Dir: "/var/lib/wal", Name: "consumer"})
r, _ := axqcompat.NewReader(axqcompat.ReaderOptions{
    Reader: rawR, Dir: "/var/lib/wal",
    Name: "consumer", WriterName: "tx-producer",
    Ctx: ctx, CloseUnderlying: true,
})

for msg := range r.C() {
    if err := process(msg.Message()); err != nil {
        msg.Error(err)         // does NOT ack → replays on next run
        continue
    }
    msg.Done()                 // → Reader.Ack(idx)
}
```

## FID mapping

ax-wal segments are named `wal-{F}-{L}` (closed) or `wal-{F}-` (open). `F` is the first record index in that segment file — uniquely identifies it AND tells you the index range. axq's `BlobFID` plays the same role for MySQL rows; we reuse `F` as `FID`.

| axq surface | ax-wal mapping |
|---|---|
| `Push` / `PushMany` | `Append` / `AppendBatch` |
| `PushProto` / `PushProtoMany` | `proto.Marshal` then Push |
| `Counter()` (Writer) | `Stats().AppendRecords` |
| `LastFID()` (Writer) | F of currently-open segment (largest F in dir) |
| `LastID()` (Writer) | `LastIndex()` |
| `MinimalFID()` (Writer) | F of oldest segment still on disk |
| `MinimalID()` (Writer) | identical to `MinimalFID` (F is the segment's first index) |
| `Performance()` | rolling 1 s rec/s |
| `Pop()` / `C()` (Reader) | wraps `Reader.Next` in a buffered chan |
| `domain.Message.Done()` | `Reader.Ack(idx)` |
| `domain.Message.Error()` | logs warn; does NOT ack → next reader replays |
| `domain.Message.UnmarshalProto()` | `proto.Unmarshal` (legacy proto) |
| `LastFID()` (Reader) | largest F in dir (= newest segment) |
| `LastID()` (Reader) | `Reader.HeadIndex()` |
| `Counter()` (Reader) | `Stats().NextCalls` |

## Why a separate sub-module

axq pulls heavy deps (gorm, zerolog, b2, uuid, axtransform). Keeping the adapter in `axqcompat/` with its own `go.mod` means consumers of plain `ax-wal` don't drag any of that into their builds.

## Limitations

- `Counter()` and `Performance()` are zero / under-reported during the first 1 s after construction.
- `Error()` only logs — no DLQ.
- `Dir` field is required for FID-related methods (`LastFID`, `MinimalFID`, `MinimalID`); pass `""` to disable them (returns 0 with no error).
- ax-wal's `Reader.Stats().NextCalls` is the closest analogue to "records delivered". It increments once per `Reader.Next` call, including a few internal retry paths — close to but not exactly equal to the count of payloads handed to the consumer.
