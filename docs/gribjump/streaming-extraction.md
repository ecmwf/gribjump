# Streaming Extraction (Protocol v4)

This note documents the GribJump **streaming** extract path: how a remote
`extract` request is served by sending results back incrementally, in bounded
memory, instead of buffering the entire reply on the server before sending it.

It complements two sibling notes:

- [Round-Robin Work Scheduling](round-robin-scheduling.md) — how tasks are
  dispatched across worker threads. Streaming reuses that machinery and adds
  per-group backpressure on top of it.
- [Metrics](metrics.md) — the counters emitted by both extraction paths.

## Motivation

The legacy path (**protocol v3**, "buffered") runs every extraction task to
completion, collects all `ExtractionResult`s into a `ResultsMap`, and only then
serialises the whole thing onto the wire in request order. Peak server memory is
therefore proportional to the *total* size of the reply: a large request holds
every decoded value in RAM at once, and the client sees nothing until the last
task finishes.

The streaming path (**protocol v4**) instead:

- hands results to the wire *as each task completes*, in whatever order they
  finish, and frees them immediately after sending;
- bounds the produced-but-not-yet-sent bytes with a per-request **byte budget**,
  applying backpressure to task dispatch when a slow client can't keep up;
- terminates the reply with a footer carrying any per-task errors.

Peak memory becomes proportional to the byte budget plus one flush batch, not to
the size of the whole reply.

## Version negotiation

Every request header is `[protocol version][log context][request type]`. The
client advertises a version; the server validates it against
`supportedProtocolVersions` (`{3, 4}`) and echoes the negotiated version back
into each `RequestHandler`.

| Constant | Value | Reply framing |
|---|---|---|
| `remoteProtocolVersion` | 3 | buffered: leading error block + one in-order result block |
| `streamingProtocolVersion` | 4 | streaming: tagged result chunks + END footer |

`ProtocolVersion::streaming()` is simply `value >= 4`. The client defaults to v4
but can be pinned to v3 (see [Configuration](#configuration)); a v4 client
talking to an old v3-only server fails the header check with a clear
version-mismatch error, and vice versa. Only the **EXTRACT** and
**FORWARD_EXTRACT** replies differ between versions; SCAN/AXES framing is
unchanged.

## Wire framing

### v3 (buffered)

```
reply := errorBlock resultBlock
errorBlock  := nErrors:size_t  (errorString)*
resultBlock := (ExtractionResult)*        # one per request, in request order
```

### v4 (streaming)

A reply is a sequence of tagged chunks, terminated by an `END` chunk that
carries the error footer:

```
reply := chunk* endChunk
chunk    := tag(RESULTS=0):uint16  count:size_t  ( index:size_t  ExtractionResult )*
endChunk := tag(END=1):uint16      errorBlock
```

Key properties:

- **Out of order.** `RESULTS` chunks are emitted in *task-completion* order, not
  request order. Each result is therefore prefixed with its `index` — the
  position of the originating request in the client's request vector.
- **Batched.** One `RESULTS` chunk carries a *batch* of `(index, result)` pairs.
  The server chooses batch boundaries by a flush threshold (below).
- **Errors trail.** Because chunks are already on the wire before the server
  knows whether every task succeeded, errors cannot lead the reply as in v3.
  They ride in the `END` footer instead, using the *same* layout as the v3
  leading error block, so `decodeErrors`/`encodeErrors` are reused verbatim.

The encoders (`Protocol::encodeExtractResultChunk`,
`Protocol::encodeExtractReplyEnd`) are batch-composable so the server can flush
whenever it likes. `Protocol::decodeExtractReplyStreaming` reassembles the
chunks into an `nRequests`-sized vector indexed by `index`, then reads the
footer (which raises on any server-side error).

## Server-side architecture

```
                 ExtractHandler (RequestHandler)
                        │  selects strategy by negotiated version
                        ▼
             ExtractReplyStrategy
              ├── BufferedExtractReply   (v3)  ── Engine::extract()
              └── StreamingExtractReply  (v4)  ── Engine::extractStreaming(sink)
                                                        │
                                                        ▼
                                             ResultSink  ◄── StreamResultSink
                                             (encode one RESULTS chunk / batch)
```

### `RequestHandler` and the reply strategy

`GribJumpUser` decodes the header and constructs a `RequestHandler` subclass
(`ExtractHandler`, `ScanHandler`, …). `RequestHandler::process()` drives the
fixed lifecycle: `receive() → info() → execute() → reportErrors() →
replyToClient()`.

`ExtractHandler` owns an `ExtractReplyStrategy`, chosen once at construction from
the negotiated version, so the handler itself carries no version branching:

- **`BufferedExtractReply` (v3)** — `execute()` calls `Engine::extract()` and
  buffers the `ResultsMap`; `reply()` serialises results in request order.
  `emitsLeadingErrorBlock()` is `true`.
- **`StreamingExtractReply` (v4)** — `execute()` calls
  `Engine::extractStreaming()` with a `StreamResultSink` wrapping the client
  stream; results go out *during* `execute()`. `emitsLeadingErrorBlock()` is
  `false` (errors go in the footer). `reply()` writes the `END` chunk plus the
  error footer. Any exception thrown mid-stream is captured and appended to the
  footer errors, because chunks already sent cannot be unsent.

### `ResultSink`: the encode seam

`ResultSink` is the seam between the engine and the wire:

```cpp
class ResultSink {
    virtual void writeResults(
        const std::vector<std::pair<size_t, const ExtractionResult*>>& batch) = 0;
};
```

- `StreamResultSink` is the production implementation: it encodes each batch as
  one v4 `RESULTS` chunk straight onto the client stream.
- The engine owns *all* batching and byte-budget policy and frees results after
  a batch is sent; the sink only encodes. This keeps the engine's streaming loop
  testable with a mock sink (e.g. a recording sink, or one that throws to
  simulate a disconnect) without a socket.

### `Engine::extractStreaming`

The heart of the path. After building the request/file maps (shared with the
buffered path), it:

1. Builds an `indexOf` map from each *canonicalised* request string back to its
   original request index. (`buildRequestMap` canonicalises request strings in
   place, so this is how a completed item is mapped to the client's index.)
2. Creates a `TaskGroup`, sets its **byte budget**
   (`streaming.byteBudget`), and enqueues the file-extraction tasks — the same
   `FileExtractionTask`s the buffered path uses.
3. **Harvest loop:** repeatedly calls `taskGroup.popCompleted()`, which blocks
   until the next task finishes and returns its id (or `nullopt` once all tasks
   are accounted for). For each completed task it moves out the results, appends
   `(index, result*)` pairs to a batch, and tracks `batchBytes`.
4. **Flush** when `batchBytes >= streaming.flushBytes`: hand the batch to
   `sink.writeResults`, then `taskGroup.releaseOutstanding(batchBytes)` to
   decrement the outstanding-byte counter (and possibly wake throttled workers),
   and free the batch's results.
5. On normal completion, a final flush drains the last partial batch and the
   task report is returned for the footer.

The **forwarding** case (`forwardExtraction`) can't stream incrementally — it
aggregates buffered replies from downstream servers — so it runs the buffered
schedule and then replays the collected results through the same sink via
`streamBufferedResults`, preserving the v4 wire framing for the client.

## Backpressure and bounded memory

The point of streaming is to bound peak memory even when the client (or network)
drains slower than the workers produce. Two thresholds cooperate:

| Threshold | Config | Default | Role |
|---|---|---|---|
| Flush size | `streaming.flushBytes` | 8 MiB | How many result bytes accumulate before one `RESULTS` chunk is sent. Trades syscall/framing overhead against latency. |
| Byte budget | `streaming.byteBudget` | 128 MiB | Ceiling on produced-but-not-yet-sent bytes per request. When exceeded, the group's task dispatch is throttled. |

### Byte accounting on the `TaskGroup`

The `TaskGroup` tracks `outstandingBytes_` — result bytes produced but not yet
sent — against `byteThreshold_`:

- When a `FileExtractionTask` finishes, `extract()` sums its produced result
  bytes and calls `TaskGroup::addOutstanding()` (which also updates the
  `peakOutstandingBytes_` high-water mark). This only happens when
  `backpressureEnabled()` is true, i.e. a finite budget was set — so the
  buffered path pays nothing.
- After a batch is sent and freed, the engine calls
  `TaskGroup::releaseOutstanding(bytes)`, decrementing the counter. If the group
  drops back under budget it calls `WorkQueue::reconsider()` to wake workers.

### Throttling dispatch in the `WorkQueue`

`WorkQueue::popNext` walks the round-robin order and serves the first group that
has queued tasks **and is not over budget** (`TaskGroup::overBudget()`). An
over-budget group keeps its place in the rotation but is skipped, so its tasks
don't run — and therefore don't produce more bytes — until the consumer catches
up and `releaseOutstanding` brings it back under budget. Other groups continue
to be served normally, so one slow client doesn't stall the whole server.

The feedback loop:

```
workers produce results ──► outstandingBytes_ rises ──► overBudget()
        ▲                                                     │
        │ reconsider() wakes workers                          ▼
release after send ◄── engine flushes batch ◄── client drains the socket
```

### Lock ordering

Two mutexes are involved: `TaskGroup::m_` and `WorkQueue::mtx_`. The rule of
thumb (noted at both declarations) is **never hold both at once**.
`overBudget()` is called from `WorkQueue::popNext()` while `WorkQueue::mtx_` is
held; reading `outstandingBytes_` under `TaskGroup::m_` there would invert the
lock order and risk deadlock. That is the sole reason `outstandingBytes_` is
`std::atomic` — `overBudget()` reads it locklessly, while every *write* still
holds `m_`.

## Cancellation and client disconnect

If the client disconnects mid-stream, the next `sink.writeResults` throws (e.g.
broken pipe). The engine must not simply return — its `TaskGroup` lives on the
stack while worker threads still reference it — so it:

1. **Catches** the exception in the harvest loop.
2. Calls `TaskGroup::cancel()`, which
   - flags every still-`PENDING` task `CANCELLED`, and
   - calls `WorkQueue::cancelGroup()` to purge the group's still-queued tasks so
     they never start.
3. Calls `drainRemaining()`, which blocks on `popCompleted()` until the tasks
   *already in flight* finish — guaranteeing no worker still references the
   soon-to-be-destroyed `TaskGroup`.
4. Sets the disconnect metrics (`client_disconnected`, `count_cancelled_tasks`,
   `count_bytes_streamed`, `peak_outstanding_bytes`) directly, because the
   rethrow skips the normal `TaskGroup::report()` step.
5. **Rethrows**, so `StreamingExtractReply` records the error into the END
   footer.

Two correctness details make cancellation terminate cleanly:

- Cancelled tasks are still counted toward completion (`notifyCancelled`
  increments the completed count), so `popCompleted()` reaches its terminal
  `nComplete_ == tasks_.size()` condition and returns `nullopt`.
- A task cancelled *after* it was popped but *before* it ran still calls
  `notifyCancelled()` from `Task::execute()`, and `WorkQueue::cancelGroup()`
  notifies the tasks it removed from the queue — so every task is accounted for
  exactly once regardless of the race.

## Client-side

`RemoteGribJump` advertises `protocolVersion_` in every header and branches only
in `extract()` when decoding the reply:

- **v4:** `Protocol::decodeExtractReplyStreaming(stream, nRequests)` reads
  `RESULTS` chunks into an `nRequests`-sized vector (slotting each result by its
  `index`), stops at `END`, and reads the error footer (which raises on
  server-side errors).
- **v3:** leading error block, then the buffered in-order reply.

To the caller the two are indistinguishable — both return a
`std::vector<std::unique_ptr<ExtractionResult>>` in request order.

`ClientTransport`/`ClientConnection` abstract the socket: production uses
`TcpTransport`, but tests can inject a fake transport (and a chosen protocol
version) to exercise the full client codec without a live TCP server.

## Configuration

| Option | Env var | YAML | Default |
|---|---|---|---|
| Client protocol version | `GRIBJUMP_CLIENT_PROTOCOL_VERSION` | `clientProtocolVersion` | 4 (streaming) |
| Flush size (server) | `GRIBJUMP_STREAMING_FLUSH_BYTES` | `streaming.flushBytes` | 8 MiB |
| Byte budget (server) | `GRIBJUMP_STREAMING_BYTE_BUDGET` | `streaming.byteBudget` | 128 MiB |

Pin the client to `3` to force the legacy buffered reply (e.g. against an older
server, or for A/B comparison). The server accepts both versions regardless.

## Metrics

Streaming emits everything the buffered path does (via `TaskGroup::report()`)
plus three streaming-only counters — `count_bytes_streamed`,
`peak_outstanding_bytes`, and (on disconnect) `client_disconnected`. See
[Metrics](metrics.md) for the full table and the note on why the disconnect path
sets some counters directly.

## Summary

| | v3 buffered | v4 streaming |
|---|---|---|
| Reply framing | error block + one in-order block | `RESULTS` chunks + `END` footer |
| Result order | request order | completion order (index-tagged) |
| Peak server memory | ∝ total reply size | ∝ byte budget + one batch |
| Errors | lead the reply | trail in `END` chunk |
| Backpressure | none | per-request byte budget throttles dispatch |
| Harvest | `TaskGroup::waitForTasks()` | `TaskGroup::popCompleted()` |
