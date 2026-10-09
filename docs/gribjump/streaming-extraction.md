# Streaming Extraction (Protocol v4)

This note documents the GribJump **streaming** extract path: how a remote
`extract` request is served by sending completed-task results back incrementally,
rather than retaining the entire reply on a local/leaf extraction server.
Streaming reduces result retention, but does **not** impose a hard memory bound.

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
- throttles new task dispatch when the accounted completed-result bytes exceed a
  per-request **byte budget**, applying backpressure when a slow client can't keep up;
- terminates the reply with a footer carrying any per-task errors.

The byte budget is a **soft dispatch threshold**, not an allocation limit. Peak
memory also depends on whole-file task sizes, work already in flight, caches and
other buffers. A large file task can still retain a large fraction of the reply
before anything can be sent.

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

1. Preserves the `streamIndex` stamped on each item during request preparation
   (or decoding of a forwarded filemap). No renumbering is needed at completion.
2. Creates a `TaskGroup`, sets its dispatch-throttling threshold
   (`streaming.byteBudget`), and submits the file-extraction tasks under the same
   cleanup guard as harvesting. The immutable `ConfigOptions` has already checked
   `streaming.flushBytes <= streaming.byteBudget`, after environment overrides,
   before the engine can submit any work.
3. **Harvest loop:** repeatedly calls `taskGroup.popCompleted()`, which blocks
   until the next task finishes and returns its id (or `nullopt` once all tasks
   are accounted for). For each completed task it moves out the results, appends
   `(index, result*)` pairs to a batch, and tracks `batchBytes`.
4. **Flush** when the batch reaches `streaming.flushBytes`: `ResultBatch` hands
   it to `sink.writeResults` and frees its results after the write succeeds. The
   engine then releases those bytes from the task group's accounting, possibly
   waking throttled workers.
5. On normal completion, a final flush drains the last partial batch and the
   task report is returned for the footer.

The **forwarding** case (`forwardExtraction`) gathers downstream replies before
sending them upstream, even if the downstream servers use v4. It then replays the
collected results through `streamBufferedResults`, preserving the v4 wire framing
but retaining the full downstream result set at the proxy.

## Backpressure and memory limitations

Backpressure limits further dispatch when a client (or network) drains slower
than workers produce completed results. Two thresholds cooperate:

| Threshold | Config | Default | Role |
|---|---|---|---|
| Flush size | `streaming.flushBytes` | 8 MiB | How many result bytes accumulate before one `RESULTS` chunk is sent. Trades syscall/framing overhead against latency. |
| Byte budget | `streaming.byteBudget` | 128 MiB | Soft threshold on accounted completed-result bytes awaiting send. When exceeded, further task dispatch for the group is throttled. |

Neither threshold is a hard bound on memory or chunk size:

- A file task extracts all of its requested fields before reporting their result
  bytes. It can allocate more than the budget before dispatch is throttled.
- Tasks already running continue to allocate and complete. Their unfinished
  results and extraction workspace are not included in the byte counter.
- Results are not split to meet the flush threshold. A single field larger than
  `streaming.flushBytes` produces a larger chunk.
- The threshold is per request, not per process. Concurrent requests, caches,
  catalogue metadata and transport buffers add to memory use.
- Forwarding proxies and the current client decoder buffer their complete result
  sets. Server-side chunking is not end-to-end lazy iteration.

In particular, a 128 MiB budget does not guarantee a 128 MiB (or budget-plus-one-
batch) peak. Bounding task sizes or reserving memory before extraction would be
needed for a stronger guarantee; neither is implemented here.

### Byte accounting on the `TaskGroup`

The `TaskGroup` tracks `outstandingBytes_` — accounted completed-task result
payloads not yet sent — against `byteThreshold_`. This is not process memory/RSS:

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
over-budget group keeps its place in the rotation but is skipped, so its queued
tasks do not start until the consumer catches up and `releaseOutstanding` brings
it back under budget. Already-running tasks are not stopped. Other eligible
groups continue to be served normally.

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

1. **Catches** the exception in the guard covering both task submission and
   harvesting.
2. Calls `TaskGroup::cancel()`, which
   - flags every still-`PENDING` task `CANCELLED`, and
   - calls `WorkQueue::cancelGroup()` to purge the group's still-queued tasks so
     they never start.
3. Drains `popCompleted()` until all submitted tasks, including those already in
   flight, are accounted for. If submission failed before the first task, there
   is no work to cancel or drain. No submitted work may outlive the stack-local
   `TaskGroup` or caller-owned extraction items.
4. Sets the disconnect metrics (`client_disconnected`, `count_cancelled_tasks`,
   `count_bytes_streamed`, `peak_outstanding_bytes`) directly, because the
   rethrow skips the normal `TaskGroup::report()` step.
5. **Rethrows**, so `StreamingExtractReply` records the error into the END
   footer.

Submission errors use the same cancellation/draining path but are not labelled
as client disconnects. Invalid byte-limit configuration is rejected earlier,
when `ConfigOptions` is constructed, before task submission is possible.

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

Resolved options must satisfy `streaming.flushBytes <= streaming.byteBudget`.
This is checked when constructing `ConfigOptions`, including environment/resource
overrides, so invalid limits fail before any extraction tasks are submitted.
The inequality prevents batching from waiting for more bytes while dispatch is
throttled; it does not turn the byte budget into a memory ceiling.

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
| Peak server memory | ∝ total reply size | Not hard-capped: depends on whole-file tasks, in-flight work and other buffers; proxies still buffer full replies |
| Errors | lead the reply | trail in `END` chunk |
| Backpressure | none | per-request byte budget throttles dispatch |
| Harvest | `TaskGroup::waitForTasks()` | `TaskGroup::popCompleted()` |
