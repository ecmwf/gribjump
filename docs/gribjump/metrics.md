# Metrics

This note catalogues the metrics GribJump collects for each request, and where
they come from. It is a reference for anyone consuming the metrics log or adding
new instrumentation.

## How metrics are collected and emitted

Metrics are accumulated through the `MetricsManager` singleton:

```cpp
MetricsManager::instance().set("name", value);   // a key/value pair
MetricsManager::instance().addRequest(request);  // append a MARS request
```

Key points:

- **Thread-local.** `MetricsManager` wraps a `thread_local Metrics` object, so
  each worker thread accumulates its own set independently. `set()` overwrites
  any previous value for the same key.
- **Emitted by the remote server.** `GribJumpUser::serve()` calls
  `MetricsManager::instance().report()` once, unconditionally, at the end of
  serving a connection (including on the error path). `Metrics::report()` writes
  a single JSON object to `eckit::Log::metrics()`.
- **Sparse.** A metric only appears in the output if it was `set()` for that
  request, so the exact key set depends on the `action` and which code paths ran.
- **Values** are `eckit::Value`; in practice strings, unsigned integers, doubles
  (elapsed times, in **seconds**), and booleans.

The in-process library path (`LocalGribJump`) sets the same Engine/Task metrics,
but nothing calls `report()`, so they are collected and never emitted. The
catalogue below therefore describes the **remote server** path.

## Always present

Emitted by `Metrics::report()` for every reported request.

| Metric | Type | Description |
|---|---|---|
| `process` | string | Process name (`eckit::Main::instance().name()`). |
| `start_time` | ISO-8601 | When the `Metrics` object was created. |
| `end_time` | ISO-8601 | When the request was reported. |
| `run_time` | seconds | Wall-clock elapsed since the `Metrics` object was created. |
| `mars_requests` | list | MARS requests accumulated via `addRequest()` (see `Lister`). |
| `context` | JSON | The `LogContext` (propagated for forwarded requests). |

## Per-request envelope

Set by `RequestHandler` for every remote request, regardless of action.

| Metric | Type | Description | Source |
|---|---|---|---|
| `gribjump_request_id` | uint | Unique id assigned to the request. | `RequestHandler` ctor |
| `action` | string | `scan`, `extract`, `forwarded-extract`, `forwarded-scan`, or `axes`. | each handler's `receive()` |
| `elapsed_receive` | seconds | Time to decode/receive the request. | `RequestHandler::process()` |
| `elapsed_execute` | seconds | Time to execute the request. | `RequestHandler::process()` |
| `elapsed_reply` | seconds | Time to reply to the client. | `RequestHandler::process()` |

## Error path

| Metric | Type | Description | Source |
|---|---|---|---|
| `error` | string | Exception message, or `"Uncaught exception"`. | `GribJumpUser::serve()` catch |

## Scan (`action = scan`)

| Metric | Type | Description | Source |
|---|---|---|---|
| `count_scan_requests` | uint | Number of scan requests in the batch. | `ScanHandler::receive()` |
| `count_scanned_fields` | uint | Number of fields scanned (`0` if none). | `Engine::scan()` |

## Extract (`action = extract`)

Covers both the **buffered** (v3) and **streaming** (v4) local extraction paths.
`elapsed_collect_results` is buffered-only; the streaming byte/peak/disconnect
metrics are streaming-only.

| Metric | Type | Description | Source | Path |
|---|---|---|---|---|
| `count_extraction_requests` | uint | Number of extraction requests in the batch. | `ExtractHandler::receive()` | both |
| `elapsed_build_filemap` | seconds | Time to build the file map. | `Engine` | both |
| `elapsed_tasks` | seconds | Time for all extraction tasks to complete/stream. | `Engine` | both |
| `elapsed_collect_results` | seconds | Time to repackage buffered results. | `Engine::extract()` | buffered only |
| `count_tasks` | uint | Number of tasks in the group. | `TaskGroup::report()` | both |
| `count_failed_tasks` | uint | Number of tasks that errored. | `TaskGroup::report()` | both |
| `count_cancelled_tasks` | uint | Number of tasks cancelled (never ran). | `TaskGroup::report()`; also set in `Engine::extractStreaming()` on disconnect | both |
| `first_error` | string | First error message (only if any errors). | `TaskGroup::report()` | both |
| `count_bytes_streamed` | uint | Total result bytes streamed to the client (partial on disconnect). | `Engine::extractStreaming()` | streaming only |
| `peak_outstanding_bytes` | uint | High-water mark of produced-but-not-yet-sent bytes (the ceiling the byte budget enforces). | `Engine::extractStreaming()` | streaming only |
| `client_disconnected` | bool | `true` if a mid-stream failure (e.g. client disconnect) aborted streaming. | `Engine::extractStreaming()` catch | streaming only |

> On the streaming disconnect path the request rethrows before
> `TaskGroup::report()` runs, so `count_cancelled_tasks`, `count_bytes_streamed`
> and `peak_outstanding_bytes` are set directly in the `catch` block instead,
> alongside `client_disconnected`. This makes the *wasted work avoided* (the
> cancelled count) observable even when the client goes away.

## Forwarded extract (`action = forwarded-extract`)

Emitted by a forwarding node that aggregates buffered replies from downstream
servers.

| Metric | Type | Description | Source |
|---|---|---|---|
| `count_extraction_requests` | uint | Number of extraction requests forwarded. | `ForwardedExtractHandler::receive()` |

## Forwarded scan (`action = forwarded-scan`)

| Metric | Type | Description | Source |
|---|---|---|---|
| `count_received_offsets` | uint | Number of offsets received for the forwarded scan. | `ForwardedScanHandler::receive()` |

## Axes (`action = axes`)

No action-specific metrics beyond the per-request envelope.

## Buffered vs. streaming parity

The four task-count metrics (`count_tasks`, `count_failed_tasks`,
`count_cancelled_tasks`, `first_error`) are emitted from `TaskGroup::report()`,
which is the terminal step of **both** extraction paths. This is deliberate: the
streaming path harvests via `popCompleted()` rather than `waitForTasks()`, so
placing the metrics in `report()` (rather than `waitForTasks()`) ensures both
paths report the same task-level figures.
