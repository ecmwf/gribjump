# Remote server status

`status()` queries the union of the configured `uri` and the GribJump destinations
in `servermap`. Destinations are deduplicated by host/port and returned in sorted
order. Both remote clients and local clients with a `servermap` can use it. It
never reports the calling process's local status and never recursively discovers
servers. With no destinations configured it raises an explicit error.

## Usage

C++:

```cpp
gribjump::GribJump gj(config);
std::cout << gj.status() << '\n';
// Optional per-server timeout (seconds) and logging context:
std::string json = gj.status(5.0, gribjump::LogContext(R"({"job":"monitor"})"));
```

Python (pybind interface):

```python
import json
from pygribjump import GribJump

gj = GribJump({"type": "remote", "uri": "server.example:9999"})
print(gj.status())
document = json.loads(gj.status(timeout=5, ctx={"job": "monitor"}))
```

CLI:

```sh
gribjump-status                          # normal GribJump config/environment
gribjump-status --config=client.yaml --timeout=5
```

The API returns JSON **text**, not a fixed diagnostic data model. The CLI dumps
that text to stdout, without a table or field-specific formatting. It exits zero
when it has produced a report, including reports with per-server failures. Local
configuration/argument errors cause a nonzero exit.

There are no new C, CFFI or Rust bindings.

## Response

The envelope has a `schema_version` and a `servers` list. Each entry contains:

* `endpoint`: configured host and port.
* `state`: `ok`, `unreachable`, `unsupported`, or `error`.
* `status`: the server's JSON document (only for `ok`).
* `error`: an explanatory string (otherwise).

Servers own their diagnostic documents. Clients validate JSON syntax but do not
interpret diagnostic fields, so new fields/sections need no client changes.
Consumers should tolerate unknown fields. JSON schema versions are independent
of the transport protocol version.

The initial server document contains:

* `schema_version`: currently 1.
* `server`: hostname, PID, GribJump version, monitoring start time, snapshot time
  (Unix seconds), and uptime (monotonic seconds since server monitoring started).
* `threads`: live worker count, busy/idle counts, active request-handler count,
  and workers with opaque thread IDs and current task/request IDs. Request-handler
  thread IDs also appear in the individual requests. These are GribJump worker
  and request-handler threads, not an inventory of all OS/library threads.
* `active_requests`: operation, request ID, handler thread ID, phase, elapsed
  seconds, seconds in the current phase, task counts and the original logging
  `context`. IDs are process-local, and match `gribjump_request_id` in request
  metrics. Correlate them with server PID/start time across restarts.

Phases currently include `receiving`, `executing`, `discovering_fields`,
`waiting_for_tasks`, and `sending_results`. Streaming requests can alternate
between waiting and sending. This makes it possible to distinguish a request
waiting for extraction from one blocked delivering results to its client.

Task counts are `total`, `queued`, `running`, `completed`, `failed`, and
`cancelled`. Completed means successful. Counts accumulate over all task groups
of the request, including groups already destroyed, and survive error reporting.
Before discovery/submission finishes, `total` can still increase; it is not a
prediction of the final work size. Queued includes cancelled tasks awaiting their
cancellation notification. Tasks are internal file/forwarding work units, **not**
individual MARS selections or GRIB fields.

Requests are registered after their protocol header has been decoded and removed
when dispatch exits, including on exceptions/disconnects. Connections still
receiving a header and status queries themselves are excluded. A disconnected
streaming request remains visible while cancelling/draining its work.

## Availability and compatibility

Status uses protocol **v4**, even when extraction is pinned to v3. Its request is
an ordinary header with the `STATUS` opcode and no body. Its reply is the normal
leading error block followed by one JSON string. Existing v3/v4 extraction wire
layouts are unchanged. A v3 `STATUS` request to a new server returns an explicit
`gribjump.status unsupported: requires protocol version 4` error.

Older servers rejecting v4 or the opcode are reported as `unsupported`. Some v3
servers close with unread header bytes, causing TCP to reset before their version
error reaches the client. On an early reply disconnect the client can make one
v3 opcode probe within the remaining timeout to identify that case. A reset or
unrecognised error alone is never treated as proof that status is unsupported.

At most eight servers are queried concurrently. Each has a connect + network I/O
deadline (default 5 seconds, configurable in `(0, 86400]`). Unreachable or stalled
servers do not discard other servers' replies. With more than eight destinations,
queries run in batches, so the timeout is not a deadline for the entire API call.
**Hostname resolution follows the system resolver's timeout**, outside the socket
I/O deadline. Status does not change extraction networking, install process-wide
signal handlers, or leave background query threads running after it returns.

On the server, status bypasses engine construction and the extraction queue. It
can respond with every worker busy. Monitoring copies small pieces of state
under short locks and serializes/writes outside those locks. It is a best-effort
live view, not an atomic snapshot across all requests and servers.

Only the threaded GribJump server is supported, not fork-per-connection mode.
Status uses the existing server connection/access model. Logging contexts are
returned to status callers; avoid putting secrets in them, and restrict server
network access accordingly. MARS selections, file paths and client addresses are
not added to the status response.
