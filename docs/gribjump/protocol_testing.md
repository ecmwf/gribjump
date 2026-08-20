# Remote protocol tests

These tests guard the gribjump remote client/server **wire protocol** so that a
code change which alters the bytes on the wire is caught.

## Unit tests

| Test executable                     | What it proves                                                                                                                                                                 |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `gribjump_test_protocol_codec`      | Byte-exact golden hashes of each payload and framed request/reply. Catches whether the protocol changed.                                                                       |
| `gribjump_test_protocol_server`     | Real `Request` subclasses + real server `dispatchRequest` parse and reply correctly, driven by a `MockEngine` (no FDB).                                                        |
| `gribjump_test_protocol_loopback`   | Real client codec wired back-to-back to real server dispatch over an in-memory stream — client and server agree on the format.                                                 |
| `gribjump_test_protocol_client`     | The real `RemoteGribJump` client (its version negotiation and reply-decode branch) driven over an injected socketpair transport whose peer runs the real server `dispatchRequest`. Covers v3 buffered, v4 streaming, errors and empty replies. |

Every framed golden in `gribjump_test_protocol_codec` is produced by calling the
**production** `Protocol::encode*` methods.

Shared helpers (`MockEngine`, the duplex in-memory stream, encode utilities)
live in `protocol_test_helpers.h`.

The single source of truth for the wire format is
`src/gribjump/remote/Protocol.h`: every message has exactly one encoder and one
matching decoder. Both the client (`RemoteGribJump`) and the server
(`GribJumpUser::dispatchRequest` / the `Request` subclasses) drive all their
stream I/O through `Protocol`, so the two sides cannot drift apart. The protocol
version and `RequestType` enum live alongside the codec in
`src/gribjump/remote/Protocol.h`.

## Running them

```sh
ctest -R gribjump_test_protocol --output-on-failure
```

## When the golden hashes change

A changed golden hash in `test_protocol_codec.cc` means the wire format changed.
This is intentional only if you meant to change the protocol. When you do:

1. **Edit the matched encode/decode pair in `Protocol`.** Each message's encoder
   and decoder sit side by side – keep them in sync.
2. **Bump `remoteProtocolVersion`** in `src/gribjump/remote/Protocol.h`. This
   ensures old clients/servers will then correctly reject the new version.
3. **Regenerate the golden hashes.** Run `gribjump_test_protocol_codec`; on a
   mismatch it prints the actual hash for each affected fixture. Copy the new
   values into the expected goldens in `test_protocol_codec.cc`.
4. Re-run the full protocol suite and confirm the round-trips still pass.

Obviously changing the protocol has implications for existing client/server
deployments. If your changes are backwards incompatible, ensure a migration plan
is in place.

If a golden hash changed and you did **not** intend to alter the protocol, treat
it as a regression.

## End-to-end remote tests

We have the following end-to-end integration tests:

- **`test_remote.cc`** — a direct client → server round-trip (extract/axes/scan
  over a real socket against a real FDB-backed server).
- **`test_remote_forward.cc`** — the server-to-server forwarding path, using
  `forwarding_client.yaml` so one server forwards requests to another.

These are heavier than the `protocol*` tests as they spin up a gribjump-server
and run client-driven tests against it to query an underlying FDB.
