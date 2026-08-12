# Remote protocol tests

These tests guard the gribjump remote client/server **wire protocol** so that a
code change which alters the bytes on the wire is caught deliberately rather
than by accident. They are all FDB-free and socket-free (except the socketpair
smoke test, which uses a local kernel socket but no FDB and no listening port),
and run in the normal fast unit-test suite.

## The layers

| Test executable | What it proves |
|---|---|
| `gribjump_test_protocol_codec` | Byte-exact golden hashes of each payload and framed request/reply. Directly answers "did the protocol change?". |
| `gribjump_test_protocol_server` | Real `Request` subclasses + real server `dispatchRequest` parse and reply correctly, driven by a `MockEngine` (no FDB). |
| `gribjump_test_protocol_loopback` | Real client codec wired back-to-back to real server dispatch over an in-memory stream — client and server agree on the format. |
| `gribjump_test_protocol_socketpair` | Same as loopback but over a genuine connected kernel socket (`AF_UNIX` `socketpair`), server on its own thread. Proves framing survives a real blocking full-duplex transport. |

Shared helpers (`MockEngine`, the duplex in-memory stream, encode utilities and
the `RemoteProtocolTestAccess` friend seam) live in `protocol_test_helpers.h`.

The single source of truth for the request framing (protocol version +
`LogContext` + `RequestType`) is `src/gribjump/remote/Protocol.h`, used by both
the client (`RemoteGribJump::sendHeader`) and the server
(`GribJumpUser::dispatchRequest`).

## Running them

```sh
ctest -R gribjump_test_protocol --output-on-failure
```

## When the golden hashes change

A changed golden hash in `test_protocol_codec.cc` means the wire format
changed. This is intentional only if you meant to change the protocol. When you
do:

1. **Bump `remoteProtocolVersion`** in `src/gribjump/remote/Protocol.h`. The
   codec test asserts its current value, so an intentional protocol change must
   update it. Old clients/servers will then correctly reject the new version.
2. **Regenerate the golden hashes.** Run `gribjump_test_protocol_codec`; on a
   mismatch it prints the actual hash for each affected fixture. Copy the new
   values into the inline goldens in `test_protocol_codec.cc`.
3. Re-run the full protocol suite and confirm the round-trips still
   pass (they exercise the semantics, not just the bytes).

If a golden hash changed and you did **not** intend to alter the protocol, treat
it as a regression: the diff points straight at the serialisation change.
