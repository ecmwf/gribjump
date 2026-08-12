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

Every framed golden in `gribjump_test_protocol_codec` is produced by calling the
**production** `ProtocolCodec::encode*` methods — the same code the real client
and server run — not a hand-rolled re-implementation of the framing. So if you
change a production encoder, the golden hash changes and the test fails; the
fixtures cannot silently agree with a modified protocol.

Shared helpers (`MockEngine`, the duplex in-memory stream, encode utilities)
live in `protocol_test_helpers.h`.

The single source of truth for the wire format is
`src/gribjump/remote/ProtocolCodec.h`: every message has exactly one encoder and
one matching decoder, side by side. Both the client (`RemoteGribJump`) and the
server (`GribJumpUser::dispatchRequest` / the `Request` subclasses) drive all
their stream I/O through `ProtocolCodec`, so the two sides cannot drift apart.
The protocol version and `RequestType` enum live in the lightweight
`src/gribjump/remote/Protocol.h`.

## Running them

```sh
ctest -R gribjump_test_protocol --output-on-failure
```

## When the golden hashes change

A changed golden hash in `test_protocol_codec.cc` means the wire format
changed. This is intentional only if you meant to change the protocol. When you
do:

1. **Edit the matched encode/decode pair in `ProtocolCodec`.** Because each
   message's encoder and decoder sit side by side, a format change is a single,
   local edit — keep the two halves in sync.
2. **Bump `remoteProtocolVersion`** in `src/gribjump/remote/Protocol.h`. The
   codec test asserts its current value, so an intentional protocol change must
   update it. Old clients/servers will then correctly reject the new version.
3. **Regenerate the golden hashes.** Run `gribjump_test_protocol_codec`; on a
   mismatch it prints the actual hash for each affected fixture. Copy the new
   values into the inline goldens in `test_protocol_codec.cc`.
4. Re-run the full protocol suite and confirm the round-trips still
   pass (they exercise the semantics, not just the bytes).

If a golden hash changed and you did **not** intend to alter the protocol, treat
it as a regression: the diff points straight at the serialisation change.
