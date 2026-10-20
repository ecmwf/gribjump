# GJ-123: extremes CCSDS extraction regression

`extremes-100.grib2` is a **738-byte**, two-message fixture derived from the
`eccodes-test-audit/extremes.grib` file used in the earlier extraction audit.
Each message contains the first **100 decoded values** of the corresponding
original field, repacked with `grib_filter` into a synthetic 10-by-10 grid.
The projection metadata is retained, but this is an index-extraction fixture,
not a geographically faithful crop of the original grid.

| Field | Packing | Values | Bits/value | Decimal scale | Binary scale |
|---|---|---:|---:|---:|---:|
| 1 | `grid_ccsds` | 100 | 12 | -9 | -39 |
| 2 | `grid_ccsds` | 100 | 12 | -3 | -19 |

All 100 selected values in each message round-trip **exactly** against the
original field's first 100 decoded values. The binary scales change from -38
and -18 to -39 and -19 when packing the smaller ranges. The negative decimal
scales are preserved: accidentally producing decimalScaleFactor=0 would hide
the bug this test is meant to catch.

The original bug used `(-10)^D` instead of `10^(-D)` in CCSDS decoding.
This fixture would therefore produce GribJump/ecCodes ratios of `-1e-18` and
`-1e-6`, respectively, with that implementation.

## Test

`test_extremes.cc` reads both messages with ecCodes, verifies the fixture's
packing/scales/value count, then addresses each original message by byte offset
through GribJump's `PathExtractionRequest` API. For each field it compares:

- every one of the 100 values, with exact equality against ecCodes;
- disjoint ranges `[0,1)`, `[31,36)`, `[63,66)`, `[99,100)`;
- the returned validity bits (all points in this fixture are present).

The test uses a disabled cache, needs no FDB or downloads, and does not modify
the fixture. CMake copies the fixture into the test's build directory.

```sh
cmake --build build --target gribjump_test_gj_123_extremes -j 6
ctest --test-dir build -R '^gribjump_test_gj_123_extremes$' --output-on-failure
```

## Regenerating the fixture

From the bundle root:

```sh
python3 gribjump/tests/regressions/GJ-123/prepare.py \
    eccodes-test-audit/extremes.grib \
    --grib-filter "$PWD/build/bin/grib_filter"
```

`prepare.py` uses only Python's standard library. It generates and runs two
`grib_filter` rule sets: one selects the original values using `element()` and
prints round-trip-precision literals, and the second repacks the two messages.
All GRIB reading, selection, and packing are performed by `grib_filter`, not
Python bindings. The large original file and the regeneration script are not
required to run the CTest regression.

A temporary constant-value pack resets the read-only binary scale to zero
before the real values are packed. With the original nonzero decimal scale
restored, this makes the CCSDS packer retain that scale instead of choosing
zero. Assertions check both the resulting decimal scale and 100-value count.

Generated with ecCodes 2.49.0 and libaec 1.1.4:

- Original SHA-256: `38ab01684103dbb9368a692f6d90341460354dde2ab77e0396199ffa7c2d90d1`
- Fixture SHA-256: `dbeede1aa3f8b6484d207fa11c0fa84c376ee4fae9ef5502d65872a656b5b719`
