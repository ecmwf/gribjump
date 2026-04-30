//! Benchmarks for the gribjump crate.
//!
//! Run with: `cargo bench --package gribjump`
//!
//! These benchmarks measure FFI overhead, not gribjump core performance.
//! Benchmark names use `rust_` prefix for comparison with C++ benchmarks.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gribjump::{ExtractionRequest, GribJump, Range};

/// Benchmark `GribJump` handle creation.
fn bench_handle_creation(c: &mut Criterion) {
    c.bench_function("rust_handle_creation", |b| {
        b.iter(|| black_box(GribJump::new().expect("failed to create handle")));
    });
}

/// Benchmark `ExtractionRequest` creation.
fn bench_request_creation(c: &mut Criterion) {
    let ranges = vec![
        Range::new(0, 11).expect("valid range"),
        Range::new(20, 31).expect("valid range"),
    ];

    c.bench_function("rust_request_creation", |b| {
        b.iter(|| {
            black_box(ExtractionRequest::new(
                "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200",
                ranges.clone(),
                "gridhash",
            ))
        });
    });
}

/// Benchmark Range creation.
fn bench_range_creation(c: &mut Criterion) {
    c.bench_function("rust_range_creation", |b| {
        b.iter(|| black_box(Range::new(0, 101).expect("valid range")));
    });
}

/// Benchmark request creation with many ranges.
fn bench_request_many_ranges(c: &mut Criterion) {
    let ranges: Vec<Range> = (0..100)
        .map(|i| Range::new(i * 10, i * 10 + 5).expect("valid range"))
        .collect();

    c.bench_function("rust_request_100_ranges", |b| {
        b.iter(|| {
            black_box(ExtractionRequest::new(
                "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200",
                ranges.clone(),
                "gridhash",
            ))
        });
    });
}

criterion_group!(
    benches,
    bench_handle_creation,
    bench_request_creation,
    bench_range_creation,
    bench_request_many_ranges,
);

criterion_main!(benches);
