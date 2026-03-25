//! Benchmarks for the gribjump crate.
//!
//! Run with: `cargo bench --package gribjump`
//!
//! Note: These benchmarks require `GribJump` libraries to be available.
//! Most benchmarks require FDB setup because eckit's `Main()` singleton must be initialized.
//! Benchmarks will be skipped if FDB setup fails.
//! Benchmark names use `rust_` prefix for comparison with C++ benchmarks.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use gribjump::{ExtractionRequest, GribJump, Range};
use std::sync::OnceLock;

// FDB setup for extraction benchmarks
mod fdb_setup {
    use fdb::{Fdb, Key};
    use std::env;
    use std::fs;
    use std::path::PathBuf;

    pub struct TestFdb;

    fn project_root() -> PathBuf {
        let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        PathBuf::from(manifest_dir)
            .parent()
            .expect("parent dir")
            .parent()
            .expect("grandparent dir")
            .to_path_buf()
    }

    pub fn setup() -> Option<TestFdb> {
        let root = project_root();
        let fdb_dir = root.join("target/bench-fdb");
        let fixtures_dir = root.join("tests/fixtures");

        // Create fixed directory
        fs::create_dir_all(&fdb_dir).ok()?;

        // Copy schema if not exists
        let schema_src = fixtures_dir.join("schema");
        let schema_dst = fdb_dir.join("schema");
        if !schema_dst.exists() {
            fs::copy(&schema_src, &schema_dst).ok()?;
        }

        let config = format!(
            "---\ntype: local\nengine: toc\nschema: {}/schema\nspaces:\n  - roots:\n      - path: {}\n",
            fdb_dir.display(),
            fdb_dir.display()
        );

        // Save config for C++ benchmarks
        fs::write(fdb_dir.join("fdb5_config.yaml"), &config).ok()?;

        // Set FDB config
        unsafe {
            env::set_var("FDB5_CONFIG", &config);
        }

        let fdb = Fdb::from_yaml(&config).ok()?;

        // Read test GRIB data
        let grib_path = fixtures_dir.join("synth11.grib");
        let grib_data = fs::read(&grib_path).ok()?;

        // Archive with keys matching the test data
        let key = Key::new()
            .with("class", "rd")
            .with("expver", "xxxx")
            .with("stream", "oper")
            .with("date", "20230508")
            .with("time", "1200")
            .with("type", "fc")
            .with("levtype", "sfc")
            .with("step", "1")
            .with("param", "151130");

        fdb.archive(&key, &grib_data).ok()?;
        let _ = fdb.flush();

        Some(TestFdb)
    }
}

static FDB_SETUP: OnceLock<Option<fdb_setup::TestFdb>> = OnceLock::new();

fn get_fdb_setup() -> Option<&'static fdb_setup::TestFdb> {
    FDB_SETUP.get_or_init(fdb_setup::setup).as_ref()
}

/// Benchmark `GribJump` handle creation.
/// Note: Requires FDB setup to initialize eckit `Main()` singleton.
fn bench_handle_creation(c: &mut Criterion) {
    // Ensure eckit is initialized via FDB setup
    let Some(_fdb) = get_fdb_setup() else {
        eprintln!("Skipping handle_creation benchmark: FDB setup failed (eckit not initialized)");
        return;
    };

    c.bench_function("rust_handle_creation", |b| {
        b.iter(|| black_box(GribJump::new().expect("failed to create handle")));
    });
}

/// Benchmark version string retrieval.
/// Note: Requires FDB setup to initialize eckit `Main()` singleton.
fn bench_version(c: &mut Criterion) {
    // Ensure eckit is initialized via FDB setup
    let Some(_fdb) = get_fdb_setup() else {
        eprintln!("Skipping version benchmark: FDB setup failed (eckit not initialized)");
        return;
    };

    c.bench_function("rust_version", |b| {
        b.iter(|| black_box(GribJump::version()));
    });
}

/// Benchmark `ExtractionRequest` creation.
fn bench_request_creation(c: &mut Criterion) {
    let ranges = vec![
        Range::new(0, 10).expect("valid range"),
        Range::new(20, 30).expect("valid range"),
    ];

    c.bench_function("rust_request_creation", |b| {
        b.iter(|| {
            black_box(ExtractionRequest::new(
                "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200",
                ranges.clone(),
            ))
        });
    });
}

/// Benchmark Range creation.
fn bench_range_creation(c: &mut Criterion) {
    c.bench_function("rust_range_creation", |b| {
        b.iter(|| black_box(Range::new(0, 100).expect("valid range")));
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
            ))
        });
    });
}

/// Benchmark extraction (requires FDB setup).
fn bench_extract(c: &mut Criterion) {
    let Some(_fdb) = get_fdb_setup() else {
        eprintln!("Skipping extraction benchmark: FDB setup failed");
        return;
    };

    let gj = GribJump::new().expect("failed to create GribJump handle");
    let gridhash = "33c7d6025995e1b4913811e77d38ec50";
    let ranges = vec![Range::new(0, 5).expect("valid range")];

    c.bench_function("rust_extract", |b| {
        b.iter(|| {
            let request = ExtractionRequest::new(
                "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=1,param=151130",
                ranges.clone(),
            )
            .with_grid_hash(gridhash);

            let results: Vec<_> = gj.extract(&[request]).expect("extraction failed").collect();
            black_box(results)
        });
    });
}

/// Benchmark extraction with result processing.
fn bench_extract_with_values(c: &mut Criterion) {
    let Some(_fdb) = get_fdb_setup() else {
        eprintln!("Skipping extraction+values benchmark: FDB setup failed");
        return;
    };

    let gj = GribJump::new().expect("failed to create GribJump handle");
    let gridhash = "33c7d6025995e1b4913811e77d38ec50";
    let ranges = vec![Range::new(0, 5).expect("valid range")];

    c.bench_function("rust_extract_with_values", |b| {
        b.iter(|| {
            let request = ExtractionRequest::new(
                "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=1,param=151130",
                ranges.clone(),
            )
            .with_grid_hash(gridhash);

            let mut total_values = 0usize;
            for result in gj.extract(&[request]).expect("extraction failed") {
                let result = result.expect("failed to get result");
                total_values += result.total_values();
            }
            black_box(total_values)
        });
    });
}

/// Benchmark extraction with larger range.
fn bench_extract_large(c: &mut Criterion) {
    let Some(_fdb) = get_fdb_setup() else {
        eprintln!("Skipping extract_large benchmark: FDB setup failed");
        return;
    };

    let gj = GribJump::new().expect("failed to create GribJump handle");
    let gridhash = "33c7d6025995e1b4913811e77d38ec50";
    // Extract 100 values instead of 5
    let ranges = vec![Range::new(0, 100).expect("valid range")];

    c.bench_function("rust_extract_large", |b| {
        b.iter(|| {
            let request = ExtractionRequest::new(
                "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=1,param=151130",
                ranges.clone(),
            )
            .with_grid_hash(gridhash);

            let mut total_values = 0usize;
            for result in gj.extract(&[request]).expect("extraction failed") {
                let result = result.expect("failed to get result");
                total_values += result.total_values();
            }
            black_box(total_values)
        });
    });
}

/// Benchmark axes query.
fn bench_axes(c: &mut Criterion) {
    let Some(_fdb) = get_fdb_setup() else {
        eprintln!("Skipping axes benchmark: FDB setup failed");
        return;
    };

    let gj = GribJump::new().expect("failed to create GribJump handle");

    c.bench_function("rust_axes", |b| {
        b.iter(|| {
            let axes = gj
                .axes("class=rd,expver=xxxx,stream=oper", 3)
                .expect("axes query failed");
            black_box(axes)
        });
    });
}

/// Benchmark `ExtractionResult` construction and value access.
/// This measures the overhead of copying data from C++ to Rust.
fn bench_result_processing(c: &mut Criterion) {
    let Some(_fdb) = get_fdb_setup() else {
        eprintln!("Skipping result_processing benchmark: FDB setup failed");
        return;
    };

    let gj = GribJump::new().expect("failed to create GribJump handle");
    let gridhash = "33c7d6025995e1b4913811e77d38ec50";
    // Multiple ranges to stress the copy overhead
    let ranges: Vec<Range> = (0..10)
        .map(|i| Range::new(i * 10, i * 10 + 9).expect("valid range"))
        .collect();

    c.bench_function("rust_result_processing", |b| {
        b.iter(|| {
            let request = ExtractionRequest::new(
                "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=1,param=151130",
                ranges.clone(),
            )
            .with_grid_hash(gridhash);

            let mut sum = 0.0f64;
            for result in gj.extract(&[request]).expect("extraction failed") {
                let result = result.expect("failed to get result");
                // Access all values to measure full processing cost
                for range in result.iter() {
                    sum += range.values().iter().sum::<f64>();
                }
            }
            black_box(sum)
        });
    });
}

criterion_group!(
    benches,
    bench_handle_creation,
    bench_version,
    bench_request_creation,
    bench_range_creation,
    bench_request_many_ranges,
    bench_extract,
    bench_extract_with_values,
    bench_extract_large,
    bench_axes,
    bench_result_processing,
);

criterion_main!(benches);
