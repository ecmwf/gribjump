//! Async integration tests for `GribJump`.
//!
//! These tests verify correct concurrent access from multiple tokio tasks.
//!
//! Note: `GribJump` uses internal `Arc<Mutex<>>`, so methods take `&self` and the type is `Clone`.
//! Can share via `gj.clone()`.
//!
//! Run with: `cargo test --test gribjump_async -- --ignored --test-threads=1`

use std::env;
use std::fs;
use std::path::PathBuf;

use fdb::{Fdb, Key};
use gribjump::{ExtractionRequest, ExtractionResult, GribJump, Range};
use tokio::task::JoinSet;

const GRID_HASH: &str = "33c7d6025995e1b4913811e77d38ec50";

/// Get the path to test fixtures directory.
fn fixtures_dir() -> PathBuf {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    PathBuf::from(manifest_dir).join("tests/fixtures")
}

/// Setup FDB and archive test data for `GribJump` tests.
fn setup_test_fdb(tmpdir: &std::path::Path) -> String {
    let schema_src = fixtures_dir().join("schema");
    let schema_dst = tmpdir.join("schema");
    fs::copy(&schema_src, &schema_dst).expect("failed to copy schema");

    let config = format!(
        r"---
type: local
engine: toc
schema: {}/schema
spaces:
  - roots:
      - path: {}
",
        tmpdir.display(),
        tmpdir.display()
    );

    // Set FDB5_CONFIG for GribJump
    unsafe {
        env::set_var("FDB5_CONFIG", &config);
    }

    // Archive test data
    let fdb = Fdb::from_yaml(&config).expect("failed to create FDB");
    let grib_data = fs::read(fixtures_dir().join("synth11.grib")).expect("failed to read GRIB");

    // Archive multiple steps for concurrent extraction tests
    for step in 1..=4 {
        let key = Key::new()
            .with("class", "rd")
            .with("expver", "xxxx")
            .with("stream", "oper")
            .with("date", "20230508")
            .with("time", "1200")
            .with("type", "fc")
            .with("levtype", "sfc")
            .with("step", &step.to_string())
            .with("param", "151130");

        fdb.archive(&key, &grib_data).expect("archive failed");
    }

    fdb.flush().expect("flush failed");
    config
}

#[tokio::test]
#[ignore = "requires GribJump libraries and thread-safe feature"]
async fn test_gribjump_concurrent_extract() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let _config = setup_test_fdb(tmpdir.path());

    // With thread-safe feature, GribJump is Clone (shares internal Arc<Mutex>)
    let gj = GribJump::new().expect("failed to create GribJump");

    let mut tasks = JoinSet::new();

    // Spawn multiple tasks that extract data concurrently
    for step in 1..=4 {
        let gj = gj.clone(); // Clone shares the internal Arc<Mutex<Handle>>

        tasks.spawn(async move {
            let ranges = vec![Range::new(0, 5).expect("valid range")];
            let request_str = format!(
                "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step={step},param=151130"
            );

            let request = ExtractionRequest::new(&request_str, ranges, GRID_HASH);

            // With thread-safe feature, extract takes &self and uses internal locking
            let results: Vec<_> = gj.extract(&[request]).expect("extract failed").collect();

            let total_values: usize = results
                .iter()
                .filter_map(|r| r.as_ref().ok())
                .map(ExtractionResult::total_values)
                .sum();

            (step, total_values)
        });
    }

    let mut results = Vec::new();
    while let Some(result) = tasks.join_next().await {
        results.push(result.expect("task panicked"));
    }

    assert_eq!(results.len(), 4);
    for (step, values) in &results {
        assert!(*values > 0, "step {step} should have values");
        println!("Step {step}: extracted {values} values");
    }

    drop(gj);
    drop(tmpdir);
}

#[tokio::test]
#[ignore = "requires GribJump libraries and thread-safe feature"]
async fn test_gribjump_concurrent_axes() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let _config = setup_test_fdb(tmpdir.path());

    let gj = GribJump::new().expect("failed to create GribJump");

    let mut tasks = JoinSet::new();

    // Spawn multiple tasks that query axes concurrently
    for i in 0..4 {
        let gj = gj.clone();

        tasks.spawn(async move {
            let axes = gj
                .axes("class=rd,expver=xxxx,stream=oper", 3)
                .expect("axes query failed");
            (i, axes.len())
        });
    }

    let mut results = Vec::new();
    while let Some(result) = tasks.join_next().await {
        results.push(result.expect("task panicked"));
    }

    assert_eq!(results.len(), 4);
    // All tasks should see the same axes
    let first_count = results[0].1;
    assert!(results.iter().all(|(_, c)| *c == first_count));
    println!("Concurrent axes: all tasks found {first_count} axis entries");

    drop(gj);
    drop(tmpdir);
}

#[tokio::test]
#[ignore = "requires GribJump libraries and thread-safe feature"]
async fn test_gribjump_spawn_blocking_pattern() {
    // Test the recommended pattern for using GribJump in async code:
    // use spawn_blocking for blocking operations
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let _config = setup_test_fdb(tmpdir.path());

    let gj = GribJump::new().expect("failed to create GribJump");

    // Extract using spawn_blocking
    let gj_clone = gj.clone();
    let result = tokio::task::spawn_blocking(move || {
        let ranges = vec![Range::new(0, 10).expect("valid range")];
        let request_str =
            "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=1,param=151130";

        let request = ExtractionRequest::new(request_str, ranges, GRID_HASH);

        let results: Vec<_> = gj_clone.extract(&[request]).expect("extract failed").collect();

        results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(ExtractionResult::total_values)
            .sum::<usize>()
    })
    .await
    .expect("spawn_blocking failed");

    assert!(result > 0);
    println!("spawn_blocking pattern: extracted {result} values");

    // Query axes using spawn_blocking
    let gj_clone = gj.clone();
    let axes_count = tokio::task::spawn_blocking(move || {
        let axes = gj_clone
            .axes("class=rd,expver=xxxx,stream=oper", 3)
            .expect("axes query failed");
        axes.len()
    })
    .await
    .expect("spawn_blocking failed");

    println!("spawn_blocking pattern: found {axes_count} axis entries");

    drop(gj);
    drop(tmpdir);
}

#[tokio::test]
#[ignore = "requires GribJump libraries and thread-safe feature"]
async fn test_gribjump_mixed_concurrent_operations() {
    // Test mixing different operations concurrently
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let _config = setup_test_fdb(tmpdir.path());

    let gj = GribJump::new().expect("failed to create GribJump");

    let mut tasks = JoinSet::new();

    // Task 1: Extract step 1
    let gj1 = gj.clone();
    tasks.spawn(async move {
        let ranges = vec![Range::new(0, 5).expect("valid range")];
        let request = ExtractionRequest::new(
            "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=1,param=151130",
            ranges,
            GRID_HASH,
        );

        let results: Vec<_> = gj1.extract(&[request]).expect("extract failed").collect();
        ("extract_1", results.len())
    });

    // Task 2: Extract step 2
    let gj2 = gj.clone();
    tasks.spawn(async move {
        let ranges = vec![Range::new(0, 5).expect("valid range")];
        let request = ExtractionRequest::new(
            "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=2,param=151130",
            ranges,
            GRID_HASH,
        );

        let results: Vec<_> = gj2.extract(&[request]).expect("extract failed").collect();
        ("extract_2", results.len())
    });

    // Task 3: Query axes
    let gj3 = gj.clone();
    tasks.spawn(async move {
        let axes = gj3
            .axes("class=rd,expver=xxxx,stream=oper", 3)
            .expect("axes failed");
        ("axes", axes.len())
    });

    // Task 4: Get version (fast, non-blocking)
    tasks.spawn(async move {
        let version = GribJump::version();
        ("version", usize::from(!version.is_empty()))
    });

    let mut results = Vec::new();
    while let Some(result) = tasks.join_next().await {
        results.push(result.expect("task panicked"));
    }

    assert_eq!(results.len(), 4);
    for (op, count) in &results {
        println!("{op}: {count}");
    }

    drop(gj);
    drop(tmpdir);
}

#[tokio::test]
#[ignore = "requires GribJump libraries and thread-safe feature"]
async fn test_gribjump_high_concurrency() {
    // Stress test with many concurrent tasks
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let _config = setup_test_fdb(tmpdir.path());

    let gj = GribJump::new().expect("failed to create GribJump");

    let mut tasks = JoinSet::new();
    let num_tasks = 16;

    for i in 0..num_tasks {
        let gj = gj.clone();
        let step = (i % 4) + 1;

        tasks.spawn(async move {
            let ranges = vec![Range::new(0, 3).expect("valid range")];
            let request_str = format!(
                "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step={step},param=151130"
            );

            let request = ExtractionRequest::new(&request_str, ranges, GRID_HASH);

            let results: Vec<_> = gj.extract(&[request]).expect("extract failed").collect();
            let values: usize = results
                .iter()
                .filter_map(|r| r.as_ref().ok())
                .map(ExtractionResult::total_values)
                .sum();

            (i, values)
        });
    }

    let mut completed = 0;
    while let Some(result) = tasks.join_next().await {
        let (task_id, values) = result.expect("task panicked");
        assert!(values > 0, "task {task_id} should extract values");
        completed += 1;
    }

    assert_eq!(completed, num_tasks);
    println!("High concurrency test: {completed} tasks completed successfully");

    drop(gj);
    drop(tmpdir);
}
