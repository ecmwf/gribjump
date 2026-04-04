//! Thread-safety tests for `GribJump`.
//!
//! These tests verify that `GribJump` works correctly under concurrent access.
//!
//! Run with: `cargo test --test gribjump_thread_safety`
//!
//! For integration tests that require `GribJump` libraries:
//! `cargo test --test gribjump_thread_safety -- --ignored --test-threads=1`

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use fdb::{Fdb, Key};
use gribjump::{ExtractionIterator, ExtractionRequest, ExtractionResult, GribJump, Range};

const GRID_HASH: &str = "33c7d6025995e1b4913811e77d38ec50";

/// Get the path to test fixtures directory.
fn fixtures_dir() -> PathBuf {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    PathBuf::from(manifest_dir).join("tests/fixtures")
}

/// Setup FDB and archive test data for thread-safety tests.
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

// =============================================================================
// Trait bound tests (compile-time verification)
// =============================================================================

/// Test: `GribJump` is Send (can be moved between threads)
#[test]
fn test_gribjump_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<GribJump>();
}

/// Test: `GribJump` is Sync (can be shared between threads via reference)
#[test]
fn test_gribjump_is_sync() {
    fn assert_sync<T: Sync>() {}
    assert_sync::<GribJump>();
}

/// Test: `GribJump` is Clone (with thread-safe feature)
#[test]
fn test_gribjump_is_clone() {
    fn assert_clone<T: Clone>() {}
    assert_clone::<GribJump>();
}

/// Test: `ExtractionIterator` is Send
#[test]
fn test_iterator_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<ExtractionIterator>();
}

/// Test: `ExtractionResult` is Send
#[test]
fn test_result_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<ExtractionResult>();
}

/// Test: `ExtractionRequest` is Send + Sync
#[test]
fn test_extraction_request_traits() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<ExtractionRequest>();
    assert_sync::<ExtractionRequest>();
}

/// Test: `Range` is Send + Sync + Copy
#[test]
fn test_range_traits() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn assert_copy<T: Copy>() {}

    assert_send::<Range>();
    assert_sync::<Range>();
    assert_copy::<Range>();
}

// =============================================================================
// Runtime tests (require GribJump libraries and FDB configuration)
// =============================================================================

/// Test: `GribJump` handle can be created
#[test]
#[ignore = "requires GribJump libraries and FDB configuration"]
fn test_handle_creation() {
    let gj = GribJump::new();
    assert!(gj.is_ok(), "Failed to create GribJump: {:?}", gj.err());
}

/// Test: Cloned `GribJump` can be sent to another thread
#[test]
#[ignore = "requires GribJump libraries and FDB configuration"]
fn test_handle_clone_send() {
    let gj = GribJump::new().expect("failed to create handle");

    // Clone handle and send to another thread
    let gj_clone = gj.clone();
    let handle = thread::spawn(move || {
        // Use the cloned handle in another thread
        let _version = GribJump::version();
        // Use the cloned handle to verify it works
        let _ = gj_clone.axes("class=rd", 1);
    });

    // Use original handle in main thread
    let _version = GribJump::version();
    let _ = gj.axes("class=rd", 1);

    handle.join().expect("thread panicked");
}

/// Test: `GribJump` can be wrapped in Arc and shared
#[test]
#[ignore = "requires GribJump libraries and FDB configuration"]
fn test_arc_sharing() {
    let gj = Arc::new(GribJump::new().expect("failed to create handle"));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let gj = Arc::clone(&gj);
            thread::spawn(move || {
                // Each thread can call methods via Arc
                let _version = GribJump::version();
                let _ = &gj; // Use the handle
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }
}

/// Test: Concurrent axes queries
#[test]
#[ignore = "requires GribJump libraries and FDB configuration"]
fn test_concurrent_axes_queries() {
    let gj = GribJump::new().expect("failed to create handle");

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let gj = gj.clone();
            thread::spawn(move || {
                for _ in 0..10 {
                    // axes() is a read operation
                    let _ = gj.axes("class=rd", 1);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }
}

/// Test: Concurrent extract calls
#[test]
#[ignore = "requires GribJump libraries and FDB configuration"]
fn test_concurrent_extractions() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let _config = setup_test_fdb(tmpdir.path());

    let gj = GribJump::new().expect("failed to create handle");

    let handles: Vec<_> = (1..=4)
        .map(|step| {
            let gj = gj.clone();
            thread::spawn(move || {
                let ranges = vec![Range::new(0, 10).expect("valid range")];
                let request_str = format!(
                    "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step={step},param=151130"
                );
                let requests =
                    vec![ExtractionRequest::new(&request_str, ranges, GRID_HASH)];

                // Each thread does its own extraction - assert success
                let results: Vec<_> = gj.extract(&requests).expect("extract failed").collect();
                assert!(!results.is_empty(), "expected results");
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }
}

/// Test: Mixed read/write operations
#[test]
#[ignore = "requires GribJump libraries and FDB configuration"]
fn test_mixed_operations() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let _config = setup_test_fdb(tmpdir.path());

    let gj = GribJump::new().expect("failed to create handle");

    let mut handles = vec![];

    // Spawn reader threads (axes queries)
    for _ in 0..4 {
        let gj = gj.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..20 {
                let _ = gj.axes("class=rd", 1);
                thread::yield_now();
            }
        }));
    }

    // Spawn writer threads (extractions)
    for step in 1..=2 {
        let gj = gj.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..5 {
                let ranges = vec![Range::new(0, 10).expect("valid range")];
                let request_str = format!(
                    "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step={step},param=151130"
                );
                let requests =
                    vec![ExtractionRequest::new(&request_str, ranges, GRID_HASH)];
                let results: Vec<_> = gj.extract(&requests).expect("extract failed").collect();
                assert!(!results.is_empty(), "expected results");
                thread::yield_now();
            }
        }));
    }

    for h in handles {
        h.join().expect("thread panicked");
    }
}

/// Test: Iterator can be sent to another thread
#[test]
#[ignore = "requires GribJump libraries and FDB configuration"]
fn test_iterator_send_to_thread() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let _config = setup_test_fdb(tmpdir.path());

    let gj = GribJump::new().expect("failed to create handle");

    let ranges = vec![Range::new(0, 10).expect("valid range")];
    let request_str = "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=1,param=151130";
    let requests = vec![ExtractionRequest::new(request_str, ranges, GRID_HASH)];

    // Create iterator on main thread
    let iter = gj.extract(&requests).expect("extract failed");

    // Move iterator to another thread and consume there
    let handle = thread::spawn(move || {
        let results: Vec<_> = iter.collect();
        assert!(!results.is_empty(), "expected results");
    });

    handle.join().expect("thread panicked");
}

/// Test: Calling methods while iterating doesn't deadlock
#[test]
#[ignore = "requires GribJump libraries and FDB configuration"]
fn test_no_deadlock_while_iterating() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let _config = setup_test_fdb(tmpdir.path());

    let gj = GribJump::new().expect("failed to create handle");

    let ranges = vec![Range::new(0, 10).expect("valid range")];
    let request_str = "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=1,param=151130";
    let requests = vec![ExtractionRequest::new(request_str, ranges, GRID_HASH)];

    let iter = gj.extract(&requests).expect("extract failed");

    // While iterating, we should be able to call other methods
    // This tests that the iterator doesn't hold a lock
    let mut count = 0;
    for result in iter {
        let _ = gj.axes("class=rd", 1);
        assert!(result.is_ok(), "expected successful result");
        count += 1;
    }
    assert!(count > 0, "expected at least one result");
}

/// Test: Stress test with many threads
#[test]
#[ignore = "requires GribJump libraries and FDB configuration"]
fn test_stress_concurrent_access() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let _config = setup_test_fdb(tmpdir.path());

    let gj = GribJump::new().expect("failed to create handle");
    let iterations = 50;
    let thread_count = 16;

    let handles: Vec<_> = (0..thread_count)
        .map(|i| {
            let gj = gj.clone();
            thread::spawn(move || {
                for j in 0..iterations {
                    if (i + j) % 3 == 0 {
                        // Read operation
                        let _ = gj.axes("class=rd", 1);
                    } else {
                        // Extract operation - use step based on thread/iteration
                        let step = ((i + j) % 4) + 1;
                        let ranges = vec![Range::new(0, 5).expect("valid range")];
                        let request_str = format!(
                            "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step={step},param=151130"
                        );
                        let requests = vec![
                            ExtractionRequest::new(&request_str, ranges, GRID_HASH)
                        ];
                        let results: Vec<_> =
                            gj.extract(&requests).expect("extract failed").collect();
                        assert!(!results.is_empty(), "expected results");
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked during stress test");
    }
}

/// Test: Concurrent errors don't cause crashes
#[test]
#[ignore = "requires GribJump libraries and FDB configuration"]
fn test_concurrent_errors_no_crash() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let _config = setup_test_fdb(tmpdir.path());

    let gj = GribJump::new().expect("failed to create handle");

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let gj = gj.clone();
            thread::spawn(move || {
                for _ in 0..20 {
                    // Use invalid requests to trigger errors (invalid MARS request format)
                    let ranges = vec![Range::new(0, 10).expect("valid range")];
                    let requests = vec![ExtractionRequest::new(
                        format!("INVALID_REQUEST_THREAD_{i}"),
                        ranges,
                        GRID_HASH,
                    )];
                    // Ignore the error - testing that concurrent errors don't crash
                    let _ = gj.extract(&requests);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Thread panicked");
    }
}
