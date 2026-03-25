//! Thread-safety tests for `GribJump`.
//!
//! These tests verify that `GribJump` works correctly under concurrent access.
//!
//! Run with: `cargo test --test gribjump_thread_safety --features thread-safe`
//!
//! For integration tests that require `GribJump` libraries:
//! `cargo test --test gribjump_thread_safety --features thread-safe -- --ignored --test-threads=1`

use std::sync::Arc;
use std::thread;

use gribjump::{ExtractionIterator, ExtractionRequest, ExtractionResult, GribJump, Range};

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
    let gj = GribJump::new().expect("failed to create handle");

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let gj = gj.clone();
            thread::spawn(move || {
                let ranges = vec![Range::new(0, 10).expect("valid range")];
                let requests = vec![ExtractionRequest::new("class=rd,expver=xxxx", ranges)];

                // Each thread does its own extraction
                let _ = gj.extract(&requests);
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
    for _ in 0..2 {
        let gj = gj.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..5 {
                let ranges = vec![Range::new(0, 10).expect("valid range")];
                let requests = vec![ExtractionRequest::new("class=rd", ranges)];
                let _ = gj.extract(&requests);
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
    let gj = GribJump::new().expect("failed to create handle");

    let ranges = vec![Range::new(0, 10).expect("valid range")];
    let requests = vec![ExtractionRequest::new("class=rd", ranges)];

    // Create iterator on main thread
    if let Ok(iter) = gj.extract(&requests) {
        // Move iterator to another thread and consume there
        let handle = thread::spawn(move || {
            for result in iter {
                let _ = result;
            }
        });

        handle.join().expect("thread panicked");
    }
}

/// Test: Calling methods while iterating doesn't deadlock
#[test]
#[ignore = "requires GribJump libraries and FDB configuration"]
fn test_no_deadlock_while_iterating() {
    let gj = GribJump::new().expect("failed to create handle");

    let ranges = vec![Range::new(0, 10).expect("valid range")];
    let requests = vec![ExtractionRequest::new("class=rd", ranges)];

    if let Ok(iter) = gj.extract(&requests) {
        // While iterating, we should be able to call other methods
        // This tests that the iterator doesn't hold a lock
        for result in iter {
            let _ = gj.axes("class=rd", 1);
            let _ = result;
        }
    }
}

/// Test: Stress test with many threads
#[test]
#[ignore = "requires GribJump libraries and FDB configuration"]
fn test_stress_concurrent_access() {
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
                        // Write operation
                        let ranges = vec![Range::new(0, 5).expect("valid range")];
                        let requests = vec![ExtractionRequest::new("class=rd", ranges)];
                        let _ = gj.extract(&requests);
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
    let gj = GribJump::new().expect("failed to create handle");

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let gj = gj.clone();
            thread::spawn(move || {
                for _ in 0..20 {
                    // Use invalid requests to trigger errors
                    let ranges = vec![Range::new(0, 10).expect("valid range")];
                    let requests = vec![ExtractionRequest::new(
                        format!("INVALID_REQUEST_THREAD_{i}"),
                        ranges,
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
