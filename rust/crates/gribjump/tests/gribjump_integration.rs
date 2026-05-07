//! Integration tests for `GribJump` safe wrapper.
//!
//! Run with `cargo test --test gribjump_integration`. Each test
//! spins up its own temp FDB + `GribJump` config so they're
//! self-contained.

use std::env;
use std::fs;
use std::path::PathBuf;

use std::collections::BTreeSet;

use fdb::{Fdb, Request};
use gribjump::{ExtractionRequest, FileExtraction, GribJump, Range};

/// Get the path to test fixtures directory.
fn fixtures_dir() -> PathBuf {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    PathBuf::from(manifest_dir).join("tests/fixtures")
}

/// Create a temporary FDB configuration for testing.
/// Copies the schema file to the temp directory since FDB resolves paths relatively.
fn create_test_config(tmpdir: &std::path::Path) -> String {
    // Copy schema to temp directory
    let schema_src = fixtures_dir().join("schema");
    let schema_dst = tmpdir.join("schema");
    fs::copy(&schema_src, &schema_dst).expect("failed to copy schema");

    format!(
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
    )
}

/// Set up a test FDB with `extract_ranges.grib` (matching C++ `test_api.cc`).
/// This file contains 3 messages with `step=1,2,3`.
fn setup_test_fdb_extract_ranges(tmpdir: &std::path::Path) -> String {
    let config = create_test_config(tmpdir);
    let fdb = Fdb::open(Some(config.as_str()), None).expect("failed to create FDB");

    // Read extract_ranges.grib - contains 3 messages with step=1,2,3
    let grib_path = fixtures_dir().join("extract_ranges.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read extract_ranges.grib");

    // Archive with fileHandle like C++ does: fdb.archive(*path.fileHandle())
    // This archives the raw GRIB data and FDB extracts metadata from it
    fdb.archive_raw(&grib_data).expect("failed to archive data");
    fdb.flush().expect("flush failed");

    config
}

/// Set up a test FDB with `axes.grib` (matching C++ `test_api_axes.cc`).
/// This file contains 6 messages with different dates and steps.
fn setup_test_fdb_axes(tmpdir: &std::path::Path) -> String {
    let config = create_test_config(tmpdir);
    let fdb = Fdb::open(Some(config.as_str()), None).expect("failed to create FDB");

    // Read axes.grib - contains messages for axes testing
    let grib_path = fixtures_dir().join("axes.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read axes.grib");

    // Archive using raw GRIB data
    fdb.archive_raw(&grib_data).expect("failed to archive data");
    fdb.flush().expect("flush failed");

    config
}

// Grid hash for test GRIB files (from C++ tests)
const GRID_HASH: &str = "33c7d6025995e1b4913811e77d38ec50";

//----------------------------------------------------------------------------------------------------------------------
// Basic API tests
//----------------------------------------------------------------------------------------------------------------------

#[test]
fn test_gribjump_version() {
    let version = gribjump::version();
    assert!(!version.is_empty());
    println!("GribJump version: {version}");
}

#[test]
fn test_gribjump_git_sha1() {
    let sha = gribjump::git_sha1();
    assert!(!sha.is_empty());
    println!("GribJump git SHA1: {sha}");
}

#[test]
fn test_gribjump_handle_creation() {
    let gj = GribJump::new();
    assert!(
        gj.is_ok(),
        "failed to create GribJump handle: {:?}",
        gj.err()
    );
}

#[test]
fn test_range_creation() {
    let range = Range::new(0, 100);
    assert!(range.is_ok());

    let range = range.expect("valid range");
    assert_eq!(range.start, 0);
    assert_eq!(range.end, 100);
}

#[test]
fn test_range_invalid() {
    // End before start should fail
    let range = Range::new(100, 50);
    assert!(range.is_err());
}

#[test]
fn test_extraction_request_creation() {
    let ranges = vec![
        Range::new(0, 11).expect("valid range"),
        Range::new(20, 31).expect("valid range"),
    ];

    let request = ExtractionRequest::new(
        "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,type=fc,levtype=sfc,step=1,param=151130",
        ranges,
        GRID_HASH,
    );

    assert!(!request.request_str.is_empty());
    assert_eq!(request.ranges.len(), 2);
    assert_eq!(request.grid_hash, GRID_HASH);
}

// TODO: test_gribjump_api_extract - requires proper test data with known numberOfValues
// The extract_ranges.grib from ECMWF test server has limited data points.
// C++ tests use ecbuild_get_test_multidata which may provide different data.

// TODO: test_gribjump_api_extract_mars - requires MarsParser support in bridge
// Currently the bridge uses MarsRequest constructor which doesn't parse MARS language.
// Need to modify gribjump_bridge.cpp to use metkit::mars::MarsParser.

//----------------------------------------------------------------------------------------------------------------------
// test_api.cc: Test 1.b - Grid hash validation
//----------------------------------------------------------------------------------------------------------------------

#[test]
fn test_gribjump_api_extract_hash_validation() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = setup_test_fdb_extract_ranges(tmpdir.path());
    // GribJump discovers FDB via this env var (its constructor takes no config param)
    unsafe {
        env::set_var("FDB5_CONFIG", &config);
    }

    #[allow(unused_mut)] // Methods take &mut self without thread-safe, &self with it
    let mut gj = GribJump::new().expect("failed to create GribJump handle");

    let ranges = vec![
        Range::new(0, 6).expect("valid range"),
        Range::new(10, 16).expect("valid range"),
    ];
    let request_str = "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc";

    // Test 1: Request with wrong hash should fail
    let request_wrong_hash = ExtractionRequest::new(request_str, ranges.clone(), "wronghash");
    let result = gj.extract(&[request_wrong_hash]);
    assert!(result.is_err(), "expected error with wrong hash");
    println!("Extract with wrong hash: error (as expected)");

    // Test 2: Request with correct hash should succeed
    let request_correct_hash = ExtractionRequest::new(request_str, ranges.clone(), GRID_HASH);
    let result = gj.extract(&[request_correct_hash]);
    assert!(result.is_ok(), "expected success with correct hash");

    let results: Vec<_> = result.expect("extraction should succeed").collect();
    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok());

    // Verify we got values (range counts may vary based on data)
    let r = results[0].as_ref().expect("result should be ok");
    let total: usize = r.iter().map(|rr| rr.len()).sum();
    assert!(total > 0, "expected some values");
    println!("Extract with correct hash: {total} values");

    // Test 3: Request with empty hash should fail (core validates, respects ignoreGridHash config)
    let request_empty_hash = ExtractionRequest::new(request_str, ranges.clone(), "");
    let result = gj.extract(&[request_empty_hash]);
    match result {
        Err(err) => {
            let err_msg = err.to_string();
            assert!(
                err_msg.contains("Grid hash") || err_msg.contains("BadValue"),
                "expected BadValue error about grid hash, got: {err_msg}"
            );
            println!("Extract with empty hash: error as expected - {err_msg}");
        }
        Ok(_) => panic!("expected error with empty hash"),
    }

    // Test 4: With GRIBJUMP_IGNORE_GRID=1, empty hash should succeed
    unsafe {
        env::set_var("GRIBJUMP_IGNORE_GRID", "1");
    }
    let request_empty_hash_ignored = ExtractionRequest::new(request_str, ranges, "");
    let result = gj.extract(&[request_empty_hash_ignored]);
    assert!(
        result.is_ok(),
        "expected success with empty hash when ignoreGridHash is set"
    );
    let results: Vec<_> = result.expect("extraction should succeed").collect();
    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok());
    println!("Extract with empty hash (ignoreGridHash=true): success");
    unsafe {
        env::remove_var("GRIBJUMP_IGNORE_GRID");
    }

    println!("test_gribjump_api_extract_hash_validation completed");
}

//----------------------------------------------------------------------------------------------------------------------
// test_api_axes.cc: Axes query
//----------------------------------------------------------------------------------------------------------------------

#[test]
fn test_gribjump_api_axes() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = setup_test_fdb_axes(tmpdir.path());
    unsafe {
        env::set_var("FDB5_CONFIG", &config);
    }

    #[allow(unused_mut)] // Methods take &mut self without thread-safe, &self with it
    let mut gj = GribJump::new().expect("failed to create GribJump handle");

    // Query axes matching C++ test_api_axes.cc
    let axes = gj
        .axes("class=rd,expver=xxxx", 3)
        .expect("axes query failed");

    assert!(!axes.is_empty(), "expected non-empty axes");

    // Print the contents (matching C++ test output)
    for (key, values) in &axes {
        print!("{key}: ");
        for value in values {
            print!("{value}, ");
        }
        println!();
    }

    // Expected values from C++ test_api_axes.cc
    // Note: axes.grib has different dates (20230508, 20230509) and steps (1, 2, 3)
    assert!(axes.contains_key("class"), "expected 'class' axis");
    assert!(axes.contains_key("date"), "expected 'date' axis");
    assert!(axes.contains_key("expver"), "expected 'expver' axis");
    assert!(axes.contains_key("step"), "expected 'step' axis");

    // Verify class values
    if let Some(class_values) = axes.get("class") {
        assert!(class_values.iter().any(|v| v == "rd"), "expected class=rd");
    }

    // Verify date values
    if let Some(date_values) = axes.get("date") {
        assert!(
            date_values
                .iter()
                .any(|v| v == "20230508" || v == "20230509"),
            "expected date values"
        );
    }

    // Verify step values
    if let Some(step_values) = axes.get("step") {
        assert!(
            step_values.iter().any(|v| v == "1" || v == "2" || v == "3"),
            "expected step values"
        );
    }

    println!("test_gribjump_api_axes completed");
}

//----------------------------------------------------------------------------------------------------------------------
// Additional API tests
//----------------------------------------------------------------------------------------------------------------------

/// Test `scan_paths` API
#[test]
fn test_gribjump_scan_paths() {
    #[allow(unused_mut)] // Methods take &mut self without thread-safe, &self with it
    let mut gj = GribJump::new().expect("failed to create GribJump handle");

    let grib_path = fixtures_dir().join("extract_ranges.grib");
    let paths = vec![grib_path.to_string_lossy().to_string()];

    let result = gj.scan_paths(&paths);
    println!("scan_paths result: {result:?}");

    assert!(result.is_ok(), "scan_paths should not error");
    let count = result.expect("scan_paths should succeed");
    println!("Scanned {count} messages");
}

/// Test `print_stats` API
#[test]
fn test_gribjump_print_stats() {
    #[allow(unused_mut)] // Methods take &mut self without thread-safe, &self with it
    let mut gj = GribJump::new().expect("failed to create GribJump handle");

    // Just verify it doesn't crash
    gj.print_stats().expect("print_stats failed");
    println!("print_stats completed");
}

/// Test `extract_from_paths` API
#[test]
fn test_gribjump_extract_from_paths() {
    use gribjump::PathExtractionRequest;

    let gj = GribJump::new().expect("failed to create GribJump handle");

    let grib_path = fixtures_dir().join("extract_ranges.grib");
    let path_str = grib_path.to_string_lossy().to_string();

    // First scan the path so gribjump knows about it
    gj.scan_paths(&[&path_str]).expect("scan_paths failed");

    let ranges = vec![Range::new(0, 6).expect("valid range")];

    let request = PathExtractionRequest::new(&path_str, ranges, GRID_HASH);
    let iter = gj
        .extract_from_paths(&[request])
        .expect("extract_from_paths failed");

    // Verify we get results
    let results: Vec<_> = iter.collect();
    assert!(!results.is_empty(), "expected at least one result");

    // Verify each result is successful
    for result in results {
        let extraction = result.expect("extraction result failed");
        assert!(extraction.num_ranges() > 0, "expected ranges in result");
    }
}

/// Test `scan_requests` API
#[test]
fn test_gribjump_scan_requests() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = setup_test_fdb_extract_ranges(tmpdir.path());
    unsafe {
        env::set_var("FDB5_CONFIG", &config);
    }

    let gj = GribJump::new().expect("failed to create GribJump handle");

    // Scan by MARS request
    let requests = vec!["class=rd,expver=xxxx"];
    let result = gj.scan_requests(&requests, false);
    println!("scan_requests result: {result:?}");

    // Also test with by_files=true
    let result2 = gj.scan_requests(&requests, true);
    println!("scan_requests (by_files=true) result: {result2:?}");
}

/// Test `ExtractionResult` helper methods
#[test]
fn test_gribjump_extraction_result_methods() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = setup_test_fdb_extract_ranges(tmpdir.path());
    unsafe {
        env::set_var("FDB5_CONFIG", &config);
    }

    let gj = GribJump::new().expect("failed to create GribJump handle");

    let ranges = vec![
        Range::new(0, 6).expect("valid range"),
        Range::new(10, 16).expect("valid range"),
    ];
    let request_str = "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc";
    let request = ExtractionRequest::new(request_str, ranges, GRID_HASH);

    let result = gj.extract(&[request]).expect("extract failed");
    let results: Vec<_> = result.collect();

    assert!(!results.is_empty(), "expected at least one result");

    let extraction_result = results
        .into_iter()
        .next()
        .expect("expected result")
        .expect("result should be ok");

    // Test num_ranges()
    let num_ranges = extraction_result.num_ranges();
    println!("num_ranges: {num_ranges}");

    // Test is_empty()
    let is_empty = extraction_result.is_empty();
    println!("is_empty: {is_empty}");
    assert!(!is_empty || num_ranges == 0);

    // Test total_values()
    let total = extraction_result.total_values();
    println!("total_values: {total}");

    // Test range() and get()
    if num_ranges > 0 {
        let range_view = extraction_result.range(0).expect("should have range 0");
        let range_view2 = extraction_result.get(0).expect("get should work");

        // Test RangeView methods
        let values = range_view.values();
        let masks = range_view.masks();
        let len = range_view.len();
        let is_range_empty = range_view.is_empty();

        println!(
            "range 0: len={len}, values.len={}, masks.len={}, is_empty={is_range_empty}",
            values.len(),
            masks.len()
        );

        // Test is_valid() for various indices
        if !values.is_empty() {
            let valid0 = range_view.is_valid(0);
            println!("is_valid(0): {valid0}");

            // Test iter_with_validity
            let with_validity: Vec<_> = range_view.iter_with_validity().take(3).collect();
            println!("iter_with_validity (first 3): {with_validity:?}");

            // Test valid_values
            let valid: Vec<_> = range_view.valid_values().take(3).collect();
            println!("valid_values (first 3): {valid:?}");
        }

        // Test to_owned()
        let owned = range_view.to_owned();
        println!(
            "owned: len={}, values.len={}",
            owned.len(),
            owned.values.len()
        );
        assert!(!owned.is_empty() || len == 0);

        // Test RangeResult is_valid
        if !owned.values.is_empty() {
            let owned_valid = owned.is_valid(0);
            println!("RangeResult is_valid(0): {owned_valid}");
        }

        // Also verify range_view2 works the same
        assert_eq!(range_view2.len(), len);
    }

    // Test out-of-bounds range access
    let out_of_bounds = extraction_result.range(999);
    assert!(out_of_bounds.is_none(), "out of bounds should return None");

    // Test to_owned_ranges()
    let owned_ranges = extraction_result.to_owned_ranges();
    println!("to_owned_ranges: {} ranges", owned_ranges.len());

    // Test iter()
    let iter_count = extraction_result.iter().count();
    println!("iter count: {iter_count}");

    // Test Debug implementation
    println!("ExtractionResult Debug: {extraction_result:?}");
}

/// Test `GribJump` clone
#[test]
fn test_gribjump_clone() {
    let gj1 = GribJump::new().expect("failed to create GribJump handle");
    let gj2 = gj1.clone();

    // Both handles should work independently
    gj1.print_stats().expect("gj1 print_stats failed");
    gj2.print_stats().expect("gj2 print_stats failed");
}

//----------------------------------------------------------------------------------------------------------------------
// test_api.cc: Test 1 - Multi-request extraction with different ranges
// Demonstrates FDB + GribJump interop: write GRIB data to FDB, extract with GribJump
//----------------------------------------------------------------------------------------------------------------------

#[test]
fn test_gribjump_api_extract_multi_request() {
    // --- FDB: set up temporary database and archive GRIB data ---
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());
    unsafe {
        env::set_var("FDB5_CONFIG", &config);
    }

    let fdb = Fdb::open(Some(config.as_str()), None).expect("failed to create FDB");
    let grib_data = fs::read(fixtures_dir().join("extract_ranges.grib"))
        .expect("failed to read extract_ranges.grib");
    fdb.archive_raw(&grib_data)
        .expect("failed to archive GRIB data to FDB");
    fdb.flush().expect("FDB flush failed");

    // --- GribJump: extract data that was just written to FDB ---
    let gj = GribJump::new().expect("failed to create GribJump handle");

    // 3 requests with different steps and different ranges (mirrors C++ test_api.cc Test 1)
    let requests = vec![
        // step=2: ranges (0,4) and (20,29) -> 5 + 10 = 15 values
        ExtractionRequest::new(
            "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc",
            vec![
                Range::new(0, 5).expect("valid range"),
                Range::new(20, 30).expect("valid range"),
            ],
            GRID_HASH,
        ),
        // step=1: range (0,99) -> 100 values (full message)
        ExtractionRequest::new(
            "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1,stream=oper,time=1200,type=fc",
            vec![Range::new(0, 100).expect("valid range")],
            GRID_HASH,
        ),
        // step=3: 4 single-element ranges -> 4 values
        ExtractionRequest::new(
            "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=3,stream=oper,time=1200,type=fc",
            vec![
                Range::new(0, 1).expect("valid range"),
                Range::new(1, 2).expect("valid range"),
                Range::new(3, 4).expect("valid range"),
                Range::new(4, 5).expect("valid range"),
            ],
            GRID_HASH,
        ),
    ];

    let results: Vec<_> = gj.extract(&requests).expect("extract failed").collect();
    assert_eq!(results.len(), 3, "expected 3 results (one per request)");

    // Result 0 (step=2): 2 ranges, 5 + 10 = 15 values
    let r0 = results[0].as_ref().expect("result 0 failed");
    assert_eq!(r0.num_ranges(), 2);
    assert_eq!(r0.range(0).expect("range 0 missing").len(), 5);
    assert_eq!(r0.range(1).expect("range 1 missing").len(), 10);
    assert_eq!(r0.total_values(), 15);

    // Result 1 (step=1): 1 range, 100 values
    let r1 = results[1].as_ref().expect("result 1 failed");
    assert_eq!(r1.num_ranges(), 1);
    assert_eq!(r1.range(0).expect("range 0 missing").len(), 100);
    assert_eq!(r1.total_values(), 100);

    // Result 2 (step=3): 4 ranges, 1 value each = 4 values
    let r2 = results[2].as_ref().expect("result 2 failed");
    assert_eq!(r2.num_ranges(), 4);
    for i in 0..4 {
        assert_eq!(r2.range(i).expect("range missing").len(), 1);
    }
    assert_eq!(r2.total_values(), 4);

    // Total across all results: 15 + 100 + 4 = 119
    let total: usize = results
        .iter()
        .map(|r| r.as_ref().expect("result failed").total_values())
        .sum();
    assert_eq!(total, 119);

    println!("test_gribjump_api_extract_multi_request completed: {total} total values");
}

//----------------------------------------------------------------------------------------------------------------------
// test_api.cc: Test 2 - Expanded MARS request (step=2/1/3)
// Demonstrates FDB + GribJump interop: write GRIB data to FDB, extract with GribJump
//----------------------------------------------------------------------------------------------------------------------

#[test]
fn test_gribjump_api_extract_mars_expanded() {
    // --- FDB: set up temporary database and archive GRIB data ---
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());
    unsafe {
        env::set_var("FDB5_CONFIG", &config);
    }

    let fdb = Fdb::open(Some(config.as_str()), None).expect("failed to create FDB");
    let grib_data = fs::read(fixtures_dir().join("extract_ranges.grib"))
        .expect("failed to read extract_ranges.grib");
    fdb.archive_raw(&grib_data)
        .expect("failed to archive GRIB data to FDB");
    fdb.flush().expect("FDB flush failed");

    // --- GribJump: extract data that was just written to FDB ---
    let gj = GribJump::new().expect("failed to create GribJump handle");

    // Single MARS request with step=2/1/3, same ranges for all fields (mirrors C++ test_api.cc Test 2)
    let mars_request = "retrieve,class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2/1/3,stream=oper,time=1200,type=fc";
    let ranges = vec![
        Range::new(0, 5).expect("valid range"),
        Range::new(20, 30).expect("valid range"),
    ];

    let results: Vec<_> = gj
        .extract_mars(mars_request, &ranges, GRID_HASH)
        .expect("extract_mars failed")
        .collect();
    assert_eq!(results.len(), 3, "expected 3 results (one per step value)");

    // Each result should have 2 ranges with 5 + 10 = 15 values
    for (i, result) in results.iter().enumerate() {
        let r = result
            .as_ref()
            .unwrap_or_else(|e| panic!("result {i} failed: {e}"));
        assert_eq!(r.num_ranges(), 2, "result {i}: expected 2 ranges");
        assert_eq!(
            r.range(0).expect("range 0 missing").len(),
            5,
            "result {i}: range 0 should have 5 values"
        );
        assert_eq!(
            r.range(1).expect("range 1 missing").len(),
            10,
            "result {i}: range 1 should have 10 values"
        );
        assert_eq!(r.total_values(), 15, "result {i}: expected 15 total values");
    }

    // Total: 3 * 15 = 45
    let total: usize = results
        .iter()
        .map(|r| r.as_ref().expect("result failed").total_values())
        .sum();
    assert_eq!(total, 45);

    println!("test_gribjump_api_extract_mars_expanded completed: {total} total values");
}

//----------------------------------------------------------------------------------------------------------------------
// test_api.cc: Test 3 - Extract by path and offsets via FDB list
// Demonstrates FDB + GribJump interop: write GRIB data to FDB, list with FDB, extract with GribJump
//----------------------------------------------------------------------------------------------------------------------

#[test]
fn test_gribjump_api_extract_from_file_via_fdb_list() {
    // --- FDB: set up temporary database and archive GRIB data ---
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = create_test_config(tmpdir.path());
    unsafe {
        env::set_var("FDB5_CONFIG", &config);
    }

    let fdb = Fdb::open(Some(config.as_str()), None).expect("failed to create FDB");
    let grib_data = fs::read(fixtures_dir().join("extract_ranges.grib"))
        .expect("failed to read extract_ranges.grib");
    fdb.archive_raw(&grib_data)
        .expect("failed to archive GRIB data to FDB");
    fdb.flush().expect("FDB flush failed");

    // --- FDB: list field locations to discover file paths and offsets ---
    let list_request = Request::new()
        .with("class", "rd")
        .with("date", "20230508")
        .with("domain", "g")
        .with("expver", "xxxx")
        .with("levtype", "sfc")
        .with("param", "151130")
        .with_values("step", &["2", "1", "3"])
        .with("stream", "oper")
        .with("time", "1200")
        .with("type", "fc");

    let list_iter = fdb
        .list(
            &list_request,
            fdb::ListOptions {
                depth: 3,
                deduplicate: false,
            },
        )
        .expect("fdb list failed");

    let mut paths = BTreeSet::new();
    let mut offsets = Vec::new();

    for item in list_iter {
        let item = item.expect("list item failed");
        // URI format: file:/path/to/file.data?length=N#offset
        // Strip scheme, query string, and fragment to get the file path
        let raw = item.uri.strip_prefix("file:").unwrap_or(&item.uri);
        let path = raw
            .split('?')
            .next()
            .expect("URI should have a path")
            .to_string();
        paths.insert(path);
        offsets.push(item.offset);
    }

    assert_eq!(paths.len(), 1, "expected all messages in 1 file");
    assert_eq!(offsets.len(), 3, "expected 3 messages");

    let file_path = paths.into_iter().next().expect("expected a file path");

    // --- GribJump: extract by file path + offsets ---
    let gj = GribJump::new().expect("failed to create GribJump handle");

    let ranges = vec![
        Range::new(0, 5).expect("valid range"),
        Range::new(20, 30).expect("valid range"),
    ];
    let mut extraction = FileExtraction::new(&file_path);
    for &offset in &offsets {
        extraction = extraction.with_message(offset, ranges.clone());
    }

    let results: Vec<_> = gj
        .extract_from_file(&extraction)
        .expect("extract_from_file failed")
        .collect();
    assert_eq!(results.len(), 3, "expected 3 results");

    // Each result should have 2 ranges with 5 + 10 = 15 values
    for (i, result) in results.iter().enumerate() {
        let r = result
            .as_ref()
            .unwrap_or_else(|e| panic!("result {i} failed: {e}"));
        assert_eq!(r.num_ranges(), 2, "result {i}: expected 2 ranges");
        assert_eq!(
            r.range(0).expect("range 0 missing").len(),
            5,
            "result {i}: range 0 should have 5 values"
        );
        assert_eq!(
            r.range(1).expect("range 1 missing").len(),
            10,
            "result {i}: range 1 should have 10 values"
        );
        assert_eq!(r.total_values(), 15, "result {i}: expected 15 total values");
    }

    // Total: 3 * 15 = 45
    let total: usize = results
        .iter()
        .map(|r| r.as_ref().expect("result failed").total_values())
        .sum();
    assert_eq!(total, 45);

    println!("test_gribjump_api_extract_from_file_via_fdb_list completed: {total} total values");
}

//----------------------------------------------------------------------------------------------------------------------
// Additional API tests
//----------------------------------------------------------------------------------------------------------------------

/// Test `ExtractionIterator` methods directly
#[test]
fn test_gribjump_iterator_methods() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = setup_test_fdb_extract_ranges(tmpdir.path());
    unsafe {
        env::set_var("FDB5_CONFIG", &config);
    }

    let gj = GribJump::new().expect("failed to create GribJump handle");

    let ranges = vec![Range::new(0, 6).expect("valid range")];
    let request_str = "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc";
    let request = ExtractionRequest::new(request_str, ranges, GRID_HASH);

    let mut iter = gj.extract(&[request]).expect("extract failed");

    // Test has_next() directly
    let has = iter.has_next();
    println!("has_next(): {has}");

    if has {
        // Test next_result() directly
        let result = iter.next_result();
        println!("next_result(): {:?}", result.is_ok());
        assert!(result.is_ok(), "next_result should succeed");
    }

    // After exhausting, has_next should be false
    while iter.has_next() {
        let _ = iter.next_result();
    }
    assert!(!iter.has_next(), "should be exhausted");

    // next_result on exhausted iterator should return error
    let err = iter.next_result();
    assert!(err.is_err(), "next_result on exhausted should error");
}

/// Test `IntoIterator` for `ExtractionResult`
#[test]
fn test_gribjump_extraction_into_iter() {
    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = setup_test_fdb_extract_ranges(tmpdir.path());
    unsafe {
        env::set_var("FDB5_CONFIG", &config);
    }

    let gj = GribJump::new().expect("failed to create GribJump handle");

    let ranges = vec![Range::new(0, 6).expect("valid range")];
    let request_str = "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc";
    let request = ExtractionRequest::new(request_str, ranges, GRID_HASH);

    let result = gj.extract(&[request]).expect("extract failed");
    let results: Vec<_> = result.collect();

    if let Some(Ok(extraction_result)) = results.into_iter().next() {
        // Test to_owned_ranges - explicitly converts to owned data
        let count = extraction_result.to_owned_ranges().len();
        println!("to_owned_ranges produced {count} owned ranges");
    }
}
