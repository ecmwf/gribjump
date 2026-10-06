//! Grid-hash bypass test, isolated in its own binary.
//!
//! `GRIBJUMP_IGNORE_GRID` is latched into a `static` on first use inside
//! gribjump's `ConfigOptions`, so it cannot be toggled mid-process. The
//! hash-validation tests in `gribjump_integration.rs` run with it unset;
//! this binary sets it before the first extraction.

use std::env;
use std::fs;
use std::path::PathBuf;

use fdb::Fdb;
use gribjump::{ExtractionRequest, GribJump, Range};

/// Get the path to test fixtures directory.
fn fixtures_dir() -> PathBuf {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    PathBuf::from(manifest_dir).join("tests/fixtures")
}

/// Create a temporary FDB configuration for testing.
/// Copies the schema file to the temp directory since FDB resolves paths relatively.
fn create_test_config(tmpdir: &std::path::Path) -> String {
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
fn setup_test_fdb_extract_ranges(tmpdir: &std::path::Path) -> String {
    let config = create_test_config(tmpdir);

    // SAFETY: Single-threaded test environment, setting FDB config before use
    unsafe {
        env::set_var("FDB5_CONFIG", &config);
    }

    let fdb = Fdb::open(
        Some(&config.parse::<eckit::Config>().expect("parse config")),
        None,
    )
    .expect("failed to create FDB");

    let grib_path = fixtures_dir().join("extract_ranges.grib");
    let grib_data = fs::read(&grib_path).expect("failed to read extract_ranges.grib");

    fdb.archive_raw(&grib_data).expect("failed to archive data");
    fdb.flush().expect("flush failed");

    config
}

#[test]
fn test_gribjump_api_extract_empty_hash_ignored() {
    // Must be set before the first extraction in this process (see module docs).
    unsafe {
        env::set_var("GRIBJUMP_IGNORE_GRID", "1");
    }

    let tmpdir = tempfile::tempdir().expect("failed to create temp dir");
    let config = setup_test_fdb_extract_ranges(tmpdir.path());
    // GribJump discovers FDB via this env var (its constructor takes no config param)
    unsafe {
        env::set_var("FDB5_CONFIG", &config);
    }

    let gj = GribJump::new().expect("failed to create GribJump handle");

    let ranges = vec![
        Range::new(0, 6).expect("valid range"),
        Range::new(10, 16).expect("valid range"),
    ];
    let request_str = "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc";

    let request_empty_hash = ExtractionRequest::new(request_str, ranges, "");
    let result = gj.extract(&[request_empty_hash]);
    assert!(
        result.is_ok(),
        "expected success with empty hash when ignoreGridHash is set"
    );
    let results: Vec<_> = result.expect("extraction should succeed").collect();
    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok());
    println!("Extract with empty hash (ignoreGridHash=true): success");
}
