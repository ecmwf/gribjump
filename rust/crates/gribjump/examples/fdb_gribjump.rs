//! FDB + `GribJump` interoperability prototype.
//!
//! Demonstrates the full workflow: write GRIB data to FDB using `rust-fdb`,
//! then extract values using `rust-gribjump`. Mirrors the C++ ``test_api.cc``
//! test patterns.
//!
//! Run with: `cargo run --example fdb_gribjump -p gribjump`

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::PathBuf;

use fdb::{Fdb, ListOptions, Request};
use gribjump::{ExtractionRequest, ExtractionResult, FileExtraction, GribJump, Range};

const GRID_HASH: &str = "33c7d6025995e1b4913811e77d38ec50";

fn fixtures_dir() -> PathBuf {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    PathBuf::from(manifest_dir).join("tests/fixtures")
}

/// Print extraction results in a readable format.
fn print_results(results: &[Result<ExtractionResult, gribjump::Error>]) {
    let mut total_values = 0;
    for (i, result) in results.iter().enumerate() {
        let result = result.as_ref().expect("extraction failed");
        println!("  Result {i}: {} range(s)", result.num_ranges());

        for (j, range) in result.iter().enumerate() {
            let valid_count = (0..range.len()).filter(|&k| range.is_valid(k)).count();
            println!(
                "    Range {j}: {} values ({} valid)",
                range.len(),
                valid_count
            );

            for (k, &value) in range.values().iter().take(3).enumerate() {
                let valid = if range.is_valid(k) { "" } else { " (invalid)" };
                println!("      [{k}] = {value:.6}{valid}");
            }
            if range.len() > 3 {
                println!("      ... ({} more)", range.len() - 3);
            }
            total_values += range.len();
        }
    }
    println!(
        "  Total: {total_values} values across {} result(s)",
        results.len()
    );
}

/// Set up a temporary FDB and archive `extract_ranges.grib` into it.
fn setup_fdb(tmpdir: &std::path::Path) -> Result<Fdb, Box<dyn std::error::Error>> {
    fs::copy(fixtures_dir().join("schema"), tmpdir.join("schema"))?;

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

    // SAFETY: Single-threaded example, setting env before any FDB/GribJump use
    unsafe {
        env::set_var("FDB5_CONFIG", &config);
    }

    let fdb = Fdb::open(Some(config.as_str()), None)?;
    let grib_data = fs::read(fixtures_dir().join("extract_ranges.grib"))?;
    println!(
        "Archiving {} bytes from extract_ranges.grib...",
        grib_data.len()
    );
    fdb.archive_raw(&grib_data)?;
    fdb.flush()?;
    println!("Data archived and flushed to FDB.\n");
    Ok(fdb)
}

/// Demo 1: Multi-request extraction with different ranges per request.
/// Mirrors C++ `test_api.cc` Test 1.
fn demo_multi_request(gj: &GribJump) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Demo 1: Multi-request extraction ---");
    println!("3 requests (step=2,1,3) with different ranges per request.\n");

    let requests = vec![
        ExtractionRequest::new(
            "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2,stream=oper,time=1200,type=fc",
            vec![Range::new(0, 5)?, Range::new(20, 30)?],
            GRID_HASH,
        ),
        ExtractionRequest::new(
            "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=1,stream=oper,time=1200,type=fc",
            vec![Range::new(0, 100)?],
            GRID_HASH,
        ),
        ExtractionRequest::new(
            "class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=3,stream=oper,time=1200,type=fc",
            vec![
                Range::new(0, 1)?,
                Range::new(1, 2)?,
                Range::new(3, 4)?,
                Range::new(4, 5)?,
            ],
            GRID_HASH,
        ),
    ];

    let results: Vec<_> = gj.extract(&requests)?.collect();
    print_results(&results);
    println!();
    Ok(())
}

/// Demo 2: Expanded MARS request (step=2/1/3, same ranges for all fields).
/// Mirrors C++ `test_api.cc` Test 2.
fn demo_expanded_mars(gj: &GribJump) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Demo 2: Expanded MARS request ---");
    println!("Single request with step=2/1/3, same ranges for all fields.\n");

    let mars_request = "retrieve,class=rd,date=20230508,domain=g,expver=xxxx,levtype=sfc,param=151130,step=2/1/3,stream=oper,time=1200,type=fc";
    let ranges = vec![Range::new(0, 5)?, Range::new(20, 30)?];

    let results: Vec<_> = gj.extract_mars(mars_request, &ranges, GRID_HASH)?.collect();
    print_results(&results);
    println!();
    Ok(())
}

/// Demo 3: Use `fdb.list()` to discover field locations, then extract by path/offset.
/// Mirrors C++ `test_api.cc` Test 3.
fn demo_path_and_offset(fdb: &Fdb, gj: &GribJump) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Demo 3: Path + offset extraction via FDB list ---");
    println!("Use fdb.list() to discover field locations, then extract by path/offset.\n");

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

    let list_iter = fdb.list(
        &list_request,
        ListOptions {
            depth: 3,
            deduplicate: false,
        },
    )?;

    let mut paths = BTreeSet::new();
    let mut offsets = Vec::new();

    for item in list_iter {
        let item = item?;
        // URI format: file:/path/to/file.data?length=N#offset
        // Strip scheme and query string to get the file path
        let raw = item.uri.strip_prefix("file:").unwrap_or(&item.uri);
        let path = raw
            .split('?')
            .next()
            .expect("URI should have a path")
            .to_string();
        println!("  Listed: offset={}, length={}", item.offset, item.length);
        paths.insert(path);
        offsets.push(item.offset);
    }

    let file_path = paths
        .into_iter()
        .next()
        .expect("expected at least one file path");
    println!("  File: {file_path}");
    println!("  Offsets: {offsets:?}\n");

    let ranges_per_message = vec![Range::new(0, 5)?, Range::new(20, 30)?];
    let mut extraction = FileExtraction::new(&file_path);
    for &offset in &offsets {
        extraction = extraction.with_message(offset, ranges_per_message.clone());
    }

    let results: Vec<_> = gj.extract_from_file(&extraction)?.collect();
    print_results(&results);
    println!();
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== FDB + GribJump Interoperability Prototype ===\n");

    let tmpdir = tempfile::tempdir()?;
    println!("Temporary FDB directory: {}", tmpdir.path().display());

    let fdb = setup_fdb(tmpdir.path())?;
    let gj = GribJump::new()?;
    println!("GribJump version: {}\n", gribjump::version());

    demo_multi_request(&gj)?;
    demo_expanded_mars(&gj)?;
    demo_path_and_offset(&fdb, &gj)?;

    println!("=== All demos completed successfully ===");
    Ok(())
}
