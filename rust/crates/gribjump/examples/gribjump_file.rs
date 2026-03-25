//! Extract data directly from a GRIB file.
//!
//! Run with: `cargo run --example gribjump_file -p gribjump -- <file.grib>`
//!
//! This example:
//! 1. Scans a GRIB file to index its messages
//! 2. Extracts a subset of values from each message
//! 3. Prints statistics about the extracted data

use std::env;

use gribjump::{FileExtraction, GribJump, Range};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <file.grib>", args[0]);
        std::process::exit(1);
    }

    let grib_path = &args[1];
    let gj = GribJump::new()?;

    // First, scan the file to index it
    println!("Scanning {grib_path}...");
    let count = gj.scan_paths(&[grib_path])?;
    println!("Found {count} message(s)\n");

    if count == 0 {
        println!("No messages found in file.");
        return Ok(());
    }

    // Extract first 50 values from each of the first few messages
    let ranges = vec![Range::new(0, 49)?];

    // Build a FileExtraction request for the first 3 messages
    // Note: offset is the byte offset in the file, not message number
    // For this example we use offsets 0, 1, 2 but real offsets depend on file structure
    let mut extraction = FileExtraction::new(grib_path);
    for offset in 0..count.min(3) {
        extraction = extraction.with_message(offset as u64, ranges.clone());
    }

    println!("Extracting from {} message(s)...", count.min(3));
    for (msg_idx, result) in gj.extract_from_file(&extraction)?.enumerate() {
        let result = result?;

        let total_values: usize = result.iter().map(|r| r.len()).sum();
        let valid_values: usize = result
            .iter()
            .map(|r| (0..r.len()).filter(|&i| r.is_valid(i)).count())
            .sum();

        println!("\nMessage {msg_idx}:");
        println!("  Total values: {total_values}");
        println!("  Valid values: {valid_values}");

        // Show value statistics
        if let Some(range) = result.range(0) {
            let valid_vals: Vec<f64> = range.valid_values().collect();

            if !valid_vals.is_empty() {
                let min = valid_vals.iter().copied().fold(f64::INFINITY, f64::min);
                let max = valid_vals.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                let sum: f64 = valid_vals.iter().sum();
                #[allow(clippy::cast_precision_loss)]
                let mean = sum / valid_vals.len() as f64;

                println!("  Min: {min:.6}");
                println!("  Max: {max:.6}");
                println!("  Mean: {mean:.6}");
            }
        }
    }

    Ok(())
}
