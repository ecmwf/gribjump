//! Extract data subsets using `GribJump`.
//!
//! Run with: `cargo run --example gribjump_extract -p gribjump -- <request> <grid_hash>`
//!
//! Example:
//!   cargo run --example `gribjump_extract` -p gribjump -- \
//!     "class=rd,expver=xxxx,date=20230508,step=1,..." \
//!     "33c7d6025995e1b4913811e77d38ec50"

use std::env;

use gribjump::{ExtractionRequest, GribJump, Range};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <request> <grid_hash>", args[0]);
        eprintln!();
        eprintln!("Arguments:");
        eprintln!("  request    MARS-style request: key=value,key=value,...");
        eprintln!("  grid_hash  Grid hash for validation (e.g., 33c7d6025995e1b4913811e77d38ec50)");
        eprintln!();
        eprintln!("Example:");
        eprintln!(
            "  {} \"class=rd,expver=xxxx,date=20230508,time=1200,step=1,param=151130\" abc123",
            args[0]
        );
        std::process::exit(1);
    }

    let request_str = &args[1];
    let grid_hash = &args[2];

    let gj = GribJump::new()?;

    // Define ranges to extract (grid indices)
    // These are inclusive ranges of grid point indices
    let ranges = vec![
        Range::new(0, 99)?,    // First 100 grid points
        Range::new(200, 299)?, // Grid points 200-299
    ];

    println!("Request: {request_str}");
    println!("Grid hash: {grid_hash}");
    println!(
        "Ranges: {:?}",
        ranges
            .iter()
            .map(|r| format!("{}-{}", r.start, r.end))
            .collect::<Vec<_>>()
    );
    println!();

    let request = ExtractionRequest::new(request_str, ranges).with_grid_hash(grid_hash);

    println!("Extracting...");
    for result in gj.extract(&[request])? {
        let result = result?;
        println!("Got {} range(s)", result.num_ranges());

        for (i, range) in result.iter().enumerate() {
            println!("\n  Range {}: {} values", i, range.len());

            // Count valid values
            let valid_count = (0..range.len()).filter(|&j| range.is_valid(j)).count();
            println!("  Valid: {}/{}", valid_count, range.len());

            // Show first few values
            println!("  Preview:");
            for (j, &value) in range.values().iter().take(5).enumerate() {
                let valid = if range.is_valid(j) { "" } else { " (invalid)" };
                println!("    [{j}] = {value:.6}{valid}");
            }
            if range.len() > 5 {
                println!("    ... ({} more values)", range.len() - 5);
            }
        }
    }

    Ok(())
}
