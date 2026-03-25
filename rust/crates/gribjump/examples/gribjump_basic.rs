//! Basic `GribJump` example - shows version info and handle creation.
//!
//! Run with: `cargo run --example gribjump_basic -p gribjump`

use gribjump::GribJump;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Print version info (works without config)
    println!("GribJump version: {}", GribJump::version());
    println!("GribJump git SHA1: {}", GribJump::git_sha1());

    // Create handle
    let gj = GribJump::new()?;
    println!("GribJump handle created successfully");

    // Print stats (debugging info)
    println!("\nStats:");
    gj.print_stats();

    Ok(())
}
