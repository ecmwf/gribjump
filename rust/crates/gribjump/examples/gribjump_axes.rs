//! Query available axes using `GribJump`.
//!
//! Run with: `cargo run --example gribjump_axes -p gribjump -- [request]`
//!
//! Examples:
//!   cargo run --example `gribjump_axes` -p gribjump -- class=od
//!   cargo run --example `gribjump_axes` -p gribjump -- class=rd,expver=xxxx

use std::env;

use gribjump::GribJump;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let gj = GribJump::new()?;

    let request = if args.len() > 1 {
        args[1].clone()
    } else {
        println!("Usage: {} [request]", args[0]);
        println!("Using default: class=od");
        "class=od".to_string()
    };

    println!("Querying axes for: {request}\n");

    // Query axes with depth=3 (full traversal)
    let axes = gj.axes(&request, 3)?;

    if axes.is_empty() {
        println!("No axes found for the given request.");
    } else {
        for (name, values) in &axes {
            println!("{name}:");
            for value in values {
                println!("  - {value}");
            }
        }
        println!("\nFound {} axis/axes", axes.len());
    }

    Ok(())
}
