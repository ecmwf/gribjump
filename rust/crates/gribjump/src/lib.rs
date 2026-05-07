//! Safe Rust wrapper for the ECMWF gribjump library.
//!
//! This crate provides a safe, idiomatic Rust interface to gribjump,
//! a library for efficient extraction of subsets from GRIB data.
//!
//! # Example
//!
//! ```no_run
//! use gribjump::{GribJump, ExtractionRequest, Range};
//!
//! let mut gj = GribJump::new().expect("failed to create GribJump handle");
//!
//! // Create a request for specific data
//! let ranges = vec![
//!     Range::new(0, 100).expect("valid range"),
//!     Range::new(200, 300).expect("valid range"),
//! ];
//! let request = ExtractionRequest::new(
//!     "class=od,expver=0001,stream=oper,date=20230508,time=1200,step=1,param=151130",
//!     ranges,
//!     "33c7d6025995e1b4913811e77d38ec50",  // grid hash
//! );
//!
//! // Extract data (zero-copy access via RangeView)
//! for result in gj.extract(&[request]).expect("extraction failed") {
//!     let result = result.expect("failed to get result");
//!     for range in result.iter() {
//!         println!("Got {} values", range.len());
//!     }
//! }
//! ```

mod error;
mod handle;
mod iterator;
mod request;
mod result;

pub use error::{Error, Result};
pub use handle::GribJump;
pub use iterator::ExtractionIterator;
pub use request::{ExtractionRequest, FileExtraction, PathExtractionRequest, Range};
pub use result::{ExtractionResult, RangeResult, RangeView};

/// Get the gribjump library version.
#[must_use]
pub fn version() -> String {
    gribjump_sys::gribjump_version()
}

/// Get the gribjump git SHA1.
#[must_use]
pub fn git_sha1() -> String {
    gribjump_sys::gribjump_git_sha1()
}
