//! C++ bindings to ECMWF gribjump library using cxx.
//!
//! This crate provides raw C++ bindings to gribjump. For a safe, idiomatic
//! Rust interface, use the `gribjump` crate instead.

#![allow(clippy::needless_lifetimes)]
#![allow(clippy::must_use_candidate)]
// Safety docs are provided in comments but not recognized by clippy inside cxx bridge
#![allow(clippy::missing_safety_doc)]

use bindman::track_cpp_api;

#[track_cpp_api("gribjump/GribJump.h", class = "GribJump")]
#[cxx::bridge(namespace = "gribjump::ffi")]
mod ffi {
    // =========================================================================
    // Shared structs (POD-like types that can cross the FFI boundary)
    // =========================================================================

    /// A range of indices for extraction (start, end inclusive).
    #[derive(Debug, Clone, Default)]
    pub struct Range {
        pub start: usize,
        pub end: usize,
    }

    /// Data for creating an ExtractionRequest on the C++ side.
    #[derive(Debug, Clone, Default)]
    pub struct ExtractionRequestData {
        /// MARS request string (e.g., "class=od,expver=0001,...")
        pub request_str: String,
        /// Ranges to extract
        pub ranges: Vec<Range>,
        /// Optional grid hash for validation
        pub grid_hash: String,
    }

    /// Data for creating a PathExtractionRequest on the C++ side.
    #[derive(Debug, Clone, Default)]
    pub struct PathExtractionRequestData {
        /// Path to the GRIB file
        pub filename: String,
        /// URI scheme (e.g., "file", "fdb")
        pub scheme: String,
        /// Offset within the file
        pub offset: usize,
        /// Host for remote access
        pub host: String,
        /// Port for remote access
        pub port: i32,
        /// Ranges to extract
        pub ranges: Vec<Range>,
        /// Optional grid hash for validation
        pub grid_hash: String,
    }

    /// Flattened extraction result data.
    ///
    /// The C++ ExtractionResult contains nested vectors:
    /// - `std::vector<std::vector<double>>` for values
    /// - `std::vector<std::vector<std::bitset<64>>>` for masks
    ///
    /// We flatten these with offset arrays for efficient FFI transfer.
    #[derive(Debug, Clone, Default)]
    pub struct ExtractionResultData {
        /// All values flattened into a single vector
        pub values: Vec<f64>,
        /// Start index in `values` for each range
        pub values_offsets: Vec<usize>,

        /// All masks as u64 (converted from bitset<64>), flattened
        pub masks: Vec<u64>,
        /// Start index in `masks` for each range
        pub masks_offsets: Vec<usize>,
    }

    /// An axis entry (key -> values mapping).
    #[derive(Debug, Clone, Default)]
    pub struct AxisEntry {
        pub key: String,
        pub values: Vec<String>,
    }

    /// Data for file-based extraction with offsets.
    ///
    /// Used by `extract_from_file` to extract from specific GRIB messages
    /// at known offsets within a file.
    #[derive(Debug, Clone, Default)]
    pub struct FileExtractionData {
        /// Path to the GRIB file
        pub path: String,
        /// Offsets of GRIB messages within the file
        pub offsets: Vec<u64>,
        /// Ranges for each message, flattened with offsets
        pub ranges: Vec<Range>,
        /// Start index in `ranges` for each message
        pub ranges_offsets: Vec<usize>,
    }

    // =========================================================================
    // C++ types and functions
    // =========================================================================

    unsafe extern "C++" {
        include!("gribjump_bridge.h");

        // ---------------------------------------------------------------------
        // Opaque handle types
        // ---------------------------------------------------------------------

        /// Wrapper around gribjump::GribJump
        type GribJumpHandle;

        /// Wrapper around gribjump::ExtractionIterator
        type ExtractionIteratorHandle;

        /// Zero-copy wrapper around gribjump::ExtractionResult.
        /// Keeps the C++ result alive and provides direct pointer access.
        type ExtractionResultHandle;

        // ---------------------------------------------------------------------
        // Library metadata
        // ---------------------------------------------------------------------

        /// Get the gribjump library version string.
        fn gribjump_version() -> String;

        /// Get the gribjump git SHA1 hash.
        fn gribjump_git_sha1() -> String;

        // ---------------------------------------------------------------------
        // Handle lifecycle
        // ---------------------------------------------------------------------

        /// Create a new GribJump handle.
        fn new_gribjump() -> Result<UniquePtr<GribJumpHandle>>;

        // ---------------------------------------------------------------------
        // Extraction
        // ---------------------------------------------------------------------

        /// Extract data using standard ExtractionRequests.
        fn extract(
            handle: Pin<&mut GribJumpHandle>,
            requests: &Vec<ExtractionRequestData>,
        ) -> Result<UniquePtr<ExtractionIteratorHandle>>;

        /// Extract data using PathExtractionRequests (direct file access).
        fn extract_from_paths(
            handle: Pin<&mut GribJumpHandle>,
            requests: &Vec<PathExtractionRequestData>,
        ) -> Result<UniquePtr<ExtractionIteratorHandle>>;

        /// Extract from a MARS request (expands to multiple ExtractionRequests internally).
        fn extract_mars(
            handle: Pin<&mut GribJumpHandle>,
            request: &str,
            ranges: &Vec<Range>,
            grid_hash: &str,
        ) -> Result<UniquePtr<ExtractionIteratorHandle>>;

        /// Extract from a specific file with message offsets.
        fn extract_from_file(
            handle: Pin<&mut GribJumpHandle>,
            data: &FileExtractionData,
        ) -> Result<UniquePtr<ExtractionIteratorHandle>>;

        // ---------------------------------------------------------------------
        // Iterator (methods on ExtractionIteratorHandle)
        // ---------------------------------------------------------------------

        /// Check if the iterator has more results.
        fn hasNext(self: &ExtractionIteratorHandle) -> bool;

        /// Get the next result from the iterator (zero-copy handle).
        fn next(
            self: Pin<&mut ExtractionIteratorHandle>,
        ) -> Result<UniquePtr<ExtractionResultHandle>>;

        // ---------------------------------------------------------------------
        // Result handle (methods on ExtractionResultHandle)
        // ---------------------------------------------------------------------

        /// Get the number of ranges in this result.
        fn num_ranges(self: &ExtractionResultHandle) -> usize;

        /// Get raw pointer to values for a specific range (zero-copy).
        ///
        /// # Safety
        ///
        /// The returned pointer is valid for the lifetime of the handle.
        /// Caller must ensure `range_idx < num_ranges()`.
        unsafe fn values_ptr(self: &ExtractionResultHandle, range_idx: usize) -> *const f64;

        /// Get the number of values in a specific range.
        fn values_len(self: &ExtractionResultHandle, range_idx: usize) -> usize;

        /// Get raw pointer to masks for a specific range.
        ///
        /// Each mask element is a u64 where each bit represents validity.
        ///
        /// # Safety
        ///
        /// The returned pointer is valid for the lifetime of the handle.
        /// Caller must ensure `range_idx < num_ranges()`.
        unsafe fn masks_ptr(self: &ExtractionResultHandle, range_idx: usize) -> *const u64;

        /// Get the number of mask elements in a specific range.
        fn masks_len(self: &ExtractionResultHandle, range_idx: usize) -> usize;

        // ---------------------------------------------------------------------
        // Axes query
        // ---------------------------------------------------------------------

        /// Query available axes for a given request.
        ///
        /// Returns a vector of (key, values) pairs representing the axes.
        fn axes(
            handle: Pin<&mut GribJumpHandle>,
            request: &str,
            level: i32,
        ) -> Result<Vec<AxisEntry>>;

        // ---------------------------------------------------------------------
        // Scanning
        // ---------------------------------------------------------------------

        /// Scan GRIB files at the given paths.
        fn scan_paths(handle: Pin<&mut GribJumpHandle>, paths: &Vec<String>) -> Result<usize>;

        /// Scan using MARS requests.
        fn scan_requests(
            handle: Pin<&mut GribJumpHandle>,
            requests: &Vec<String>,
            by_files: bool,
        ) -> Result<usize>;

        // ---------------------------------------------------------------------
        // Diagnostics
        // ---------------------------------------------------------------------

        /// Print statistics to stdout.
        fn stats(handle: Pin<&mut GribJumpHandle>) -> Result<()>;

        // ---------------------------------------------------------------------
        // Test functions (for verifying exception handling)
        // ---------------------------------------------------------------------

        /// Test function that throws eckit::Exception
        fn test_throw_eckit_exception() -> Result<()>;

        /// Test function that throws eckit::SeriousBug
        fn test_throw_eckit_serious_bug() -> Result<()>;

        /// Test function that throws eckit::UserError
        fn test_throw_eckit_user_error() -> Result<()>;

        /// Test function that throws std::runtime_error
        fn test_throw_std_exception() -> Result<()>;

        /// Test function that throws an int (non-std::exception type)
        fn test_throw_int() -> Result<()>;
    }
}

pub use ffi::*;

// Re-export cxx types needed by downstream crates
pub use cxx::{Exception, UniquePtr};

#[cfg(test)]
mod tests {
    use super::ffi;

    #[test]
    fn test_eckit_exception_caught_by_trycatch() {
        let result = ffi::test_throw_eckit_exception();
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        // Generic eckit::Exception gets ECKIT: prefix
        assert!(
            err.what().starts_with("ECKIT: "),
            "Expected ECKIT: prefix, got: {}",
            err.what()
        );
        assert!(
            err.what().contains("test eckit exception"),
            "Expected eckit exception message, got: {}",
            err.what()
        );
    }

    #[test]
    fn test_eckit_serious_bug_caught_by_trycatch() {
        let result = ffi::test_throw_eckit_serious_bug();
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        // SeriousBug gets specific prefix
        assert!(
            err.what().starts_with("ECKIT_SERIOUS_BUG: "),
            "Expected ECKIT_SERIOUS_BUG: prefix, got: {}",
            err.what()
        );
        assert!(
            err.what().contains("test serious bug"),
            "Expected serious bug message, got: {}",
            err.what()
        );
    }

    #[test]
    fn test_eckit_user_error_caught_by_trycatch() {
        let result = ffi::test_throw_eckit_user_error();
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        // UserError gets specific prefix
        assert!(
            err.what().starts_with("ECKIT_USER_ERROR: "),
            "Expected ECKIT_USER_ERROR: prefix, got: {}",
            err.what()
        );
        assert!(
            err.what().contains("test user error"),
            "Expected user error message, got: {}",
            err.what()
        );
    }

    #[test]
    fn test_std_exception_caught_by_trycatch() {
        let result = ffi::test_throw_std_exception();
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        // std::exception should NOT have any ECKIT prefix
        assert!(
            !err.what().starts_with("ECKIT"),
            "std::exception should not have ECKIT prefix, got: {}",
            err.what()
        );
        assert!(
            err.what().contains("test std exception"),
            "Expected std exception message, got: {}",
            err.what()
        );
    }

    #[test]
    fn test_non_std_exception_caught_by_trycatch() {
        let result = ffi::test_throw_int();
        assert!(result.is_err());
        let err = result.expect_err("expected error");
        // Non-std exceptions get a generic message
        assert!(
            err.what().contains("unknown exception"),
            "Expected unknown exception message, got: {}",
            err.what()
        );
    }
}
