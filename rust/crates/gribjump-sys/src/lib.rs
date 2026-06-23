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
#[cxx::bridge(namespace = "gribjump_bridge")]
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
        include!("GribJumpBridge.h");

        // =====================================================================
        // Library — runtime initialisation and metadata
        // =====================================================================

        type Library;

        /// Initialise the gribjump library (sets up `eckit::Main`). Idempotent.
        #[Self = "Library"]
        fn initialise();

        /// Get the gribjump library version string.
        #[Self = "Library"]
        fn version() -> String;

        /// Get the gribjump git SHA1 hash.
        #[Self = "Library"]
        fn git_sha1() -> String;

        // =====================================================================
        // GribJumpHandle — main handle, factories + extraction + query
        // =====================================================================

        type GribJumpHandle;

        /// Extract data using standard ExtractionRequests.
        fn extract(
            self: Pin<&mut GribJumpHandle>,
            requests: &Vec<ExtractionRequestData>,
        ) -> Result<UniquePtr<ExtractionIteratorHandle>>;

        /// Extract data using PathExtractionRequests (direct file access).
        fn extract_from_paths(
            self: Pin<&mut GribJumpHandle>,
            requests: &Vec<PathExtractionRequestData>,
        ) -> Result<UniquePtr<ExtractionIteratorHandle>>;

        /// Extract from a MARS request (expands to multiple ExtractionRequests internally).
        fn extract_mars(
            self: Pin<&mut GribJumpHandle>,
            request: &str,
            ranges: &Vec<Range>,
            grid_hash: &str,
        ) -> Result<UniquePtr<ExtractionIteratorHandle>>;

        /// Extract from a specific file with message offsets.
        fn extract_from_file(
            self: Pin<&mut GribJumpHandle>,
            data: &FileExtractionData,
        ) -> Result<UniquePtr<ExtractionIteratorHandle>>;

        /// Query available axes for a given request.
        fn axes(
            self: Pin<&mut GribJumpHandle>,
            request: &str,
            level: i32,
        ) -> Result<Vec<AxisEntry>>;

        /// Scan GRIB files at the given paths.
        fn scan_paths(self: Pin<&mut GribJumpHandle>, paths: &Vec<String>) -> Result<usize>;

        /// Scan using MARS requests.
        fn scan_requests(
            self: Pin<&mut GribJumpHandle>,
            requests: &Vec<String>,
            by_files: bool,
        ) -> Result<usize>;

        /// Print statistics to stdout.
        fn stats(self: Pin<&mut GribJumpHandle>) -> Result<()>;

        /// Create a new GribJump handle.
        #[Self = "GribJumpHandle"]
        fn create() -> Result<UniquePtr<GribJumpHandle>>;

        // =====================================================================
        // ExtractionIteratorHandle
        // =====================================================================

        type ExtractionIteratorHandle;

        fn hasNext(self: &ExtractionIteratorHandle) -> bool;

        fn next(
            self: Pin<&mut ExtractionIteratorHandle>,
        ) -> Result<UniquePtr<ExtractionResultHandle>>;

        // =====================================================================
        // ExtractionResultHandle — zero-copy result wrapper
        // =====================================================================

        type ExtractionResultHandle;

        fn num_ranges(self: &ExtractionResultHandle) -> usize;

        /// # Safety
        /// The returned pointer is valid for the lifetime of the handle.
        /// Caller must ensure `range_idx < num_ranges()`.
        unsafe fn values_ptr(self: &ExtractionResultHandle, range_idx: usize) -> *const f64;

        fn values_len(self: &ExtractionResultHandle, range_idx: usize) -> usize;

        /// # Safety
        /// The returned pointer is valid for the lifetime of the handle.
        /// Caller must ensure `range_idx < num_ranges()`.
        unsafe fn masks_ptr(self: &ExtractionResultHandle, range_idx: usize) -> *const u64;

        fn masks_len(self: &ExtractionResultHandle, range_idx: usize) -> usize;
    }
}

pub use ffi::*;

// Re-export cxx types needed by downstream crates
pub use cxx::{Exception, UniquePtr};
