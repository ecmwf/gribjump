//! `GribJump` handle wrapper.

use std::collections::HashMap;
use std::sync::Arc;

use gribjump_sys::UniquePtr;
use parking_lot::Mutex;

use crate::error::Result;
use crate::iterator::ExtractionIterator;
use crate::request::{ExtractionRequest, FileExtraction, PathExtractionRequest, Range};

// Private wrapper to make UniquePtr Send-safe for use with Arc<Mutex<...>>
struct HandleInner(UniquePtr<gribjump_sys::GribJumpHandle>);

// SAFETY: HandleInner is only accessed through Mutex which provides synchronization.
// The underlying C++ GribJump handle is protected by the Mutex.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for HandleInner {}

/// A thread-safe handle to the gribjump library.
///
/// This is the main entry point for extraction operations. The handle can be
/// cloned to share access across threads.
///
/// # Example
///
/// ```no_run
/// use gribjump::{GribJump, ExtractionRequest, Range};
///
/// let gj = GribJump::new().expect("failed to create handle");
///
/// // Clone for use in another thread
/// let gj2 = gj.clone();
///
/// std::thread::spawn(move || {
///     let ranges = vec![Range::new(0, 100).unwrap()];
///     let request = ExtractionRequest::new("class=od", ranges, "grid_hash");
///     let _ = gj2.extract(&[request]);
/// });
/// ```
pub struct GribJump {
    inner: Arc<Mutex<HandleInner>>,
}

impl Clone for GribJump {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl GribJump {
    /// Create a new gribjump handle.
    ///
    /// # Errors
    ///
    /// Returns an error if handle creation fails.
    pub fn new() -> Result<Self> {
        let handle = gribjump_sys::new_gribjump()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(HandleInner(handle))),
        })
    }

    /// Extract data using the given requests.
    ///
    /// # Arguments
    ///
    /// * `requests` - Slice of extraction requests
    ///
    /// # Errors
    ///
    /// Returns an error if extraction fails.
    pub fn extract(&self, requests: &[ExtractionRequest]) -> Result<ExtractionIterator> {
        let cxx_requests: Vec<_> = requests.iter().map(ExtractionRequest::to_cxx).collect();
        let mut guard = self.inner.lock();
        let it = gribjump_sys::extract(guard.0.pin_mut(), &cxx_requests)?;
        drop(guard);
        Ok(ExtractionIterator::new(it))
    }

    /// Extract data from file paths.
    ///
    /// # Arguments
    ///
    /// * `requests` - Slice of path-based extraction requests
    ///
    /// # Errors
    ///
    /// Returns an error if extraction fails.
    pub fn extract_from_paths(
        &self,
        requests: &[PathExtractionRequest],
    ) -> Result<ExtractionIterator> {
        let cxx_requests: Vec<_> = requests.iter().map(PathExtractionRequest::to_cxx).collect();
        let mut guard = self.inner.lock();
        let it = gribjump_sys::extract_from_paths(guard.0.pin_mut(), &cxx_requests)?;
        drop(guard);
        Ok(ExtractionIterator::new(it))
    }

    /// Extract from a MARS request string.
    ///
    /// This is useful for high-cardinality requests where the MARS request
    /// will be expanded internally by gribjump.
    ///
    /// # Arguments
    ///
    /// * `request` - MARS request string
    /// * `ranges` - Ranges to extract
    /// * `grid_hash` - Grid hash for validation
    ///
    /// # Errors
    ///
    /// Returns an error if extraction fails.
    pub fn extract_mars(
        &self,
        request: &str,
        ranges: &[Range],
        grid_hash: &str,
    ) -> Result<ExtractionIterator> {
        let cxx_ranges: Vec<_> = ranges.iter().copied().map(Range::to_cxx).collect();
        let mut guard = self.inner.lock();
        let it = gribjump_sys::extract_mars(guard.0.pin_mut(), request, &cxx_ranges, grid_hash)?;
        drop(guard);
        Ok(ExtractionIterator::new(it))
    }

    /// Extract from a file with specific message offsets.
    ///
    /// # Arguments
    ///
    /// * `extraction` - File extraction specification
    ///
    /// # Errors
    ///
    /// Returns an error if extraction fails.
    pub fn extract_from_file(&self, extraction: &FileExtraction) -> Result<ExtractionIterator> {
        let cxx_data = extraction.to_cxx();
        let mut guard = self.inner.lock();
        let it = gribjump_sys::extract_from_file(guard.0.pin_mut(), &cxx_data)?;
        drop(guard);
        Ok(ExtractionIterator::new(it))
    }

    /// Get axes information for a request.
    ///
    /// Returns a map of axis names to their possible values.
    ///
    /// # Arguments
    ///
    /// * `request` - MARS request string
    /// * `level` - Axis level (typically 1, 2, or 3)
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use gribjump::GribJump;
    ///
    /// let gj = GribJump::new().unwrap();
    /// let axes = gj.axes("class=od", 1).unwrap();
    ///
    /// for (name, values) in &axes {
    ///     println!("{}: {:?}", name, values);
    /// }
    /// ```
    pub fn axes(&self, request: &str, level: i32) -> Result<HashMap<String, Vec<String>>> {
        let mut guard = self.inner.lock();
        let entries = gribjump_sys::axes(guard.0.pin_mut(), request, level)?;
        drop(guard);
        Ok(entries.into_iter().map(|e| (e.key, e.values)).collect())
    }

    /// Scan GRIB files at the given paths.
    ///
    /// This indexes the files for faster subsequent extraction.
    ///
    /// # Arguments
    ///
    /// * `paths` - Paths to GRIB files to scan
    ///
    /// # Returns
    ///
    /// The number of messages scanned.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning fails.
    pub fn scan_paths<S: AsRef<str>>(&self, paths: &[S]) -> Result<usize> {
        let cxx_paths: Vec<String> = paths.iter().map(|p| p.as_ref().to_string()).collect();
        let mut guard = self.inner.lock();
        let count = gribjump_sys::scan_paths(guard.0.pin_mut(), &cxx_paths)?;
        drop(guard);
        Ok(count)
    }

    /// Scan using MARS requests.
    ///
    /// # Arguments
    ///
    /// * `requests` - MARS request strings to scan
    /// * `by_files` - If true, scan by files rather than by messages
    ///
    /// # Returns
    ///
    /// The number of messages scanned.
    ///
    /// # Errors
    ///
    /// Returns an error if scanning fails.
    pub fn scan_requests<S: AsRef<str>>(&self, requests: &[S], by_files: bool) -> Result<usize> {
        let cxx_requests: Vec<String> = requests.iter().map(|r| r.as_ref().to_string()).collect();
        let mut guard = self.inner.lock();
        let count = gribjump_sys::scan_requests(guard.0.pin_mut(), &cxx_requests, by_files)?;
        drop(guard);
        Ok(count)
    }

    /// Print statistics to stdout.
    ///
    /// This is primarily for debugging and diagnostics.
    pub fn print_stats(&self) -> Result<()> {
        gribjump_sys::stats(self.inner.lock().0.pin_mut())?;
        Ok(())
    }
}
