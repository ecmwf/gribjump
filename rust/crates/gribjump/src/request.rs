//! Extraction request types.

use crate::error::{Error, Result};

/// A range of indices to extract (start and end inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Range {
    /// Start index (inclusive).
    pub start: usize,
    /// End index (inclusive).
    pub end: usize,
}

impl Range {
    /// Create a new range.
    ///
    /// # Errors
    ///
    /// Returns an error if `start > end`.
    pub fn new(start: usize, end: usize) -> Result<Self> {
        if start > end {
            return Err(Error::InvalidArgument(format!(
                "Range start ({start}) must be <= end ({end})"
            )));
        }
        Ok(Self { start, end })
    }

    /// Create a range without validation.
    ///
    /// Use when you know the range is valid.
    #[must_use]
    pub const fn new_unchecked(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// Returns the number of elements in the range.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.end.saturating_sub(self.start) + 1
    }

    /// Returns true if the range is empty (start > end, which shouldn't happen).
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.start > self.end
    }

    /// Convert to the cxx Range type.
    ///
    /// The Rust Range uses inclusive end, but gribjump C++ uses exclusive end,
    /// so we add 1 to the end value.
    #[must_use]
    pub(crate) const fn to_cxx(self) -> gribjump_sys::Range {
        gribjump_sys::Range {
            start: self.start,
            end: self.end + 1,
        }
    }
}

/// An extraction request for gribjump.
#[derive(Debug, Clone)]
pub struct ExtractionRequest {
    /// MARS request string (e.g., "class=od,expver=0001,...")
    pub request_str: String,
    /// Ranges to extract
    pub ranges: Vec<Range>,
    /// Grid hash for validation (required by gribjump)
    pub grid_hash: String,
}

impl ExtractionRequest {
    /// Create a new extraction request.
    ///
    /// # Arguments
    ///
    /// * `request` - MARS request string
    /// * `ranges` - Ranges to extract
    /// * `grid_hash` - Grid hash for validation
    #[must_use]
    pub fn new(
        request: impl Into<String>,
        ranges: Vec<Range>,
        grid_hash: impl Into<String>,
    ) -> Self {
        Self {
            request_str: request.into(),
            ranges,
            grid_hash: grid_hash.into(),
        }
    }

    /// Convert to the cxx request data type.
    #[must_use]
    pub(crate) fn to_cxx(&self) -> gribjump_sys::ExtractionRequestData {
        gribjump_sys::ExtractionRequestData {
            request_str: self.request_str.clone(),
            ranges: self.ranges.iter().copied().map(Range::to_cxx).collect(),
            grid_hash: self.grid_hash.clone(),
        }
    }
}

/// A path-based extraction request for gribjump.
///
/// Used for extracting data directly from GRIB files.
#[derive(Debug, Clone)]
pub struct PathExtractionRequest {
    /// Path to the GRIB file
    pub filename: String,
    /// URI scheme (e.g., "file", "fdb")
    pub scheme: String,
    /// Offset within the file
    pub offset: usize,
    /// Host for remote access
    pub host: Option<String>,
    /// Port for remote access
    pub port: i32,
    /// Ranges to extract
    pub ranges: Vec<Range>,
    /// Grid hash for validation (required by gribjump)
    pub grid_hash: String,
}

impl PathExtractionRequest {
    /// Create a new path-based extraction request.
    #[must_use]
    pub fn new(
        filename: impl Into<String>,
        ranges: Vec<Range>,
        grid_hash: impl Into<String>,
    ) -> Self {
        Self {
            filename: filename.into(),
            scheme: "file".to_string(),
            offset: 0,
            host: None,
            port: 0,
            ranges,
            grid_hash: grid_hash.into(),
        }
    }

    /// Set the URI scheme.
    #[must_use]
    pub fn with_scheme(mut self, scheme: impl Into<String>) -> Self {
        self.scheme = scheme.into();
        self
    }

    /// Set the offset within the file.
    #[must_use]
    pub const fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    /// Set the remote host.
    #[must_use]
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Set the remote port.
    #[must_use]
    pub const fn with_port(mut self, port: i32) -> Self {
        self.port = port;
        self
    }

    /// Convert to the cxx request data type.
    #[must_use]
    pub(crate) fn to_cxx(&self) -> gribjump_sys::PathExtractionRequestData {
        gribjump_sys::PathExtractionRequestData {
            filename: self.filename.clone(),
            scheme: self.scheme.clone(),
            offset: self.offset,
            host: self.host.clone().unwrap_or_default(),
            port: self.port,
            ranges: self.ranges.iter().copied().map(Range::to_cxx).collect(),
            grid_hash: self.grid_hash.clone(),
        }
    }
}

/// Data for file-based extraction with specific message offsets.
///
/// Used by `extract_from_file` to extract from specific GRIB messages
/// at known offsets within a file.
#[derive(Debug, Clone, Default)]
pub struct FileExtraction {
    /// Path to the GRIB file
    pub path: String,
    /// Message extractions: (offset, ranges) pairs
    messages: Vec<(u64, Vec<Range>)>,
}

impl FileExtraction {
    /// Create a new file extraction request.
    #[must_use]
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            messages: Vec::new(),
        }
    }

    /// Add a message extraction.
    ///
    /// # Arguments
    ///
    /// * `offset` - Byte offset of the GRIB message within the file
    /// * `ranges` - Ranges to extract from this message
    #[must_use]
    pub fn with_message(mut self, offset: u64, ranges: Vec<Range>) -> Self {
        self.messages.push((offset, ranges));
        self
    }

    /// Add multiple message extractions.
    #[must_use]
    pub fn with_messages(mut self, messages: impl IntoIterator<Item = (u64, Vec<Range>)>) -> Self {
        self.messages.extend(messages);
        self
    }

    /// Convert to the cxx file extraction data type.
    #[must_use]
    pub(crate) fn to_cxx(&self) -> gribjump_sys::FileExtractionData {
        let mut offsets = Vec::with_capacity(self.messages.len());
        let mut ranges = Vec::new();
        let mut ranges_offsets = Vec::with_capacity(self.messages.len());

        for (offset, msg_ranges) in &self.messages {
            offsets.push(*offset);
            ranges_offsets.push(ranges.len());
            for r in msg_ranges {
                ranges.push((*r).to_cxx());
            }
        }

        gribjump_sys::FileExtractionData {
            path: self.path.clone(),
            offsets,
            ranges,
            ranges_offsets,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_creation() {
        let r = Range::new(0, 100).expect("valid range");
        assert_eq!(r.len(), 101); // inclusive
        assert!(!r.is_empty());
    }

    #[test]
    fn test_range_single_element() {
        let r = Range::new(50, 50).expect("valid range");
        assert_eq!(r.len(), 1);
        assert!(!r.is_empty());
    }

    #[test]
    fn test_range_invalid() {
        let result = Range::new(100, 50);
        assert!(result.is_err());
    }

    #[test]
    fn test_extraction_request() {
        let ranges = vec![Range::new_unchecked(0, 100)];
        let req = ExtractionRequest::new("class=od,expver=0001", ranges, "abc123");

        assert_eq!(req.request_str, "class=od,expver=0001");
        assert_eq!(req.grid_hash, "abc123");
    }

    #[test]
    fn test_path_extraction_request_builder() {
        let ranges = vec![Range::new_unchecked(0, 100)];
        let req = PathExtractionRequest::new("/path/to/file.grib", ranges, "abc123")
            .with_scheme("fdb")
            .with_offset(1024)
            .with_host("localhost")
            .with_port(8080);

        assert_eq!(req.filename, "/path/to/file.grib");
        assert_eq!(req.scheme, "fdb");
        assert_eq!(req.offset, 1024);
        assert_eq!(req.host, Some("localhost".to_string()));
        assert_eq!(req.port, 8080);
        assert_eq!(req.grid_hash, "abc123");
    }

    #[test]
    fn test_file_extraction_builder() {
        let fe = FileExtraction::new("/path/to/file.grib")
            .with_message(0, vec![Range::new_unchecked(0, 100)])
            .with_message(1024, vec![Range::new_unchecked(0, 50)]);

        assert_eq!(fe.path, "/path/to/file.grib");
        assert_eq!(fe.messages.len(), 2);
    }
}
