//! Extraction iterator.

use gribjump_sys::UniquePtr;

use crate::error::{Error, Result};
use crate::result::ExtractionResult;

/// Iterator over extraction results.
///
/// Yields `ExtractionResult` for each field extracted.
pub struct ExtractionIterator {
    handle: UniquePtr<gribjump_sys::ExtractionIteratorHandle>,
}

impl ExtractionIterator {
    /// Create a new extraction iterator from a cxx handle.
    pub(crate) const fn new(handle: UniquePtr<gribjump_sys::ExtractionIteratorHandle>) -> Self {
        Self { handle }
    }

    /// Check if there are more results available.
    #[must_use]
    pub fn has_next(&self) -> bool {
        self.handle.hasNext()
    }

    /// Get the next result.
    ///
    /// # Errors
    ///
    /// Returns an error if the iterator is exhausted or extraction fails.
    pub fn next_result(&mut self) -> Result<ExtractionResult> {
        if !self.handle.hasNext() {
            return Err(Error::IteratorExhausted);
        }

        let handle = self.handle.pin_mut().next()?;
        Ok(ExtractionResult::from_handle(handle))
    }
}

impl Iterator for ExtractionIterator {
    type Item = Result<ExtractionResult>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.handle.hasNext() {
            return None;
        }

        match self.handle.pin_mut().next() {
            Ok(handle) => Some(Ok(ExtractionResult::from_handle(handle))),
            Err(e) => Some(Err(e.into())),
        }
    }
}

// SAFETY: The underlying C++ iterator is not thread-safe, but we only allow
// mutable access through &mut self, so this is safe.
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for ExtractionIterator {}
