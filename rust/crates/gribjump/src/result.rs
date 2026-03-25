//! Extraction result types.

use std::fmt;

use gribjump_sys::UniquePtr;

/// Result from a single extraction containing values and masks for each range.
///
/// This struct holds a handle to C++ data and provides zero-copy access
/// to individual ranges via [`RangeView`]. The data lives in C++ memory
/// and is accessed through raw pointers.
pub struct ExtractionResult {
    /// Handle to the C++ `ExtractionResult` (owns the data)
    handle: UniquePtr<gribjump_sys::ExtractionResultHandle>,
}

impl fmt::Debug for ExtractionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtractionResult")
            .field("num_ranges", &self.num_ranges())
            .field("total_values", &self.total_values())
            .finish()
    }
}

// SAFETY: ExtractionResult can be safely sent between threads because:
// 1. The C++ ExtractionResultHandle is uniquely owned (not shared)
// 2. The underlying data (values/masks vectors) is immutable after creation
// 3. There is no thread-local state in the C++ code
// 4. The UniquePtr ensures exclusive ownership during transfer
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for ExtractionResult {}

/// Borrowed view into a single range's values and masks.
///
/// This is a zero-copy view into the parent [`ExtractionResult`].
/// The data lives in C++ memory and is borrowed for the lifetime of
/// the parent result.
///
/// Use [`to_owned`](RangeView::to_owned) if you need owned data.
#[derive(Debug, Clone, Copy)]
pub struct RangeView<'a> {
    /// Extracted values (borrowed slice into C++ memory)
    values: &'a [f64],
    /// Validity masks (borrowed slice into C++ memory, each u64 is a bitset for 64 values)
    masks: &'a [u64],
}

/// Owned values and mask for a single range extraction.
///
/// Use this when you need to store the data beyond the lifetime of the
/// [`ExtractionResult`], or when you need to mutate the data.
#[derive(Debug, Clone, Default)]
pub struct RangeResult {
    /// Extracted values
    pub values: Vec<f64>,
    /// Validity masks (each u64 is a bitset for 64 values)
    pub masks: Vec<u64>,
}

impl ExtractionResult {
    /// Create from a C++ `ExtractionResultHandle`.
    ///
    /// This is a zero-copy operation - data stays in C++ memory.
    pub(crate) const fn from_handle(
        handle: UniquePtr<gribjump_sys::ExtractionResultHandle>,
    ) -> Self {
        Self { handle }
    }

    /// Get a borrowed view of a specific range by index.
    ///
    /// Returns `None` if the index is out of bounds.
    ///
    /// # Safety (internal)
    ///
    /// This uses `unsafe` internally to create slices from C++ pointers.
    /// The safety is guaranteed because:
    /// - The handle owns the C++ data and outlives the returned view
    /// - The returned `RangeView` borrows from `&self`, preventing use-after-free
    /// - The C++ pointers are valid for the lifetime of the handle
    #[must_use]
    pub fn range(&self, index: usize) -> Option<RangeView<'_>> {
        if index >= self.handle.num_ranges() {
            return None;
        }

        // SAFETY:
        // - handle owns the C++ ExtractionResult which owns the data
        // - RangeView lifetime is tied to &self via the return type
        // - Data is valid and immutable for the lifetime of handle
        // - Pointers returned by C++ point to valid contiguous arrays
        let values = unsafe {
            let ptr = self.handle.values_ptr(index);
            let len = self.handle.values_len(index);
            if ptr.is_null() || len == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(ptr, len)
            }
        };

        let masks = unsafe {
            let ptr = self.handle.masks_ptr(index);
            let len = self.handle.masks_len(index);
            if ptr.is_null() || len == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(ptr, len)
            }
        };

        Some(RangeView { values, masks })
    }

    /// Get the number of ranges in this result.
    #[must_use]
    pub fn num_ranges(&self) -> usize {
        self.handle.num_ranges()
    }

    /// Check if this result is empty (no ranges).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.handle.num_ranges() == 0
    }

    /// Get the result for a specific range by index (borrowed view).
    #[must_use]
    pub fn get(&self, index: usize) -> Option<RangeView<'_>> {
        self.range(index)
    }

    /// Get the total number of values across all ranges.
    #[must_use]
    pub fn total_values(&self) -> usize {
        (0..self.num_ranges())
            .map(|i| self.handle.values_len(i))
            .sum()
    }

    /// Iterate over range views (zero-copy).
    pub fn iter(&self) -> impl Iterator<Item = RangeView<'_>> {
        (0..self.num_ranges()).filter_map(|i| self.range(i))
    }

    /// Convert all ranges to owned data.
    ///
    /// This performs a copy of all values and masks from C++ to Rust.
    #[must_use]
    pub fn to_owned_ranges(&self) -> Vec<RangeResult> {
        self.iter().map(|r| r.to_owned()).collect()
    }
}

impl<'a> RangeView<'a> {
    /// Get the values for this range.
    #[must_use]
    pub const fn values(&self) -> &'a [f64] {
        self.values
    }

    /// Get the masks for this range.
    #[must_use]
    pub const fn masks(&self) -> &'a [u64] {
        self.masks
    }

    /// Convert to owned data.
    ///
    /// This performs a copy of the values and masks from C++ to Rust.
    #[must_use]
    pub fn to_owned(&self) -> RangeResult {
        RangeResult {
            values: self.values.to_vec(),
            masks: self.masks.to_vec(),
        }
    }

    /// Check if a value at the given index is valid according to the mask.
    ///
    /// # Arguments
    ///
    /// * `index` - The value index to check
    ///
    /// # Returns
    ///
    /// `true` if the value is valid (bit is set), `false` otherwise.
    /// Returns `true` if no mask is available (all values assumed valid).
    #[must_use]
    pub fn is_valid(&self, index: usize) -> bool {
        if self.masks.is_empty() {
            return true;
        }

        let word_idx = index / 64;
        let bit_idx = index % 64;

        if word_idx >= self.masks.len() {
            return true;
        }

        (self.masks[word_idx] & (1u64 << bit_idx)) != 0
    }

    /// Get the number of values in this range.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if this range view is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Iterate over values with their validity status.
    pub fn iter_with_validity(&self) -> impl Iterator<Item = (f64, bool)> + '_ {
        self.values
            .iter()
            .enumerate()
            .map(|(i, &v)| (v, self.is_valid(i)))
    }

    /// Get only the valid values.
    pub fn valid_values(&self) -> impl Iterator<Item = f64> + '_ {
        self.values
            .iter()
            .enumerate()
            .filter(|(i, _)| self.is_valid(*i))
            .map(|(_, &v)| v)
    }
}

impl RangeResult {
    /// Check if a value at the given index is valid according to the mask.
    #[must_use]
    pub fn is_valid(&self, index: usize) -> bool {
        if self.masks.is_empty() {
            return true;
        }

        let word_idx = index / 64;
        let bit_idx = index % 64;

        if word_idx >= self.masks.len() {
            return true;
        }

        (self.masks[word_idx] & (1u64 << bit_idx)) != 0
    }

    /// Get the number of values in this range.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if this range result is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl IntoIterator for ExtractionResult {
    type Item = RangeResult;
    type IntoIter = std::vec::IntoIter<RangeResult>;

    fn into_iter(self) -> Self::IntoIter {
        self.to_owned_ranges().into_iter()
    }
}
