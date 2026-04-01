// gribjump_bridge.h - C++ bridge declarations for cxx
//
// This header declares wrapper types and shim functions that convert between
// the native gribjump C++ API and cxx-compatible types.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "rust/cxx.h"

// Include eckit exception for the global trycatch handler
#include "eckit/exception/Exceptions.h"

// Custom exception handler for cxx - catches eckit exceptions globally
// This replaces per-function try-catch blocks throughout the bridge
// Exception messages are prefixed with type for Rust-side discrimination
// Order matters: catch specific exceptions before base classes
namespace rust::behavior {
template <typename Try, typename Fail>
static void trycatch(Try&& func, Fail&& fail) noexcept try {
    func();
}
catch (const eckit::SeriousBug& e) {
    fail((std::string("ECKIT_SERIOUS_BUG: ") + e.what()).c_str());
}
catch (const eckit::UserError& e) {
    fail((std::string("ECKIT_USER_ERROR: ") + e.what()).c_str());
}
catch (const eckit::BadParameter& e) {
    fail((std::string("ECKIT_BAD_PARAMETER: ") + e.what()).c_str());
}
catch (const eckit::NotImplemented& e) {
    fail((std::string("ECKIT_NOT_IMPLEMENTED: ") + e.what()).c_str());
}
catch (const eckit::OutOfRange& e) {
    fail((std::string("ECKIT_OUT_OF_RANGE: ") + e.what()).c_str());
}
catch (const eckit::FileError& e) {
    fail((std::string("ECKIT_FILE_ERROR: ") + e.what()).c_str());
}
catch (const eckit::AssertionFailed& e) {
    fail((std::string("ECKIT_ASSERTION_FAILED: ") + e.what()).c_str());
}
catch (const eckit::Exception& e) {
    fail((std::string("ECKIT: ") + e.what()).c_str());
}
catch (const std::exception& e) {
    fail(e.what());
}
catch (...) {
    fail("unknown exception (non-std::exception type)");
}
}  // namespace rust::behavior

#include "gribjump/GribJump.h"
#include "gribjump/api/ExtractionIterator.h"
#include "gribjump/ExtractionData.h"

namespace gribjump::ffi {

// ============================================================================
// Shared struct forward declarations (defined by cxx in generated code)
// ============================================================================

struct Range;
struct ExtractionRequestData;
struct PathExtractionRequestData;
struct ExtractionResultData;
struct AxisEntry;
struct FileExtractionData;

// ============================================================================
// Wrapper classes for opaque C++ types
// ============================================================================

/// Wrapper around gribjump::GribJump that can be passed through cxx.
class GribJumpHandle {
public:

    GribJumpHandle();
    ~GribJumpHandle() = default;

    // Non-copyable, non-movable
    GribJumpHandle(const GribJumpHandle&)            = delete;
    GribJumpHandle& operator=(const GribJumpHandle&) = delete;
    GribJumpHandle(GribJumpHandle&&)                 = delete;
    GribJumpHandle& operator=(GribJumpHandle&&)      = delete;

    /// Access the underlying GribJump instance.
    gribjump::GribJump& inner() { return impl_; }
    const gribjump::GribJump& inner() const { return impl_; }

private:

    gribjump::GribJump impl_;
};

/// Wrapper around gribjump::ExtractionIterator.
class ExtractionIteratorHandle {
public:

    explicit ExtractionIteratorHandle(gribjump::ExtractionIterator&& it);
    ~ExtractionIteratorHandle() = default;

    // Non-copyable
    ExtractionIteratorHandle(const ExtractionIteratorHandle&)            = delete;
    ExtractionIteratorHandle& operator=(const ExtractionIteratorHandle&) = delete;

    // Movable
    ExtractionIteratorHandle(ExtractionIteratorHandle&&)            = default;
    ExtractionIteratorHandle& operator=(ExtractionIteratorHandle&&) = default;

    /// Check if there are more results.
    bool hasNext() const;

    /// Get the next result as a zero-copy handle.
    std::unique_ptr<class ExtractionResultHandle> next();

private:

    gribjump::ExtractionIterator impl_;
};

/// Zero-copy wrapper around gribjump::ExtractionResult.
/// Values are truly zero-copy; masks are converted lazily from bitset<64>.
class ExtractionResultHandle {
public:

    explicit ExtractionResultHandle(gribjump::ExtractionResult&& result);
    ~ExtractionResultHandle() = default;

    // Non-copyable
    ExtractionResultHandle(const ExtractionResultHandle&)            = delete;
    ExtractionResultHandle& operator=(const ExtractionResultHandle&) = delete;

    // Movable
    ExtractionResultHandle(ExtractionResultHandle&&)            = default;
    ExtractionResultHandle& operator=(ExtractionResultHandle&&) = default;

    /// Get the number of ranges in this result.
    size_t num_ranges() const;

    /// Get raw pointer to values for a specific range (zero-copy).
    const double* values_ptr(size_t range_idx) const;

    /// Get the number of values in a specific range.
    size_t values_len(size_t range_idx) const;

    /// Get raw pointer to masks for a specific range.
    /// Each mask element is a uint64_t where each bit represents validity.
    /// Note: Masks are converted from bitset<64> on first access (cached).
    const uint64_t* masks_ptr(size_t range_idx) const;

    /// Get the number of mask elements in a specific range.
    size_t masks_len(size_t range_idx) const;

private:

    gribjump::ExtractionResult result_;
    // Cached masks converted from bitset<64> to uint64_t
    mutable std::vector<std::vector<uint64_t>> masks_cache_;
    mutable bool masks_converted_         = false;
    mutable bool masks_conversion_failed_ = false;

    /// Convert masks from bitset<64> to uint64_t (lazy, cached).
    /// Returns true on success, false if conversion failed (OOM).
    bool try_convert_masks() const noexcept;
};

// ============================================================================
// Library metadata functions
// ============================================================================

/// Get the gribjump library version string.
rust::String gribjump_version();

/// Get the gribjump git SHA1 hash.
rust::String gribjump_git_sha1();

// ============================================================================
// Handle lifecycle functions
// ============================================================================

/// Create a new GribJump handle.
std::unique_ptr<GribJumpHandle> new_gribjump();

// ============================================================================
// Extraction functions
// ============================================================================

/// Extract data using standard ExtractionRequests.
std::unique_ptr<ExtractionIteratorHandle> extract(GribJumpHandle& handle,
                                                  const rust::Vec<ExtractionRequestData>& requests);

/// Extract data using PathExtractionRequests.
std::unique_ptr<ExtractionIteratorHandle> extract_from_paths(GribJumpHandle& handle,
                                                             const rust::Vec<PathExtractionRequestData>& requests);

/// Extract from a MARS request (expands internally).
std::unique_ptr<ExtractionIteratorHandle> extract_mars(GribJumpHandle& handle, rust::Str request,
                                                       const rust::Vec<Range>& ranges, rust::Str grid_hash);

/// Extract from a specific file with message offsets.
std::unique_ptr<ExtractionIteratorHandle> extract_from_file(GribJumpHandle& handle, const FileExtractionData& data);

// ============================================================================
// Axes query functions
// ============================================================================

/// Query available axes for a given request.
rust::Vec<AxisEntry> axes(GribJumpHandle& handle, rust::Str request, int32_t level);

// ============================================================================
// Scanning functions
// ============================================================================

/// Scan GRIB files at the given paths.
size_t scan_paths(GribJumpHandle& handle, const rust::Vec<rust::String>& paths);

/// Scan using MARS requests.
size_t scan_requests(GribJumpHandle& handle, const rust::Vec<rust::String>& requests, bool by_files);

// ============================================================================
// Diagnostics functions
// ============================================================================

/// Print statistics to stdout.
void stats(GribJumpHandle& handle);

// ============================================================================
// Test functions (for verifying exception handling)
// ============================================================================

/// Test function that throws eckit::Exception
void test_throw_eckit_exception();

/// Test function that throws eckit::SeriousBug
void test_throw_eckit_serious_bug();

/// Test function that throws eckit::UserError
void test_throw_eckit_user_error();

/// Test function that throws std::runtime_error
void test_throw_std_exception();

/// Test function that throws an int (non-std::exception type)
void test_throw_int();

}  // namespace gribjump::ffi
