// gribjump_bridge.cpp - C++ bridge implementation
//
// This file implements the shim functions that convert between the native
// gribjump C++ API and cxx-compatible types.

#include "gribjump_bridge.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/GribJump.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/api/ExtractionIterator.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/runtime/Main.h"
#include "metkit/mars/MarsRequest.h"

#include <stdexcept>

// Include the cxx-generated header for our bridge types
#include "gribjump-sys/src/lib.rs.h"

namespace gribjump::ffi {

// ============================================================================
// Helper functions for type conversion
// ============================================================================

/// Convert rust::Vec<Range> to std::vector<gribjump::Range>
static std::vector<gribjump::Range> to_cpp_ranges(const rust::Vec<Range>& ranges) {
    std::vector<gribjump::Range> result;
    result.reserve(ranges.size());
    for (const auto& r : ranges) {
        result.emplace_back(r.start, r.end);
    }
    return result;
}

/// Convert rust::Vec<ExtractionRequestData> to std::vector<gribjump::ExtractionRequest>
static std::vector<gribjump::ExtractionRequest> to_cpp_requests(const rust::Vec<ExtractionRequestData>& requests) {
    std::vector<gribjump::ExtractionRequest> result;
    result.reserve(requests.size());
    for (const auto& req : requests) {
        result.emplace_back(std::string(req.request_str), to_cpp_ranges(req.ranges), std::string(req.grid_hash));
    }
    return result;
}

/// Convert rust::Vec<PathExtractionRequestData> to std::vector<gribjump::PathExtractionRequest>
static std::vector<gribjump::PathExtractionRequest> to_cpp_path_requests(
    const rust::Vec<PathExtractionRequestData>& requests) {
    std::vector<gribjump::PathExtractionRequest> result;
    result.reserve(requests.size());
    for (const auto& req : requests) {
        result.emplace_back(std::string(req.filename), std::string(req.scheme), req.offset, std::string(req.host),
                            req.port, to_cpp_ranges(req.ranges), std::string(req.grid_hash));
    }
    return result;
}

// ============================================================================
// GribJumpHandle implementation
// ============================================================================

GribJumpHandle::GribJumpHandle() : impl_() {}

// ============================================================================
// ExtractionIteratorHandle implementation
// ============================================================================

ExtractionIteratorHandle::ExtractionIteratorHandle(gribjump::ExtractionIterator&& it) : impl_(std::move(it)) {}

bool ExtractionIteratorHandle::hasNext() const {
    return impl_.hasNext();
}

std::unique_ptr<ExtractionResultHandle> ExtractionIteratorHandle::next() {
    if (!impl_.hasNext()) {
        throw eckit::SeriousBug("Iterator exhausted: next() called after hasNext() returned false");
    }
    // impl_.next() returns unique_ptr<ExtractionResult>, dereference and move
    auto result_ptr = impl_.next();
    return std::make_unique<ExtractionResultHandle>(std::move(*result_ptr));
}

// ============================================================================
// ExtractionResultHandle implementation (zero-copy)
// ============================================================================

ExtractionResultHandle::ExtractionResultHandle(gribjump::ExtractionResult&& result) :
    result_(std::move(result)), masks_converted_(false) {}

size_t ExtractionResultHandle::num_ranges() const {
    return result_.values().size();
}

const double* ExtractionResultHandle::values_ptr(size_t range_idx) const {
    if (range_idx >= result_.values().size()) {
        return nullptr;
    }
    return result_.values()[range_idx].data();
}

size_t ExtractionResultHandle::values_len(size_t range_idx) const {
    if (range_idx >= result_.values().size()) {
        return 0;
    }
    return result_.values()[range_idx].size();
}

bool ExtractionResultHandle::try_convert_masks() const {
    // Already converted or failed - don't retry
    if (masks_converted_ || masks_conversion_failed_) {
        return masks_converted_;
    }

    try {
        const auto& masks = result_.mask();
        masks_cache_.reserve(masks.size());

        for (const auto& range_masks : masks) {
            std::vector<uint64_t> converted;
            converted.reserve(range_masks.size());
            for (const auto& bits : range_masks) {
                converted.push_back(bits.to_ullong());
            }
            masks_cache_.push_back(std::move(converted));
        }

        masks_converted_ = true;
        return true;
    }
    catch (const std::bad_alloc&) {
        // OOM - handle gracefully by returning false
        masks_conversion_failed_ = true;
        masks_cache_.clear();  // Release any partial allocations
        return false;
    }
}

const uint64_t* ExtractionResultHandle::masks_ptr(size_t range_idx) const {
    if (!try_convert_masks()) {
        return nullptr;
    }
    if (range_idx >= masks_cache_.size()) {
        return nullptr;
    }
    return masks_cache_[range_idx].data();
}

size_t ExtractionResultHandle::masks_len(size_t range_idx) const {
    if (!try_convert_masks()) {
        return 0;
    }
    if (range_idx >= masks_cache_.size()) {
        return 0;
    }
    return masks_cache_[range_idx].size();
}

// ============================================================================
// Library metadata functions
// ============================================================================

rust::String gribjump_version() {
    return rust::String(LibGribJump::instance().version());
}

rust::String gribjump_git_sha1() {
    return rust::String(LibGribJump::instance().gitsha1());
}

// ============================================================================
// Handle lifecycle functions
// ============================================================================

std::unique_ptr<GribJumpHandle> new_gribjump() {
    // Initialize eckit Main if not already done
    // This is required for eckit's PathName, Resource, and other subsystems
    if (!eckit::Main::ready()) {
        static const char* argv[] = {"gribjump", nullptr};
        eckit::Main::initialise(1, const_cast<char**>(argv));
    }
    return std::make_unique<GribJumpHandle>();
}

// ============================================================================
// Extraction functions
// ============================================================================

std::unique_ptr<ExtractionIteratorHandle> extract(GribJumpHandle& handle,
                                                  const rust::Vec<ExtractionRequestData>& requests) {
    auto cpp_requests = to_cpp_requests(requests);
    auto it           = handle.inner().extract(cpp_requests);
    return std::make_unique<ExtractionIteratorHandle>(std::move(it));
}

std::unique_ptr<ExtractionIteratorHandle> extract_from_paths(GribJumpHandle& handle,
                                                             const rust::Vec<PathExtractionRequestData>& requests) {
    auto cpp_requests = to_cpp_path_requests(requests);
    auto it           = handle.inner().extract(cpp_requests);
    return std::make_unique<ExtractionIteratorHandle>(std::move(it));
}

std::unique_ptr<ExtractionIteratorHandle> extract_mars(GribJumpHandle& handle, rust::Str request,
                                                       const rust::Vec<Range>& ranges, rust::Str grid_hash) {
    std::string request_str{request};
    auto mars_request = metkit::mars::MarsRequest::parse(request_str);
    auto cpp_ranges   = to_cpp_ranges(ranges);
    std::string hash{grid_hash};
    auto it = handle.inner().extract(mars_request, cpp_ranges, hash);
    return std::make_unique<ExtractionIteratorHandle>(std::move(it));
}

std::unique_ptr<ExtractionIteratorHandle> extract_from_file(GribJumpHandle& handle, const FileExtractionData& data) {
    std::string path_str{data.path};
    eckit::PathName path{path_str};

    // Convert offsets
    std::vector<eckit::Offset> offsets;
    offsets.reserve(data.offsets.size());
    for (auto o : data.offsets) {
        offsets.push_back(eckit::Offset(o));
    }

    // Unflatten ranges: ranges_offsets tells us where each message's ranges start
    std::vector<std::vector<gribjump::Range>> ranges_per_message;
    ranges_per_message.reserve(data.ranges_offsets.size());

    for (size_t i = 0; i < data.ranges_offsets.size(); ++i) {
        size_t start = data.ranges_offsets[i];
        size_t end   = (i + 1 < data.ranges_offsets.size()) ? data.ranges_offsets[i + 1] : data.ranges.size();

        std::vector<gribjump::Range> msg_ranges;
        msg_ranges.reserve(end - start);
        for (size_t j = start; j < end; ++j) {
            msg_ranges.emplace_back(data.ranges[j].start, data.ranges[j].end);
        }
        ranges_per_message.push_back(std::move(msg_ranges));
    }

    auto it = handle.inner().extract(path, offsets, ranges_per_message);
    return std::make_unique<ExtractionIteratorHandle>(std::move(it));
}

// ============================================================================
// Axes query functions
// ============================================================================

rust::Vec<AxisEntry> axes(GribJumpHandle& handle, rust::Str request, int32_t level) {
    std::string request_str(request);
    auto cpp_axes = handle.inner().axes(request_str, level);

    rust::Vec<AxisEntry> result;
    result.reserve(cpp_axes.size());

    for (const auto& [key, values] : cpp_axes) {
        AxisEntry entry;
        entry.key = rust::String(key);
        entry.values.reserve(values.size());
        for (const auto& v : values) {
            entry.values.push_back(rust::String(v));
        }
        result.push_back(std::move(entry));
    }

    return result;
}

// ============================================================================
// Scanning functions
// ============================================================================

size_t scan_paths(GribJumpHandle& handle, const rust::Vec<rust::String>& paths) {
    std::vector<eckit::PathName> cpp_paths;
    cpp_paths.reserve(paths.size());
    for (const auto& p : paths) {
        cpp_paths.emplace_back(std::string(p));
    }
    return handle.inner().scan(cpp_paths);
}

size_t scan_requests(GribJumpHandle& handle, const rust::Vec<rust::String>& requests, bool by_files) {
    std::vector<metkit::mars::MarsRequest> cpp_requests;
    cpp_requests.reserve(requests.size());
    for (const auto& r : requests) {
        cpp_requests.emplace_back(std::string(r));
    }
    return handle.inner().scan(cpp_requests, by_files);
}

// ============================================================================
// Diagnostics functions
// ============================================================================

void stats(GribJumpHandle& handle) {
    handle.inner().stats();
}

// ============================================================================
// Test functions (for verifying exception handling)
// ============================================================================

void test_throw_eckit_exception() {
    throw eckit::Exception("test eckit exception");
}

void test_throw_eckit_serious_bug() {
    throw eckit::SeriousBug("test serious bug");
}

void test_throw_eckit_user_error() {
    throw eckit::UserError("test user error");
}

void test_throw_std_exception() {
    throw std::runtime_error("test std exception");
}

void test_throw_int() {
    throw 42;
}

}  // namespace gribjump::ffi
