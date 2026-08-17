// gribjump GribJumpHandle bridge — wraps `gribjump::GribJump`.
#pragma once

#include "ExtractionIteratorHandle.h"
#include "Types.h"

#include "gribjump/GribJump.h"

#include "rust/cxx.h"

#include <cstddef>
#include <cstdint>
#include <memory>

namespace gribjump_bridge {

//----------------------------------------------------------------------------------------------------------------------

class GribJumpHandle {
public:

    GribJumpHandle();
    ~GribJumpHandle() = default;

    GribJumpHandle(const GribJumpHandle&)            = delete;
    GribJumpHandle& operator=(const GribJumpHandle&) = delete;
    GribJumpHandle(GribJumpHandle&&)                 = delete;
    GribJumpHandle& operator=(GribJumpHandle&&)      = delete;

    /// Access the underlying `gribjump::GribJump` instance.
    gribjump::GribJump& inner() { return impl_; }
    const gribjump::GribJump& inner() const { return impl_; }

    // ============== Extraction ==============

    std::unique_ptr<ExtractionIteratorHandle> extract(const rust::Vec<ExtractionRequestData>& requests);
    std::unique_ptr<ExtractionIteratorHandle> extract_from_paths(const rust::Vec<PathExtractionRequestData>& requests);
    std::unique_ptr<ExtractionIteratorHandle> extract_mars(rust::Str request, const rust::Vec<Range>& ranges,
                                                           rust::Str grid_hash);
    std::unique_ptr<ExtractionIteratorHandle> extract_from_file(const FileExtractionData& data);

    // ============== Query ==============

    rust::Vec<AxisEntry> axes(rust::Str request, int32_t level);

    // ============== Scan / diagnostics ==============

    size_t scan_paths(const rust::Vec<rust::String>& paths);
    size_t scan_requests(const rust::Vec<rust::String>& requests, bool by_files);
    void stats();

    // ============== Factories ==============

    static std::unique_ptr<GribJumpHandle> create();

private:

    gribjump::GribJump impl_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump_bridge
