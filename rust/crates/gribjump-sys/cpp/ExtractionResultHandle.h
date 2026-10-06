// gribjump ExtractionResultHandle bridge — zero-copy wrapper around
// `gribjump::ExtractionResult`. Values are truly zero-copy; masks are
// converted lazily from `std::bitset<64>` to `uint64_t` on first access
// (cached).
#pragma once

#include "gribjump/ExtractionData.h"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace gribjump_bridge {

//----------------------------------------------------------------------------------------------------------------------

class ExtractionResultHandle {
public:

    explicit ExtractionResultHandle(gribjump::ExtractionResult&& result);
    ~ExtractionResultHandle() = default;

    ExtractionResultHandle(const ExtractionResultHandle&)            = delete;
    ExtractionResultHandle& operator=(const ExtractionResultHandle&) = delete;
    ExtractionResultHandle(ExtractionResultHandle&&)                 = default;
    ExtractionResultHandle& operator=(ExtractionResultHandle&&)      = default;

    size_t num_ranges() const;
    const double* values_ptr(size_t range_idx) const;
    size_t values_len(size_t range_idx) const;
    const uint64_t* masks_ptr(size_t range_idx) const;
    size_t masks_len(size_t range_idx) const;

private:

    bool try_convert_masks() const;

    gribjump::ExtractionResult result_;
    mutable std::vector<std::vector<uint64_t>> masks_cache_;
    mutable bool masks_converted_ = false;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump_bridge
