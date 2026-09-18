// gribjump ExtractionResultHandle bridge — implementation.

#include "gribjump_exceptions.h"

#include "ExtractionResultHandle.h"
#include "gribjump-sys/src/lib.rs.h"

namespace gribjump_bridge {

//----------------------------------------------------------------------------------------------------------------------

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
    if (masks_converted_) {
        return true;
    }

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

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump_bridge
