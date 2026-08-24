/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// The shared enumeration index used to key the streaming (v4) FORWARD_EXTRACT
/// reply. Because streaming sends results in task-completion order rather than
/// filemap order, each chunk must carry the index of the item it belongs to.
///
/// The index is deterministic and derived identically on both ends: files in
/// filemap (std::map) order, items in each ExtractionItems vector order. That
/// vector order is offset-ascending on the wire -- encodeForwardExtractRequest
/// sorts each file's items by offset in place before sending, and
/// decodeForwardExtractRequest reconstructs them in that received order -- so
/// the proxy (post-sort) and the leaf agree without transmitting anything
/// beyond the index itself.

#pragma once

#include <cstddef>
#include <unordered_map>
#include <vector>

#include "gribjump/ExtractionItem.h"
#include "gribjump/Types.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

///@todo: Bake this info into extraction items, then remove all this logic. (v4)


/// Flatten a filemap into index -> item order (the shared streaming index).
/// Used by the proxy decode to slot each incoming (index, result) chunk back
/// into the correct ExtractionItem.
inline std::vector<ExtractionItem*> flattenFilemap(const filemap_t& filemap) {
    std::vector<ExtractionItem*> byIndex;
    for (const auto& [fname, extractionItems] : filemap) {
        for (ExtractionItem* item : extractionItems) {
            byIndex.push_back(item);
        }
    }
    return byIndex;
}

/// Inverse of flattenFilemap: item -> index. Used by the leaf sink to label
/// each harvested item (which arrives as a pointer, not an index) with its
/// enumeration index for the outgoing chunk.
inline std::unordered_map<ExtractionItem*, size_t> filemapItemIndex(const filemap_t& filemap) {
    std::unordered_map<ExtractionItem*, size_t> indexOf;
    size_t index = 0;
    for (const auto& [fname, extractionItems] : filemap) {
        for (ExtractionItem* item : extractionItems) {
            indexOf.emplace(item, index++);
        }
    }
    return indexOf;
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
