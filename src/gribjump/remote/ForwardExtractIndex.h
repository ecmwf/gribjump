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
///
/// The leaf stamps this index onto each item at decode time
/// (Protocol::decodeForwardExtractRequest) and reads it back via
/// ExtractionItem::streamIndex(). The proxy still needs the inverse mapping
/// (index -> item) to slot each incoming chunk into place, which flattenFilemap
/// provides below.

#pragma once

#include <cstddef>
#include <vector>

#include "gribjump/ExtractionItem.h"
#include "gribjump/Types.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

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

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
