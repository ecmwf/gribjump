/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <memory>
#include <vector>
#include <map>
#include <bitset>

namespace gribjump {

class ExtractionItem;
class ExtractionRequest;

using MarsRequests = std::vector<metkit::mars::MarsRequest>;
using Range = std::pair<size_t, size_t>;
using Interval = std::pair<size_t, size_t>;
using RangesList = std::vector<std::vector<Range>>;
using GridHashes = std::vector<std::string>;
using ExtractionRequests = std::vector<ExtractionRequest>;
using Bitmap = std::vector<bool>;

using Ranges = std::vector<Range>;
using ExValues = std::vector<std::vector<double>>;
using ExMask = std::vector<std::vector<std::bitset<64>>>;

using ExtractionItems = std::vector<ExtractionItem*>; // Non-owning pointers
using ExItemMap = std::map<std::string, std::unique_ptr<ExtractionItem>>;

// filemap holds non-owning pointers to ExtractionItems
using filemap_t = std::map<std::string, ExtractionItems>;

} // namespace gribjump
