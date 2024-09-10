// types.h
#pragma once

namespace gribjump {

class ExtractionItem;

using MarsRequests = std::vector<metkit::mars::MarsRequest>;
using Range = std::pair<size_t, size_t>;
using Interval = std::pair<size_t, size_t>;
using RangesList = std::vector<std::vector<Range>>;
using Bitmap = std::vector<bool>;

using Ranges = std::vector<Range>;
using ExValues = std::vector<std::vector<double>>;
using ExMask = std::vector<std::vector<std::bitset<64>>>;

using ExtractionItems = std::vector<ExtractionItem*>; // Non-owning pointers
using ExItemMap = std::map<std::string, std::unique_ptr<ExtractionItem>>;

} // namespace gribjump
