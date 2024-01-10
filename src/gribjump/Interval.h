#pragma once

#include <cstddef>
#include <vector>

// NOTE: Ranges are treated as half-open intervals [start, end)
using Interval = std::pair<size_t, size_t>;
