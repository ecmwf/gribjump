/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley
/// @author Tiago Quintino

/// @file GribJump.h
/// @brief Public C++ facade API for GribJump extraction workflows.

#pragma once

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "eckit/filesystem/PathName.h"
#include "gribjump/api/ExtractionIterator.h"
#include "gribjump/config/Metrics.h"

namespace eckit {
class Offset;
}
namespace metkit {
namespace mars {
class MarsRequest;
}
}  // namespace metkit
namespace gribjump {
class ExtractionRequest;
class PathExtractionRequest;
}  // namespace gribjump

namespace gribjump {

class GribJumpBase;

/// @brief Half-open interval of element indices within a decoded GRIB field.
///
/// The interval is interpreted as [start, end), where @c start is inclusive and
/// @c end is exclusive.
typedef std::pair<size_t, size_t> Range;

/// @brief Main entry point for the GribJump C++ API.
///
/// This class provides a high-level facade over a polymorphic backend
/// implementation selected via configuration. Depending on runtime setup, the
/// backend is typically either local extraction (direct data access) or remote
/// extraction (delegating requests to a GribJump server).
class GribJump {
public:

    /// @brief Construct a GribJump facade with backend selected from config.
    GribJump();

    /// @brief Destroy the GribJump facade and its backend implementation.
    ~GribJump();

    /// @brief Scan GRIB files at the provided paths and build jump-info indexes.
    /// @param paths GRIB file paths to scan.
    /// @param ctx Optional logging context.
    /// @return Number of fields scanned.
    /// @throws eckit::Exception If scanning fails.
    size_t scan(const std::vector<eckit::PathName>& paths, const LogContext& ctx = LogContext());

    /// @brief Scan fields that match the provided MARS requests.
    /// @param requests MARS requests used to identify fields to scan.
    /// @param byfiles If true, groups scanning by file; otherwise processes by request.
    /// @param ctx Optional logging context.
    /// @return Number of fields scanned.
    /// @throws eckit::Exception If scanning fails.
    size_t scan(std::vector<metkit::mars::MarsRequest> requests, bool byfiles = false,
                const LogContext& ctx = LogContext());

    /// @brief Extract subsets from fields matching MARS-style extraction requests.
    ///
    /// Each ExtractionRequest contains a request string, one or more index ranges,
    /// and an optional grid hash.
    ///
    /// @param requests Extraction requests to execute.
    /// @param ctx Optional logging context.
    /// @return Iterator over extraction results, one result per input request.
    /// @throws eckit::Exception If extraction fails.
    ExtractionIterator extract(std::vector<ExtractionRequest>& requests, const LogContext& ctx = LogContext());

    /// @brief Extract subsets using explicit path-based extraction requests.
    ///
    /// Each PathExtractionRequest specifies location details (path/scheme/offset,
    /// and optional remote host/port) in addition to extraction ranges.
    ///
    /// @param requests Path-based extraction requests.
    /// @param ctx Optional logging context.
    /// @return Iterator over extraction results, one result per input request.
    /// @throws eckit::Exception If extraction fails.
    ExtractionIterator extract(std::vector<PathExtractionRequest>& requests, const LogContext& ctx = LogContext());

    /// @brief Extract identical ranges from all fields matching a single MARS request.
    ///
    /// This convenience overload expands the MARS request to matching fields and
    /// applies the same set of ranges to each field.
    ///
    /// @param request MARS request used to identify one or more fields.
    /// @param ranges Half-open element ranges to extract from each matching field.
    /// @param gridHash Optional grid hash for grid-consistency verification.
    /// @param ctx Optional logging context.
    /// @return Iterator over extraction results for matching fields.
    /// @throws eckit::Exception If request expansion or extraction fails.
    ExtractionIterator extract(const metkit::mars::MarsRequest& request, const std::vector<Range>& ranges,
                               const std::string& gridHash, const LogContext& ctx = LogContext());

    /// @brief Extract from specific GRIB messages in a single file.
    ///
    /// The @p offsets vector identifies GRIB message starts in @p path. The
    /// @p ranges vector provides per-message extraction ranges.
    ///
    /// @param path File path containing GRIB messages.
    /// @param offsets Message offsets within the file.
    /// @param ranges Per-message half-open index ranges to extract.
    /// @param ctx Optional logging context.
    /// @return Iterator over extraction results.
    /// @throws eckit::Exception If extraction fails.
    ExtractionIterator extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets,
                               const std::vector<std::vector<Range>>& ranges, const LogContext& ctx = LogContext());

    /// @brief Query available axes (parameter dimensions) for a MARS request.
    /// @param request MARS request string used as query filter.
    /// @param level Axis enumeration depth (default: 3).
    /// @param ctx Optional logging context.
    /// @return Map from axis name to the set of discovered values.
    /// @throws eckit::Exception If axis discovery fails.
    std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request, int level = 3,
                                                                const LogContext& ctx = LogContext());

    /// @brief Emit extraction statistics to the configured log.
    void stats();

private:

    std::unique_ptr<GribJumpBase> impl_;
};

}  // namespace gribjump
