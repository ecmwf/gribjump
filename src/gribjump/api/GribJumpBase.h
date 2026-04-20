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

/// @file GribJumpBase.h
/// @brief Abstract backend interface for GribJump implementations.

#pragma once

#include <stddef.h>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "eckit/filesystem/PathName.h"
#include "gribjump/api/ExtractionData.h"
#include "gribjump/api/Types.h"
#include "gribjump/config/Config.h"
#include "gribjump/config/Stats.h"

namespace eckit {
class Offset;
}
namespace metkit {
namespace mars {
class MarsRequest;
}
}  // namespace metkit

namespace gribjump {

/// @brief Abstract interface implemented by GribJump backends.
///
/// Typical concrete implementations include LocalGribJump (direct local
/// extraction) and RemoteGribJump (delegating extraction to a GribJump server).
///
/// Instances are non-copyable and non-movable to preserve backend state and
/// resource ownership semantics.
class GribJumpBase {
public:

    /// @brief Construct backend interface with explicit configuration.
    /// @param config Runtime configuration for backend setup.
    GribJumpBase(const Config& config);

    /// @brief Construct backend interface using default configuration lookup.
    GribJumpBase();

    GribJumpBase(const GribJumpBase&)            = delete;
    GribJumpBase& operator=(const GribJumpBase&) = delete;
    GribJumpBase(GribJumpBase&&)                 = delete;
    GribJumpBase& operator=(GribJumpBase&&)      = delete;

    /// @brief Virtual destructor for polymorphic backend deletion.
    virtual ~GribJumpBase();

    /// @brief Scan GRIB files at the provided paths and build jump-info indexes.
    /// @param paths GRIB file paths to scan.
    /// @return Number of fields scanned.
    /// @throws eckit::Exception If scanning fails.
    size_t virtual scan(const std::vector<eckit::PathName>& paths) = 0;

    /// @brief Scan fields that match the provided MARS requests.
    /// @param requests MARS requests used to identify fields to scan.
    /// @param byfiles If true, groups scanning by file; otherwise processes by request.
    /// @return Number of fields scanned.
    /// @throws eckit::Exception If scanning fails.
    virtual size_t scan(const std::vector<metkit::mars::MarsRequest>& requests, bool byfiles) = 0;

    /// @brief Extract subsets from fields matching extraction requests.
    /// @param requests Extraction requests to execute.
    /// @return Vector of owned extraction results, one result per request.
    ///
    /// Ownership semantics: each std::unique_ptr<ExtractionResult> in the returned
    /// vector is transferred to the caller.
    ///
    /// @throws eckit::Exception If extraction fails.
    virtual std::vector<std::unique_ptr<ExtractionResult>> extract(std::vector<ExtractionRequest>&) = 0;

    /// @brief Extract subsets from path-based extraction requests.
    /// @param requests Path extraction requests to execute.
    /// @return Vector of owned extraction results, one result per request.
    ///
    /// Ownership semantics: each std::unique_ptr<ExtractionResult> in the returned
    /// vector is transferred to the caller.
    ///
    /// @throws eckit::Exception If extraction fails.
    virtual std::vector<std::unique_ptr<ExtractionResult>> extract(PathExtractionRequests& requests) = 0;

    /// @brief Extract from specific GRIB messages in a single file.
    /// @param path File path containing GRIB messages.
    /// @param offsets Message offsets within the file.
    /// @param ranges Per-message half-open index ranges to extract.
    /// @return Vector of owned extraction results.
    ///
    /// Ownership semantics: each std::unique_ptr<ExtractionResult> in the returned
    /// vector is transferred to the caller.
    ///
    /// @throws eckit::Exception If extraction fails.
    virtual std::vector<std::unique_ptr<ExtractionResult>> extract(const eckit::PathName& path,
                                                                   const std::vector<eckit::Offset>& offsets,
                                                                   const std::vector<std::vector<Range>>& ranges) = 0;

    /// @brief Query available axes (parameter dimensions) for a request.
    /// @param request MARS request string used as query filter.
    /// @param level Axis enumeration depth.
    /// @return Map from axis name to set of discovered values.
    /// @throws eckit::Exception If axis discovery fails.
    virtual std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request, int level) = 0;

    /// @brief Emit backend extraction statistics to the configured log.
    virtual void stats();

protected:  // members

    Stats stats_;
};

}  // namespace gribjump
