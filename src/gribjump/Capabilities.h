/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley

/// Capability (role) interfaces for GribJump.
///
/// The high-level GribJump facade is composed of these narrow interfaces, each of
/// which may be backed independently by a local or remote implementation (e.g. local
/// listing with remote extraction). Concrete backends such as LocalGribJump and
/// RemoteGribJump implement several of these roles at once (see GribJumpBase), but the
/// facade only ever depends on the roles, not the concrete types.

#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "eckit/filesystem/PathName.h"
#include "eckit/io/Offset.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/Types.h"
#include "gribjump/api/ListRequest.h"
#include "gribjump/api/ListResult.h"

namespace gribjump {

// Scans grib files, populating the index/cache.
class Scanner {
public:

    virtual ~Scanner() = default;

    virtual size_t scan(const std::vector<eckit::PathName>& paths) = 0;

    virtual size_t scan(const std::vector<metkit::mars::MarsRequest>& requests, bool byfiles) = 0;
};

// Extracts values (and masks) for ranges of fields.
class Extractor {
public:

    virtual ~Extractor() = default;

    virtual std::vector<std::unique_ptr<ExtractionResult>> extract(std::vector<ExtractionRequest>& requests) = 0;

    virtual std::vector<std::unique_ptr<ExtractionResult>> extract(PathExtractionRequests& requests) = 0;

    virtual std::vector<std::unique_ptr<ExtractionResult>> extract(const eckit::PathName& path,
                                                                   const std::vector<eckit::Offset>& offsets,
                                                                   const std::vector<std::vector<Range>>& ranges) = 0;
};

// Reports the available values of each axis for a request.
class AxesProvider {
public:

    virtual ~AxesProvider() = default;

    virtual std::map<std::string, std::unordered_set<std::string>> axes(const std::string& request, int level) = 0;
};

// Lists the fields matching a request.
class Lister {
public:

    virtual ~Lister() = default;

    virtual ListIterator list(const ListRequest& request) = 0;
};

}  // namespace gribjump
