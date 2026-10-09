/*
 * (C) Copyright 2026- ECMWF.
 * This software is licensed under the terms of the Apache Licence Version 2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <algorithm>
#include <map>
#include <string>
#include <utility>

#include "eckit/filesystem/URI.h"
#include "eckit/io/Length.h"
#include "gribjump/ExtractionData.h"
#include "gribjump/URIHelper.h"
#include "metkit/mars/MarsRequest.h"

namespace gribjump {

/// One discovered field. Owns the complete backend URI and metadata; no GRIB
/// values are read. Metadata is returned as supplied by the catalogue.
class ListResult {
public:

    using Metadata = std::map<std::string, std::string>;

    ListResult(eckit::URI uri, Metadata metadata, eckit::Length length) :
        uri_(std::move(uri)), metadata_(std::move(metadata)), length_(length) {}

    /// Use asRawString() to serialise all components. eckit's asString() is
    /// scheme-specific and may omit the scheme, host, query or fragment.
    const eckit::URI& uri() const { return uri_; }
    const Metadata& metadata() const { return metadata_; }
    metkit::mars::MarsRequest marsRequest() const { return metkit::mars::MarsRequest("retrieve", metadata_); }
    eckit::Offset offset() const { return URIHelper::offset(uri_); }
    eckit::Length length() const { return length_; }

    PathExtractionRequest extractionRequest(const Ranges& ranges, const std::string& gridHash = "") const {
        return PathExtractionRequest(uri_.path(), uri_.scheme(), static_cast<long long>(offset()), uri_.host(),
                                     std::max(0, uri_.port()), ranges, gridHash);
    }

private:

    eckit::URI uri_;
    Metadata metadata_;
    eckit::Length length_;
};

}  // namespace gribjump
