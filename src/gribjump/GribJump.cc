/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley
/// @author Tiago Quintino

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/log/Timer.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/GribJump.h"
#include <cstddef>
#include "gribjump/GribJumpBase.h"
#include "gribjump/GribJumpFactory.h"
#include "gribjump/Metrics.h"
#include "gribjump/Types.h"
#include "gribjump/api/ExtractionIterator.h"
#include "gribjump/api/ListResult.h"
#include "gribjump/api/ResultIterator.h"
#include "gribjump/tools/ToolUtils.h"
#include "metkit/mars/MarsRequest.h"

namespace gribjump {

GribJump::GribJump() {
    ConfigOptions& cfg = ConfigOptions::instance();

    // Deduplicate backends by transport type, so capabilities sharing a transport reuse
    // the same backend object (and its resources), while differing ones get distinct objects.
    std::map<std::string, std::shared_ptr<GribJumpBase>> cache;
    auto backend = [&](const std::string& capability) -> std::shared_ptr<GribJumpBase> {
        const std::string type = cfg.capabilityType(capability);
        auto it                = cache.find(type);
        if (it == cache.end()) {
            std::shared_ptr<GribJumpBase> b(GribJumpFactory::build(type));
            it = cache.emplace(type, b).first;
            backends_.push_back(b);
        }
        return it->second;
    };

    scanner_      = backend("scan");
    extractor_    = backend("extract");
    axesProvider_ = backend("axes");
    lister_       = backend("list");
}

GribJump::~GribJump() {}

size_t GribJump::scan(const std::vector<eckit::PathName>& paths, const LogContext& ctx) {
    ContextManager::instance().set(ctx);

    if (paths.empty()) {
        throw eckit::UserError("Paths must not be empty", Here());
    }

    size_t ret = scanner_->scan(paths);
    return ret;
}

size_t GribJump::scan(const std::vector<metkit::mars::MarsRequest> requests, bool byfiles, const LogContext& ctx) {
    ContextManager::instance().set(ctx);

    if (requests.empty()) {
        throw eckit::UserError("Requests must not be empty", Here());
    }

    size_t ret = scanner_->scan(requests, byfiles);
    return ret;
}

/// @todo: we ought to be asserting that the requests are of cardinality 1, though currently awkward as they are
/// represented with strings not MarsRequest (for efficiency)
///        Perhaps we can do this check deeper in the code, when it is explicitly required.
/// @note: Future note: we may switch VectorResultSource to a queue or something similar for better streaming support, but
/// ideally the API should not change.
ExtractionIterator GribJump::extract(std::vector<ExtractionRequest>& requests, const LogContext& ctx) {
    ContextManager::instance().set(ctx);

    if (requests.empty()) {
        throw eckit::UserError("Requests must not be empty", Here());
    }
    return ExtractionIterator{std::make_unique<VectorResultSource<ExtractionResult>>(extractor_->extract(requests))};
}

ExtractionIterator GribJump::extract(std::vector<PathExtractionRequest>& requests, const LogContext& ctx) {
    ContextManager::instance().set(ctx);

    if (requests.empty()) {
        throw eckit::UserError("Requests must not be empty", Here());
    }
    return ExtractionIterator{std::make_unique<VectorResultSource<ExtractionResult>>(extractor_->extract(requests))};
}

ExtractionIterator GribJump::extract(const metkit::mars::MarsRequest& request, const std::vector<Range>& ranges,
                                     const std::string& gridHash, const LogContext& ctx) {
    // Expand the request into multiple extraction requests (one per field)

    std::vector<metkit::mars::MarsRequest> marsrequests = flattenRequest(request);

    std::vector<ExtractionRequest> requests;
    for (auto& req : marsrequests) {
        requests.push_back(ExtractionRequest(req.asString(), ranges, gridHash));
    }

    return extract(requests, ctx);
}

ExtractionIterator GribJump::extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets,
                                     const std::vector<std::vector<Range>>& ranges, const LogContext& ctx) {
    ContextManager::instance().set(ctx);

    if (path.asString().empty()) {
        throw eckit::UserError("Path must not be empty", Here());
    }
    if (offsets.empty()) {
        throw eckit::UserError("Offsets must not be empty", Here());
    }
    if (offsets.size() != ranges.size()) {
        throw eckit::UserError("Offsets and ranges must be the same size", Here());
    }

    return ExtractionIterator{std::make_unique<VectorResultSource<ExtractionResult>>(extractor_->extract(path, offsets, ranges))};
}


ListIterator GribJump::list(const ListRequest& request, const LogContext& ctx) {
    ContextManager::instance().set(ctx);
    return lister_->list(request);
}


std::map<std::string, std::unordered_set<std::string>> GribJump::axes(const std::string& request, int level,
                                                                      const LogContext& ctx) {
    ContextManager::instance().set(ctx);

    if (request.empty()) {
        throw eckit::UserError("Request string must not be empty", Here());
    }

    auto out = axesProvider_->axes(request, level);
    return out;
}

void GribJump::stats() {
    for (auto& backend : backends_) {
        backend->stats();
    }
}

}  // namespace gribjump
