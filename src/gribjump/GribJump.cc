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

#include "eckit/log/Timer.h"

#include "gribjump/GribJump.h"
#include "gribjump/GribJumpBase.h"
#include "gribjump/GribJumpFactory.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/api/ExtractionIterator.h"

namespace gribjump {

GribJump::GribJump() {
    impl_ = std::unique_ptr<GribJumpBase>(GribJumpFactory::build(LibGribJump::instance().config()));
}

GribJump::~GribJump() {}

size_t GribJump::scan(const std::vector<eckit::PathName>& paths, const LogContext& ctx) {
    ContextManager::instance().set(ctx);

    if (paths.empty()) {
        throw eckit::UserError("Paths must not be empty", Here());
    }

    size_t ret = impl_->scan(paths);
    return ret;
}

size_t GribJump::scan(const std::vector<metkit::mars::MarsRequest> requests, bool byfiles, const LogContext& ctx) {
    ContextManager::instance().set(ctx);

    if (requests.empty()) {
        throw eckit::UserError("Requests must not be empty", Here());
    }

    size_t ret = impl_->scan(requests, byfiles);
    return ret;
}


std::vector<std::vector<std::unique_ptr<ExtractionResult>>> GribJump::extract_old(
    std::vector<ExtractionRequest>& requests, const LogContext& ctx) {
    ContextManager::instance().set(ctx);

    if (requests.empty()) {
        throw eckit::UserError("Requests must not be empty", Here());
    }

    // Wrap around new method
    ExtractionIterator it = extract_new(requests, ctx);
    std::vector<std::vector<std::unique_ptr<ExtractionResult>>> out_old;

    while (it.hasNext()) {
        out_old.push_back(std::vector<std::unique_ptr<ExtractionResult>>());
        out_old.back().push_back(it.next());
    }

    return out_old;
}

/// @todo: this will replace the old extract method
/// @todo: we ought to be asserting that the requests are of cardinality 1, though currently awkward as they are
/// represented with strings not MarsRequest (for efficiency)
///        Perhaps we can do this check deeper in the code, when it is explicitly required.
/// @note: Future note: we may switch VectorSource to a queue or something similar for better streaming support, but
/// ideally the API should not change.
ExtractionIterator GribJump::extract_new(std::vector<ExtractionRequest>& requests, const LogContext& ctx) {
    ContextManager::instance().set(ctx);

    if (requests.empty()) {
        throw eckit::UserError("Requests must not be empty", Here());
    }
    return ExtractionIterator{std::make_unique<VectorSource>(VectorSource{impl_->extract(requests)})};
}

std::vector<std::unique_ptr<ExtractionItem>> GribJump::extract(const eckit::PathName& path,
                                                               const std::vector<eckit::Offset>& offsets,
                                                               const std::vector<std::vector<Range>>& ranges,
                                                               const LogContext& ctx) {
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

    auto out = impl_->extract(path, offsets, ranges);
    return out;
}

ExtractionIterator GribJump::extract_new(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets,
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
    NOTIMP;  /// @todo the new way.
    // return ExtractionIterator{std::make_unique<VectorSource>(VectorSource{impl_->extract(path, offsets, ranges)})};;
}


std::map<std::string, std::unordered_set<std::string>> GribJump::axes(const std::string& request, int level,
                                                                      const LogContext& ctx) {
    ContextManager::instance().set(ctx);

    if (request.empty()) {
        throw eckit::UserError("Request string must not be empty", Here());
    }

    auto out = impl_->axes(request, level);
    return out;
}

void GribJump::stats() {
    impl_->stats();
}

}  // namespace gribjump
