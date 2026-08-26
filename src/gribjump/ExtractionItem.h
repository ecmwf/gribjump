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

#pragma once

#include <bitset>
#include <limits>
#include "eckit/filesystem/URI.h"
#include "gribjump/ExtractionData.h"
#include "metkit/mars/MarsRequest.h"

#include "gribjump/ExtractionData.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/Types.h"
#include "gribjump/URIHelper.h"
namespace gribjump {

// An object for grouping request, uri and result information together.
/// @todo: Recently reworked. Code which uses this object could be refactored to have less moving of vectors to and from
/// this object.
///
/// The v4 streaming reply is keyed by the client's request-vector position. The server scrambles request order
/// (canonicalise -> std::map by canonical string -> group by file -> complete in task order), and a completed item
/// only knows its request string. Rather than rebuild a canonical-string -> index map and hash per result, the index
/// is stamped here at build time (Engine::buildRequestMap) and read directly by the streaming harvest loop.
class ExtractionItem {

public:

    // Prefer this constructor, which takes an ExtractionRequest directly
    ExtractionItem(std::unique_ptr<ExtractionRequest> request) :
        request_(std::move(request)), result_{std::make_unique<ExtractionResult>()} {}
    // Because sometimes we dont use marsrequests.
    ExtractionItem(const Ranges& ranges) :
        request_{std::make_unique<ExtractionRequest>("", ranges)}, result_{std::make_unique<ExtractionResult>()} {}

    ExtractionItem(const ExtractionItem&)            = delete;
    ExtractionItem& operator=(const ExtractionItem&) = delete;
    ExtractionItem(ExtractionItem&&)                 = default;
    ExtractionItem& operator=(ExtractionItem&&)      = default;

    ~ExtractionItem() {};

    // Getters
    const eckit::URI& URI() const { return uri_; }

    const ExValues& values() {
        ASSERT(result_);
        return result_->values();
    }

    const ExMask& mask() const {
        ASSERT(result_);
        return result_->mask();
    }
    const Ranges& intervals() const { return request_->ranges(); }
    const std::string& request() const { return request_->requestString(); }
    const std::string& gridHash() const { return request_->gridHash(); }

    /// The index that keys this item's v4 streaming reply chunk, stamped at
    /// build/decode time. On the client extraction path it is the client's
    /// original request-vector position (see Engine::buildRequestMap); on the
    /// forwarded (leaf) path it is the shared filemap enumeration index (see
    /// Protocol::decodeForwardExtractRequest and ForwardExtractIndex.h's
    /// flattenFilemap, which the proxy uses to slot chunks back by index).
    size_t streamIndex() const {
        ASSERT(streamIndex_ != std::numeric_limits<size_t>::max());
        return streamIndex_;
    }
    void streamIndex(size_t index) { streamIndex_ = index; }

    std::unique_ptr<ExtractionResult> result() { return std::move(result_); }

    /// Byte size of the held result, used for the streaming byte budget.
    size_t resultBytes() const { return result_ ? result_->nbytes() : 0; }

    /// @note alternatively we could store the offset directly instead of the uri.
    eckit::Offset offset() const {
        std::string fragment = uri_.fragment();
        eckit::Offset offset;

        try {
            offset = std::stoll(fragment);
        }
        catch (std::invalid_argument& e) {
            throw eckit::BadValue("Invalid offset: '" + fragment + "' in URI: " + uri_.asString(), Here());
        }

        return offset;
    }

    // Setters
    void URI(const eckit::URI& uri) { uri_ = uri; }
    void request(std::unique_ptr<ExtractionRequest> request) { request_ = std::move(request); }
    void result(std::unique_ptr<ExtractionResult> result) { result_ = std::move(result); }

    bool isRemote() const { return URIHelper::isRemote(uri_); }

    void debug_print() const {
        std::cout << "ExtractionItem: {" << std::endl;
        std::cout << *request_ << std::endl;
        if (result_)
            std::cout << *result_ << std::endl;
        std::cout << "}" << std::endl;
    }

private:

    std::unique_ptr<ExtractionRequest> request_;

    // Set on Listing
    eckit::URI uri_;

    // Set on Extraction
    std::unique_ptr<ExtractionResult> result_;

    // Set at build time (client path, Engine::buildRequestMap) or decode time
    // (forwarded path, Protocol::decodeForwardExtractRequest).
    size_t streamIndex_ = std::numeric_limits<size_t>::max();
};

// ------------------------------------------------------------------

}  // namespace gribjump
