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

#pragma once

#include <bitset>
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
class ExtractionItem : public eckit::NonCopyable {

public:

    // Prefer this constructor, which takes an ExtractionRequest directly
    ExtractionItem(std::unique_ptr<ExtractionRequest> request) :
        request_(std::move(request)), result_{std::make_unique<ExtractionResult>()} {}
    // Because sometimes we dont use marsrequests.
    ExtractionItem(const Ranges& ranges) :
        request_{std::make_unique<ExtractionRequest>("", ranges)}, result_{std::make_unique<ExtractionResult>()} {}

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

    std::unique_ptr<ExtractionResult> result() { return std::move(result_); }

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
};

// ------------------------------------------------------------------

}  // namespace gribjump
