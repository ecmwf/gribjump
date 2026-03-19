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

/// @file ExtractionItem.h
/// @brief Internal container that groups request metadata, URI location, and extraction result.

#pragma once

#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#include "eckit/io/Offset.h"
#include "eckit/log/CodeLocation.h"
#include "gribjump/api/ExtractionData.h"
#include "gribjump/api/Types.h"
#include "gribjump/core/URIHelper.h"

namespace gribjump {

/// @brief Internal container that groups request, URI, and result state.
///
/// This object is used in internal extraction pipelines to keep together the
/// request definition, resolved data location, and the extracted payload.
class ExtractionItem {

public:

    /// @brief Construct from an owned extraction request.
    /// @param request Owned request transferred into this item.
    ///
    /// Ownership semantics: @p request is consumed (moved) and must not be used
    /// by the caller afterwards.
    ExtractionItem(std::unique_ptr<ExtractionRequest> request) :
        request_(std::move(request)), result_{std::make_unique<ExtractionResult>()} {}

    /// @brief Construct from ranges only, with an empty request string.
    /// @param ranges Extraction intervals used to create an internal request.
    ExtractionItem(const Ranges& ranges) :
        request_{std::make_unique<ExtractionRequest>("", ranges)}, result_{std::make_unique<ExtractionResult>()} {}

    ExtractionItem(const ExtractionItem&)            = delete;
    ExtractionItem& operator=(const ExtractionItem&) = delete;
    ExtractionItem(ExtractionItem&&)                 = default;
    ExtractionItem& operator=(ExtractionItem&&)      = default;

    /// @brief Destroy this extraction item.
    ~ExtractionItem() {};

    /// @brief Access the resolved URI.
    /// @return URI of the target data location.
    const eckit::URI& URI() const { return uri_; }

    /// @brief Access extracted values.
    /// @return Nested value vectors grouped by range.
    const ExValues& values() {
        ASSERT(result_);
        return result_->values();
    }

    /// @brief Access extracted missing-value masks.
    /// @return Nested mask vectors grouped by range.
    const ExMask& mask() const {
        ASSERT(result_);
        return result_->mask();
    }

    /// @brief Access extraction intervals from the associated request.
    /// @return Range list from the request.
    const Ranges& intervals() const { return request_->ranges(); }

    /// @brief Access the request string.
    /// @return MARS request string.
    const std::string& request() const { return request_->requestString(); }

    /// @brief Access optional request grid hash.
    /// @return Grid hash string.
    const std::string& gridHash() const { return request_->gridHash(); }

    /// @brief Transfer ownership of the stored extraction result.
    /// @return Owned result pointer.
    ///
    /// Ownership semantics: the returned std::unique_ptr is moved out of this
    /// object and becomes owned by the caller.
    std::unique_ptr<ExtractionResult> result() { return std::move(result_); }

    /// @brief Parse and return byte offset encoded in the URI fragment.
    /// @return Message byte offset.
    /// @throws eckit::BadValue If URI fragment is not a valid integer offset.
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

    /// @brief Set the resolved URI.
    /// @param uri URI value.
    void URI(const eckit::URI& uri) { uri_ = uri; }

    /// @brief Replace the stored extraction request.
    /// @param request Owned request pointer.
    ///
    /// Ownership semantics: @p request is consumed (moved) into this object.
    void request(std::unique_ptr<ExtractionRequest> request) { request_ = std::move(request); }

    /// @brief Replace the stored extraction result.
    /// @param result Owned result pointer.
    ///
    /// Ownership semantics: @p result is consumed (moved) into this object.
    void result(std::unique_ptr<ExtractionResult> result) { result_ = std::move(result); }

    /// @brief Check whether the URI points to a remote resource.
    /// @return True if URI is remote, false otherwise.
    bool isRemote() const { return URIHelper::isRemote(uri_); }

    /// @brief Print this item for debugging purposes.
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
