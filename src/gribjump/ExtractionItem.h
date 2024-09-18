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
#include "metkit/mars/MarsRequest.h"

#include "gribjump/LibGribJump.h"
#include "gribjump/Types.h"
namespace gribjump {


// An object for grouping request, uri and result information together.
class ExtractionItem : public eckit::NonCopyable {

public:

    ExtractionItem(const metkit::mars::MarsRequest& baseRequest, const Ranges& ranges): 
        request_(baseRequest), ranges_(ranges) {
            /// @note We could reserve the values and mask here based on the ranges
            /// @note We're not always going to have mars requests (e.g. file name, tree, ...) More generic object?
    }

    ExtractionItem(const Ranges& ranges) : request_(""), ranges_(ranges) {} // Because sometimes we dont use marsrequests.

    ~ExtractionItem() {};
    
    const eckit::URI& URI() const { return uri_; }
    // const ExValues& values() const { return values_; }
    ExValues& values() { return values_; }
    const ExMask& mask() const { return mask_; }
    const Ranges& intervals() const { return ranges_; }
    const metkit::mars::MarsRequest& request() const { return request_; }
    
    /// @note alternatively we could store the offset directly instead of the uri.
    eckit::Offset offset() const {
        
        std::string fragment =  uri_.fragment();
        eckit::Offset offset;
        
        try {
            offset = std::stoll(fragment);
        } catch (std::invalid_argument& e) {
            throw eckit::BadValue("Invalid offset: '" + fragment + "' in URI: " + uri_.asString(), Here());
        }

        return offset;
    }

    void gridHash(const std::string& hash) { gridHash_ = hash; }
    const std::string& gridHash() const { return gridHash_; }

    void URI(const eckit::URI& uri) { uri_ = uri; }

    void values(ExValues values) { values_ = std::move(values); }
    void mask(ExMask mask) { mask_ = std::move(mask); }

    void debug_print() const {
        std::cout << "ExtractionItem: {" << std::endl;
        std::cout << "  MarsRequest: " << request_ << std::endl;
        std::cout << "  Ranges: " << std::endl;
        for (auto& r : ranges_) {
            std::cout << "   {" << r.first << ", " << r.second << "}" << std::endl;
        }

        std::cout << "  Values: {" << std::endl;
        for (auto& v : values_) {
            std::cout << "   {";
            for (auto& d : v) {
                std::cout << d << ", ";
            }
            std::cout << "}" << std::endl;
        }
        std::cout << "}" << std::endl;

        std::cout << "  Mask: {" << std::endl;
        for (auto& v : mask_) {
            std::cout << "   {";
            for (auto& d : v) {
                std::cout << d << ", ";
            }
            std::cout << "}" << std::endl;
        }
        std::cout << "}" << std::endl;
        
        std::cout << "}" << std::endl;
    }

private:

    const metkit::mars::MarsRequest request_;
    const Ranges& ranges_;

    // Set on Listing
    eckit::URI uri_;

    // Set on Extraction
    ExValues values_;
    ExMask mask_;

    // Optional extras
    std::string gridHash_=""; //< if supplied, check hash matches the jumpinfo
};

// ------------------------------------------------------------------

} // namespace gribjump
