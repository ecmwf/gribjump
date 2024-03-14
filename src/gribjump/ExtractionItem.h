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

#include "eckit/filesystem/URI.h"
#include "metkit/mars/MarsRequest.h"

#include "gribjump/LibGribJump.h"

namespace gribjump {

typedef std::vector<std::pair<size_t, size_t>> Ranges;

typedef std::vector<std::vector<double>> ExValues; // assumes 1 to 1 map between request and result
typedef std::vector<std::vector<std::bitset<64>>> ExMask;

// An object for grouping request, uri and result information together.
// Note, this is a one to one mapping between request and result.
// i.e. the request is assumed to be of cardinality 1. /// No it isn't! It's the base request

class ExtractionItem {

public:

    ExtractionItem(const metkit::mars::MarsRequest& baseRequest, const Ranges& ranges): 
        request_(baseRequest), ranges_(ranges) {
            /// @note We could reserve the values and mask here based on the ranges
            /// @todo is there a cheap way to assert cardinality 1?

        LOG_DEBUG_LIB(LibGribJump) << "ExtractionItem: " << request_ << std::endl;
    }

    ~ExtractionItem() {
        delete uri_;
        delete values_;
        delete mask_;
    }
    
    const eckit::URI& uri() const { return *uri_; }
    const ExValues& values() const { return *values_; }
    const ExMask& mask() const { return *mask_; }
    const Ranges& intervals() const { return ranges_; }
    const metkit::mars::MarsRequest& request() const { return request_; }
    
    /// @note alternatively we could store the offset directly instead of the uri.
    eckit::Offset offset() const {
        ASSERT(uri_);
        
        std::string fragment =  uri_->fragment();
        eckit::Offset offset;
        
        try {
            offset = std::stoll(fragment);
        } catch (std::invalid_argument& e) {
            throw eckit::BadValue("Invalid offset: '" + fragment + "' in URI: " + uri_->asString(), Here());
        }

        return offset;
    }

    void URI(eckit::URI* uri) { uri_ = uri; }
    // void values(Values* values) { values_ = values; }
    // void mask(Mask* mask) { mask_ = mask; }

    /// @todo, dont do this. only for temp compatability 
    // Make a copy of the input values / mask
    void values(const ExValues& values) { values_ = new ExValues(values); }
    void mask(const ExMask& mask) { mask_ = new ExMask(mask); }

    void debug_print() const {
        std::cout << "ExtractionItem: {" << std::endl;
        std::cout << "  MarsRequest: " << request_ << std::endl;
        std::cout << "  Ranges: " << std::endl;
        for (auto& r : ranges_) {
            std::cout << "   {" << r.first << ", " << r.second << "}" << std::endl;
        }
        if (uri_) std::cout << "  URI: " << uri_->asString() << ", fragment: " << uri_->fragment() << std::endl;
        if (values_){
            std::cout << "  Values: " << std::endl;
            for (auto& v : *values_) {
                std::cout << "   {";
                for (auto& d : v) {
                    std::cout << d << ", ";
                }
                std::cout << "}" << std::endl;
            }
        }
        if (mask_){
            std::cout << "  Mask: " << std::endl;
            for (auto& v : *mask_) {
                std::cout << "   {";
                for (auto& d : v) {
                    std::cout << d << ", ";
                }
                std::cout << "}" << std::endl;
            }
        }
        std::cout << "}" << std::endl;
    }

private:

    const metkit::mars::MarsRequest& request_;
    const Ranges& ranges_;

    // Set on Listing
    eckit::URI* uri_;

    // Set on Extraction
    ExValues * values_;
    ExMask* mask_;
};

// ------------------------------------------------------------------

} // namespace gribjump
