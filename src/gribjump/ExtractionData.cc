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

#include "gribjump/ExtractionData.h"
#include "eckit/value/Value.h"

namespace gribjump {

ExtractionResult::ExtractionResult(std::vector<std::vector<double>> values, std::vector<std::vector<std::bitset<64>>> mask): 
    values_(std::move(values)),
    mask_(std::move(mask))
    {}

ExtractionResult::ExtractionResult(eckit::Stream& s){
    s >> values_;
    std::vector<std::vector<std::string>> bitsetStrings;
    s >> bitsetStrings;
    for (auto& v : bitsetStrings) {
        std::vector<std::bitset<64>> bitset;
        for (auto& b : v) {
            bitset.push_back(std::bitset<64>(b));
        }
        mask_.push_back(bitset);
    }
}

void ExtractionResult::values_ptr(double*** values, unsigned long* nrange, unsigned long** nvalues) {
    *nrange = values_.size();
    *values = new double*[*nrange];
    *nvalues = new unsigned long[*nrange];
    for (size_t i = 0; i < *nrange; i++) {
        (*nvalues)[i] = values_[i].size();
        (*values)[i] = values_[i].data();
    }
}

void ExtractionResult::encode(eckit::Stream& s) const {
    s << values_;
    std::vector<std::vector<std::string>> bitsetStrings;
    for (auto& v : mask_) {
        std::vector<std::string> bitsetString;
        for (auto& b : v) {
            bitsetString.push_back(b.to_string());
        }
        bitsetStrings.push_back(bitsetString);
    }
    s << bitsetStrings;
}

void ExtractionResult::print(std::ostream& s) const {
    s << "ExtractionResult[Values:[";
    for (auto& v : values_) {
        s << v << ", ";
    }
    s << "]; Masks:[";
    for (auto& v : mask_) {
        s << "[";
        for (auto& b : v) {
            s << std::hex << b.to_ullong() << std::dec << ", ";
        }
        s << "], ";
    }
    s << "]" << std::endl;
}

std::ostream& operator<<(std::ostream& s, const ExtractionResult& o) {
    o.print(s);
    return s;
}

eckit::Stream& operator<<(eckit::Stream& s, const ExtractionResult& o) {
    o.encode(s);
    return s;
}

// ---------------------------------------------------------------------------------------------

ExtractionRequest::ExtractionRequest(metkit::mars::MarsRequest request, std::vector<Range> ranges):
    ranges_(std::move(ranges)),
    request_(std::move(request))
    {}

ExtractionRequest::ExtractionRequest(eckit::Stream& s){
    request_ = metkit::mars::MarsRequest(s);
    size_t numRanges;
    s >> numRanges;
    for (size_t j = 0; j < numRanges; j++) {
        size_t start, end;
        s >> start >> end;
        ranges_.push_back(std::make_pair(start, end));
    }
}

eckit::Stream& operator<<(eckit::Stream& s, const ExtractionRequest& o) {
    o.encode(s);
    return s;
}

void ExtractionRequest::encode(eckit::Stream& s) const {
    s << request_;
    s << ranges_.size();
    for (auto& [start, end] : ranges_) {
        s << start << end;
    }
}

void ExtractionRequest::print(std::ostream& s) const {
    s << "ExtractionRequest[Request: " << request_ << "; Ranges: ";
    for (auto& [start, end] : ranges_) {
        s << "(" << start << ", " << end << "), ";
    }
    s << "]" << std::endl;
}

std::ostream& operator<<(std::ostream& s, const ExtractionRequest& o) {
    o.print(s);
    return s;
}

} // namespace gribjump
