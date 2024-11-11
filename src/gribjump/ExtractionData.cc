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
#include "eckit/io/Buffer.h"

namespace gribjump {

namespace {

void encodeVector(eckit::Stream& s, const std::vector<double>& v) {
    size_t size = v.size();
    s << size;
    eckit::Buffer buffer(v.data(), size * sizeof(double));
    s << buffer;
}

std::vector<double> decodeVector(eckit::Stream& s) {
    size_t size;
    s >> size;
    eckit::Buffer buffer(size * sizeof(double));
    s >> buffer;
    double* data = (double*) buffer.data();

    return std::vector<double>(data, data + size);
}

void encodeVector(eckit::Stream& s, const std::vector<unsigned long long>& v) {
    size_t size = v.size();
    s << size;
    eckit::Buffer buffer(v.data(), size * sizeof(unsigned long long));
    s << buffer;
}

std::vector<unsigned long long> decodeVectorUll(eckit::Stream& s) {
    size_t size;
    s >> size;
    eckit::Buffer buffer(size * sizeof(unsigned long long));
    s >> buffer;
    unsigned long long* data = (unsigned long long*) buffer.data();
    return std::vector<unsigned long long>(data, data + size);
}


void encodeVectorVector(eckit::Stream& s, const std::vector<std::vector<double>>& v) {
    size_t size = v.size();
    s << size;
    size_t totalSize = 0;
    for (auto& inner : v) {
        totalSize += inner.size();
        s << inner.size();
    }
    s << totalSize;
    eckit::Buffer buffer(totalSize * sizeof(double));
    double* data = (double*) buffer.data();
    for (auto& inner : v) {
        for (auto& d : inner) {
            *data++ = d;
        }
    }
    s << buffer;
}

std::vector<std::vector<double>> decodeVectorVector(eckit::Stream& s) {
    size_t size;
    s >> size;
    std::vector<size_t> innerSizes;
    size_t totalSize = 0;
    for (size_t i = 0; i < size; i++) {
        size_t innerSize;
        s >> innerSize;
        innerSizes.push_back(innerSize);
        totalSize += innerSize;
    }

    eckit::Buffer buffer(totalSize * sizeof(double));
    s >> buffer;
    double* data = (double*) buffer.data();

    std::vector<std::vector<double>> result;
    size_t offset = 0;
    for (auto& innerSize : innerSizes) {
        std::vector<double> inner(data + offset, data + offset + innerSize);
        result.push_back(inner);
        offset += innerSize;
    }
    
    return result;
}

} // namespace

ExtractionResult::ExtractionResult() {}

ExtractionResult::ExtractionResult(std::vector<std::vector<double>> values, std::vector<std::vector<std::bitset<64>>> mask): 
    values_(std::move(values)),
    mask_(std::move(mask))
    {}

ExtractionResult::ExtractionResult(eckit::Stream& s) {
    size_t numRanges;
    s >> numRanges;
    for (size_t i = 0; i < numRanges; i++) {
        values_.push_back(decodeVector(s));
    }

    // s >> numRanges;
    // for (size_t i = 0; i < numRanges; i++) {
    //     std::vector<unsigned long long> bitsetUll = decodeVectorUll(s);
    //     for (auto& b : bitsetUll) {
    //         mask_[i].push_back(std::bitset<64>(b));
    //     }

    // }
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

    s << values_.size(); // vector of vectors
    for (auto& v : values_) {
        encodeVector(s, v);
    }

    // s << mask_.size(); // vector of vectors
    // for (auto& v : mask_) {
    //     std::vector<unsigned long long> bitsetUll;
    //     for (auto& b : v) {
    //         bitsetUll.push_back(b.to_ullong());
    //     }
    //     encodeVector(s, bitsetUll);
    // }
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

//---------------------------------------------------------------------------------------------------------------------

ExtractionRequest::ExtractionRequest(const metkit::mars::MarsRequest& request, const std::vector<Range>& ranges, std::string gridHash):
    ranges_(ranges),
    request_(request),
    gridHash_(gridHash)
    {}
ExtractionRequest::ExtractionRequest() {}

ExtractionRequest::ExtractionRequest(eckit::Stream& s) {
    request_ = metkit::mars::MarsRequest(s);
    s >> gridHash_;
    size_t numRanges;
    s >> numRanges;
    for (size_t j = 0; j < numRanges; j++) {
        size_t start, end;
        s >> start >> end;
        ranges_.push_back(std::make_pair(start, end));
    }
}

std::vector<ExtractionRequest> ExtractionRequest::split(const std::string& key) const {

    std::vector<metkit::mars::MarsRequest> reqs = request_.split(key);

    std::vector<ExtractionRequest> requests;
    requests.reserve(reqs.size());
    for (auto& r : reqs) {
        requests.push_back(ExtractionRequest(r, ranges_));
    }
    return requests;
}

std::vector<ExtractionRequest> ExtractionRequest::split(const std::vector<std::string>& keys) const {
    
    std::vector<metkit::mars::MarsRequest> reqs = request_.split(keys);

    std::vector<ExtractionRequest> requests;
    requests.reserve(reqs.size());
    for (auto& r : reqs) {
        requests.push_back(ExtractionRequest(r, ranges_));
    }
    return requests;
}

eckit::Stream& operator<<(eckit::Stream& s, const ExtractionRequest& o) {
    o.encode(s);
    return s;
}

void ExtractionRequest::encode(eckit::Stream& s) const {
    s << request_;
    s << gridHash_;
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
    s << "]";
}

std::ostream& operator<<(std::ostream& s, const ExtractionRequest& o) {
    o.print(s);
    return s;
}

} // namespace gribjump
