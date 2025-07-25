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
#include "eckit/io/Buffer.h"
#include "eckit/value/Value.h"

namespace gribjump {

namespace {

template <typename T>
void encodeVector(eckit::Stream& s, const std::vector<T>& v) {
    size_t size = v.size();
    s << size;
    eckit::Buffer buffer(v.data(), size * sizeof(T));
    s << buffer;
}

template <typename T>
std::vector<T> decodeVector(eckit::Stream& s) {
    size_t size;
    s >> size;
    eckit::Buffer buffer(size * sizeof(T));
    s >> buffer;
    T* data = (T*)buffer.data();

    return std::vector<T>(data, data + size);
}

template <typename T>
void encodeVectorVector(eckit::Stream& s, const std::vector<std::vector<T>>& vv) {
    std::vector<size_t> sizes;
    sizes.reserve(vv.size());
    size_t totalSize = 0;
    for (auto& v : vv) {
        sizes.push_back(v.size());
        totalSize += v.size();
    }
    encodeVector(s, sizes);

    // Make a flat vector
    std::vector<T> flat;
    flat.reserve(totalSize);
    for (auto& v : vv) {
        flat.insert(flat.end(), v.begin(), v.end());
    }
    encodeVector(s, flat);
}

template <typename T>
std::vector<std::vector<T>> decodeVectorVector(eckit::Stream& s) {
    std::vector<size_t> sizes = decodeVector<size_t>(s);
    std::vector<T> flat       = decodeVector<T>(s);

    std::vector<std::vector<T>> vv;
    size_t pos = 0;
    for (auto& size : sizes) {
        vv.push_back(std::vector<T>(flat.begin() + pos, flat.begin() + pos + size));
        pos += size;
    }
    return vv;
}

void encodeRanges(eckit::Stream& s, const std::vector<Range>& ranges) {
    size_t size = ranges.size();
    s << size;
    eckit::Buffer buffer(ranges.data(), size * sizeof(size_t) * 2);  // does this really work?
    s << buffer;
}

std::vector<Range> decodeRanges(eckit::Stream& s) {
    size_t size;
    s >> size;
    eckit::Buffer buffer(size * sizeof(size_t) * 2);
    s >> buffer;
    size_t* data = (size_t*)buffer.data();

    std::vector<Range> ranges;
    for (size_t i = 0; i < size; i++) {
        ranges.push_back(std::make_pair(data[i * 2], data[i * 2 + 1]));
    }

    return ranges;
}

void encodeMask(eckit::Stream& s, const std::vector<std::vector<std::bitset<64>>>& mask) {

    size_t totalSize = 0;
    std::vector<size_t> sizes;
    for (auto& v : mask) {
        totalSize += v.size();
        sizes.push_back(v.size());
    }
    std::vector<uint64_t> flat;
    flat.reserve(totalSize);
    for (auto& v : mask) {
        for (auto& b : v) {
            flat.push_back(b.to_ullong());
        }
    }
    encodeVector(s, sizes);
    encodeVector(s, flat);
}

std::vector<std::vector<std::bitset<64>>> decodeMask(eckit::Stream& s) {

    std::vector<size_t> sizes  = decodeVector<size_t>(s);
    std::vector<uint64_t> flat = decodeVector<uint64_t>(s);

    std::vector<std::vector<std::bitset<64>>> mask;
    size_t pos = 0;
    for (auto& size : sizes) {
        std::vector<std::bitset<64>> maskBitset;
        for (size_t i = 0; i < size; i++) {
            maskBitset.push_back(std::bitset<64>(flat[pos + i]));
        }
        mask.push_back(maskBitset);
        pos += size;
    }
    return mask;
}
}  // namespace

ExtractionResult::ExtractionResult() {}

ExtractionResult::ExtractionResult(std::vector<std::vector<double>>&& values,
                                   std::vector<std::vector<std::bitset<64>>>&& mask) :
    values_(std::move(values)), mask_(std::move(mask)) {}

ExtractionResult::ExtractionResult(eckit::Stream& s) {
    values_ = decodeVectorVector<double>(s);
    mask_   = decodeMask(s);
}

void ExtractionResult::encode(eckit::Stream& s) const {
    encodeVectorVector(s, values_);
    encodeMask(s, mask_);
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

ExtractionRequest::ExtractionRequest(const std::string& request, const std::vector<Range>& ranges,
                                     std::string gridHash) :
    ranges_(ranges), request_(request), gridHash_(gridHash) {}

ExtractionRequest::ExtractionRequest(const std::string& filename, const std::string& scheme, size_t offset,
                                     const std::vector<Range>& ranges, const std::string& gridHash) :
    ranges_(ranges), gridHash_(gridHash) {

    std::ostringstream oss;
    // oss << "uri=" << scheme << "://" << filename << "#offset=" << offset;
    oss << scheme << ":" << filename << "#" << offset;
    request_ = oss.str();
}

ExtractionRequest::ExtractionRequest() {}

ExtractionRequest::ExtractionRequest(eckit::Stream& s) {
    s >> request_;
    s >> gridHash_;
    ranges_ = decodeRanges(s);
}

eckit::Stream& operator<<(eckit::Stream& s, const ExtractionRequest& o) {
    o.encode(s);
    return s;
}

void ExtractionRequest::encode(eckit::Stream& s) const {
    s << request_;
    s << gridHash_;
    encodeRanges(s, ranges_);
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

}  // namespace gribjump
