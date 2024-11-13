// /*
//  * (C) Copyright 2024- ECMWF.
//  *
//  * This software is licensed under the terms of the Apache Licence Version 2.0
//  * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
//  * In applying this licence, ECMWF does not waive the privileges and immunities
//  * granted to it by virtue of its status as an intergovernmental organisation nor
//  * does it submit to any jurisdiction.
//  */

// /// @author Christopher Bradley

// #include "eckit/value/Value.h"
// #include "eckit/io/Buffer.h"
// #include "gribjump/Serialiser.h"


// namespace gribjump {

// void Serialiser::encode(eckit::Stream& s, const std::vector<double>& v) {
//     size_t size = v.size();
//     s << size;
//     eckit::Buffer buffer(v.data(), size * sizeof(double));
//     s << buffer;
// }

// std::vector<double> Serialiser::decodeVector(eckit::Stream& s) {
//     size_t size;
//     s >> size;
//     eckit::Buffer buffer(size * sizeof(double));
//     s >> buffer;
//     double* data = (double*) buffer.data();

//     return std::vector<double>(data, data + size);
// }

// // -----------------------------------------------------------------------------------------------
// // vector of pairs

// // conclusion: naive is worse for vector of pairs.
// void Serialiser::encode(eckit::Stream& s, const Ranges& v, bool naive) {
//     if (naive) {
//         size_t size = v.size();
//         s << size;
//         for (auto& pair : v) {
//             s << pair.first;
//             s << pair.second;
//         }
//         return;
//     }
//     // else
//     size_t size = v.size();
//     s << size;

//     // We know they are pairs, don't need size
//     eckit::Buffer buffer(v.data(), v.size() * sizeof(size_t) * 2);
//     s << buffer;
// }

// Ranges Serialiser::decodeRanges(eckit::Stream& s, bool naive) {
//     if (naive) {
//         Ranges result;
//         size_t size;
//         s >> size;
//         for (size_t i = 0; i < size; i++) {
//             size_t first;
//             size_t second;
//             s >> first;
//             s >> second;
//             result.push_back(std::make_pair(first, second));
//         }
//         return result;
//     }
//     // else
//     size_t size;
//     s >> size;
//     eckit::Buffer buffer(size * sizeof(size_t) * 2);
//     s >> buffer;
//     size_t* data = (size_t*) buffer.data();

//     Ranges result;
//     for (size_t i = 0; i < size; i++) {
//         result.push_back(std::make_pair(data[i*2], data[i*2 + 1]));
//     }
//     return result;
// }


// // -----------------------------------------------------------------------------------------------

// void Serialiser::encode(eckit::Stream& s, const std::vector<std::string>& v) {
//     // Don't want to just do s << str, since this is quite slow.
//     // Use a buffer for all strings.
//     size_t size = v.size();
//     s << size;
//     size_t totalSize = 0;
//     for (auto& str : v) {
//         totalSize += str.size();
//         s << str.size();
//     }
//     eckit::Buffer buffer(totalSize);
//     char* data = (char*) buffer.data();
//     for (auto& str : v) {
//         for (auto& c : str) {
//             *data++ = c;
//         }
//     }
//     s << buffer;
// }

// std::vector<std::string> Serialiser::decodeVectorString(eckit::Stream& s) {
//     size_t size;
//     s >> size;
//     std::vector<size_t> innerSizes;
//     size_t totalSize = 0;
//     for (size_t i = 0; i < size; i++) {
//         size_t innerSize;
//         s >> innerSize;
//         innerSizes.push_back(innerSize);
//         totalSize += innerSize;
//     }

//     eckit::Buffer buffer(totalSize);
//     s >> buffer;
//     char* data = (char*) buffer.data();

//     std::vector<std::string> result;
//     size_t offset = 0;
//     for (auto& innerSize : innerSizes) {
//         std::string inner(data + offset, innerSize);
//         result.push_back(inner);
//         offset += innerSize;
//     }

//     return result;
// }

// // Naive version is actually faster
// void Serialiser::encodeNaive(eckit::Stream& s, const std::vector<std::string>& v) {
//     s << v;
// }

// std::vector<std::string> Serialiser::decodeVectorStringNaive(eckit::Stream& s) {
//     std::vector<std::string> result;
//     s >> result;
//     return result;
// }

// // -----------------------------------------------------------------------------------------------

// void Serialiser::encode(eckit::Stream& s, const std::vector<std::vector<double>>& v) {
//     size_t size = v.size();
//     s << size;
//     size_t totalSize = 0;
//     for (auto& inner : v) {
//         totalSize += inner.size();
//         s << inner.size();
//     }
//     eckit::Buffer buffer(totalSize * sizeof(double));
//     double* data = (double*) buffer.data();
//     for (auto& inner : v) {
//         for (auto& d : inner) {
//             *data++ = d;
//         }
//     }
//     s << buffer;
// }

// std::vector<std::vector<double>> Serialiser::decodeVectorVector(eckit::Stream& s) {
//     std::vector<std::vector<double>> result;

//     size_t size;
//     s >> size;
//     std::vector<size_t> innerSizes;
//     size_t totalSize = 0;
//     for (size_t i = 0; i < size; i++) {
//         size_t innerSize;
//         s >> innerSize;
//         innerSizes.push_back(innerSize);
//         totalSize += innerSize;
//     }

//     eckit::Buffer buffer(totalSize * sizeof(double));
//     s >> buffer;
//     double* data = (double*) buffer.data();

//     size_t offset = 0;
//     for (auto& innerSize : innerSizes) {
//         std::vector<double> inner(data + offset, data + offset + innerSize);
//         result.push_back(inner);
//         offset += innerSize;
//     }
    
//     return result;
// }

// // -----------------------------------------------------------------------------------------------
// void Serialiser::encode(eckit::Stream& s, const std::vector<ExtractionRequest>& v, bool naive) {
//     if (naive) {
//         size_t size = v.size();
//         s << size;
//         for (auto& req : v) {
//             req.encode(s);
//         }
//         return;
//     }

//     std::vector<std::string> gridHashes;
//     RangesList ranges;

//     // reserve
//     gridHashes.reserve(v.size());
//     ranges.reserve(v.size());

//     for (auto& req : v) { // This copy is grim
//         gridHashes.push_back(req.gridHash());
//         ranges.push_back(req.ranges());
//     }

//     encodeNaive(s, gridHashes);
//     s << ranges.size();
//     for (auto& r : ranges) {
//         encode(s, r, false);
//     }

//     // encode the mars requests naively
//     s << v.size();
//     for (auto& req : v) {
//         s << req.request();
//     }
// }

// std::vector<ExtractionRequest> Serialiser::decodeExtractionRequests(eckit::Stream& s, bool naive) {
//     if (naive) {
//         std::vector<ExtractionRequest> result;
//         size_t size;
//         s >> size;
//         for (size_t i = 0; i < size; i++) {
//             result.push_back(ExtractionRequest(s));
//         }
//         return result;
//     }

//     std::vector<std::string> gridHashes = decodeVectorStringNaive(s);
   
//     size_t numRanges;
//     s >> numRanges;
//     RangesList ranges;
//     ranges.reserve(numRanges);
//     for (size_t i = 0; i < numRanges; i++) {
//         ranges.push_back(decodeRanges(s, false));
//     }

//     std::vector<metkit::mars::MarsRequest> marsrequests;
//     size_t numMarsRequests;
//     s >> numMarsRequests;
//     marsrequests.reserve(numMarsRequests);
//     for (size_t i = 0; i < numMarsRequests; i++) {
//         metkit::mars::MarsRequest marsrequest(s);
//         marsrequests.push_back(marsrequest);
//     }

//     // repack
//     std::vector<ExtractionRequest> result;
//     for (size_t i = 0; i < marsrequests.size(); i++) {
//         result.push_back(ExtractionRequest(marsrequests[i], ranges[i], gridHashes[i]));
//     }

//     return result;
// }


// // -----------------------------------------------------------------------------------------------
// void Serialiser::encode(eckit::Stream& s, const std::vector<std::vector<ExtractionResult>>& v) {
//     size_t size = v.size();
//     s << size;
//     for (auto& inner : v) {
//         size_t innerSize = inner.size();
//         s << innerSize;
//         for (auto& res : inner) {
//             res.encode(s);
//         }
//     }

// }

// std::vector<std::vector<ExtractionResult>> Serialiser::decodeExtractionResults(eckit::Stream& s) {
//     std::vector<std::vector<ExtractionResult>> result;

//     size_t size;
//     s >> size;
//     for (size_t i = 0; i < size; i++) {
//         size_t innerSize;
//         s >> innerSize;
//         std::vector<ExtractionResult> inner;
//         for (size_t j = 0; j < innerSize; j++) {
//             inner.push_back(ExtractionResult(s));
//         }
//         result.push_back(std::move(inner));
//     }

//     return result;
// }


// } // namespace gribjump

