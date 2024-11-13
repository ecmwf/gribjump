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

// #pragma once

// #include <vector>
// #include "eckit/serialisation/Stream.h"
// #include "gribjump/ExtractionData.h"
// #include "gribjump/Types.h"

// // Class to help with serialisation of containers

// namespace gribjump {

// class Serialiser {
// public:
//     static void encode(eckit::Stream& s, const std::vector<double>& v);
//     static std::vector<double> decodeVector(eckit::Stream& s);

//     static void encode(eckit::Stream& s, const std::vector<std::string>& v);
//     static std::vector<std::string> decodeVectorString(eckit::Stream& s);
    
//     static void encode(eckit::Stream& s, const std::vector<std::vector<double>>& v);
//     static std::vector<std::vector<double>> decodeVectorVector(eckit::Stream& s);

//     static void encode(eckit::Stream& s, const std::vector<ExtractionRequest>& v, bool naive=false);
//     static std::vector<ExtractionRequest> decodeExtractionRequests(eckit::Stream& s, bool naive=false);

//     // We tend to have a vector of vectors of ExtractionResults
//     static void encode(eckit::Stream& s, const std::vector<std::vector<ExtractionResult>>& v);
//     static std::vector<std::vector<ExtractionResult>> decodeExtractionResults(eckit::Stream& s);


//     static void encode(eckit::Stream& s, const Ranges& v, bool naive=false);
//     static Ranges decodeRanges(eckit::Stream& s, bool naive=false);

//     //-------------------------------------------------------------------------------------------------
//     // Naive implementations, for timing comparison
//     static void encodeNaive(eckit::Stream& s, const std::vector<std::string>& v);
//     static std::vector<std::string> decodeVectorStringNaive(eckit::Stream& s);
// };

// } // namespace gribjump

