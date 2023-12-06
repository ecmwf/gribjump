/*
 * (C) Copyright 1996- ECMWF.
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
#include <vector>
#include "eckit/serialisation/Stream.h"
#include "metkit/mars/MarsRequest.h"

using Range = std::tuple<size_t, size_t>;

namespace gribjump {

class ExtractionResult  {
public: // methods
    // NB Takes ownership of inputs
    ExtractionResult(std::vector<std::vector<double>> values, std::vector<std::vector<std::bitset<64>>> mask);
    ExtractionResult(eckit::Stream& s);

    std::vector<std::vector<double>> getValues() const {return values_;}
    std::vector<std::vector<std::bitset<64>>> getMask() const {return mask_;}

private: // methods
    friend eckit::Stream& operator<<(eckit::Stream& s, const ExtractionResult& o);
    void encode(eckit::Stream& s) const;

private: // members
    std::vector<std::vector<double>> values_;
    std::vector<std::vector<std::bitset<64>>> mask_;
};


class ExtractionRequest {

public: // methods
    // NB Takes ownership of inputs
    ExtractionRequest(metkit::mars::MarsRequest, std::vector<Range>);
    ExtractionRequest(eckit::Stream& s);

    std::vector<Range> getRanges() const {return ranges_;}
    metkit::mars::MarsRequest getRequest() const {return request_;}

private: // methods
    friend eckit::Stream& operator<<(eckit::Stream& s, const ExtractionRequest& o);
    void encode(eckit::Stream& s) const;

private: // members
    std::vector<Range> ranges_;
    metkit::mars::MarsRequest request_;
};

} // namespace gribjump
