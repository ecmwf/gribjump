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
/// @author Tiago Quintino

#pragma once

#include <bitset>
#include <vector>

#include "eckit/serialisation/Stream.h"
#include "gribjump/Types.h"

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

class ExtractionResult {
public:  // methods

    ExtractionResult();
    ExtractionResult(std::vector<std::vector<double>>&& values, std::vector<std::vector<std::bitset<64>>>&& mask);
    explicit ExtractionResult(eckit::Stream& s);

    // Movable, not copyable
    ExtractionResult(const ExtractionResult&)            = delete;
    ExtractionResult& operator=(const ExtractionResult&) = delete;

    ExtractionResult(ExtractionResult&&)            = default;
    ExtractionResult& operator=(ExtractionResult&&) = default;

    std::vector<std::vector<double>>& mutable_values() { return values_; }
    std::vector<std::vector<std::bitset<64>>>& mutable_mask() { return mask_; }
    const std::vector<std::vector<double>>& values() const { return values_; }
    const std::vector<std::vector<std::bitset<64>>>& mask() const { return mask_; }

    size_t nrange() const { return values_.size(); }
    size_t nvalues(size_t i) const { return values_[i].size(); }
    size_t total_values() const {
        size_t total = 0;
        for (auto& v : values_) {
            total += v.size();
        }
        return total;
    }

private:  // methods

    void encode(eckit::Stream& s) const;
    void print(std::ostream&) const;
    friend eckit::Stream& operator<<(eckit::Stream& s, const ExtractionResult& o);
    friend std::ostream& operator<<(std::ostream& s, const ExtractionResult& r);


private:  // members

    std::vector<std::vector<double>> values_;
    std::vector<std::vector<std::bitset<64>>> mask_;
};

//----------------------------------------------------------------------------------------------------------------------

class ExtractionRequest {

public:  // methods

    ExtractionRequest();
    ExtractionRequest(const std::string&, const std::vector<Range>&, std::string gridHash = "");
    explicit ExtractionRequest(eckit::Stream& s);

    const std::vector<Range>& ranges() const { return ranges_; }
    const std::string& requestString() const { return request_; }
    void requestString(const std::string& s) { request_ = s; }
    const std::string& gridHash() const { return gridHash_; }

private:  // methods

    void print(std::ostream&) const;
    void encode(eckit::Stream& s) const;
    friend eckit::Stream& operator<<(eckit::Stream& s, const ExtractionRequest& o);
    friend std::ostream& operator<<(std::ostream& s, const ExtractionRequest& r);

private:  // members

    std::vector<Range> ranges_;
    std::string request_;
    std::string gridHash_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
