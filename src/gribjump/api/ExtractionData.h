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

/// @file ExtractionData.h
/// @brief Public data containers for GribJump extraction requests and results.

#pragma once

#include <stddef.h>
#include <bitset>
#include <iosfwd>
#include <string>
#include <vector>

#include "gribjump/api/Types.h"

namespace eckit {
class Stream;
}

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

/// @brief Container holding extracted values and missing-value masks.
///
/// This type is move-only. Values are stored per requested range as nested
/// vectors. Missing-value flags are stored as vectors of bitset<64>, with
/// bit value 1 indicating present data and 0 indicating missing data.
class ExtractionResult {
public:  // methods

    /// @brief Construct an empty extraction result.
    ExtractionResult();

    /// @brief Construct from extracted values and missing-value masks.
    ExtractionResult(std::vector<std::vector<double>>&& values, std::vector<std::vector<std::bitset<64>>>&& mask);

    /// @brief Decode a result from a serialisation stream.
    /// @param s Input stream containing a serialised ExtractionResult.
    explicit ExtractionResult(eckit::Stream& s);

    ExtractionResult(const ExtractionResult&)            = delete;
    ExtractionResult& operator=(const ExtractionResult&) = delete;

    ExtractionResult(ExtractionResult&&)            = default;
    ExtractionResult& operator=(ExtractionResult&&) = default;

    /// @brief Mutable access to extracted values.
    /// @return Nested vector where outer index is range and inner index is data point.
    std::vector<std::vector<double>>& mutable_values() { return values_; }

    /// @brief Mutable access to missing-value bit masks.
    /// @return Nested vector where outer index is range and inner vector contains bitset<64> masks.
    std::vector<std::vector<std::bitset<64>>>& mutable_mask() { return mask_; }

    /// @brief Read-only access to extracted values.
    /// @return Nested vector where outer index is requested range and inner index is data point.
    const std::vector<std::vector<double>>& values() const { return values_; }

    /// @brief Read-only access to missing-value masks.
    /// @return Nested vector of bitset<64> masks (1 = present, 0 = missing), grouped by range.
    const std::vector<std::vector<std::bitset<64>>>& mask() const { return mask_; }

    /// @brief Number of requested ranges represented in this result.
    /// @return Range count.
    size_t nrange() const { return values_.size(); }

    /// @brief Number of extracted values in one range.
    /// @param i Zero-based range index.
    /// @return Number of values in range @p i.
    size_t nvalues(size_t i) const { return values_[i].size(); }

    /// @brief Total number of extracted values across all ranges.
    /// @return Sum of value counts for every range.
    size_t total_values() const {
        size_t total = 0;
        for (auto& v : values_) {
            total += v.size();
        }
        return total;
    }

private:  // methods

    /// @brief Encode this result into a serialisation stream.
    /// @param s Output stream.
    void encode(eckit::Stream& s) const;

    /// @brief Print a human-readable representation.
    /// @param[out] Output stream.
    void print(std::ostream&) const;

    /// @brief Stream serialisation helper.
    /// @param s Stream to write into.
    /// @param o Result object to serialise.
    /// @return Reference to @p s.
    friend eckit::Stream& operator<<(eckit::Stream& s, const ExtractionResult& o);

    /// @brief Stream-print helper.
    /// @param s Output stream.
    /// @param r Result object to print.
    /// @return Reference to @p s.
    friend std::ostream& operator<<(std::ostream& s, const ExtractionResult& r);


private:  // members

    std::vector<std::vector<double>> values_;
    std::vector<std::vector<std::bitset<64>>> mask_;
};

//----------------------------------------------------------------------------------------------------------------------

/// @brief Request describing what subset data to extract from matching fields.
///
/// An ExtractionRequest pairs a MARS request string with one or more element
/// index ranges and an optional grid hash.
class ExtractionRequest {

public:  // methods

    /// @brief Construct an empty extraction request.
    ExtractionRequest();

    /// @brief Construct a request from MARS request string, ranges, and grid hash.
    /// @param request MARS request string identifying target field(s).
    /// @param ranges Element index ranges to extract from matching fields.
    /// @param gridHash Optional grid identifier used for grid compatibility checks.
    ExtractionRequest(const std::string&, const std::vector<Range>&, std::string gridHash = "");

    /// @brief Decode a request from a serialisation stream.
    /// @param s Input stream containing a serialised ExtractionRequest.
    explicit ExtractionRequest(eckit::Stream& s);

    /// @brief Access extraction ranges.
    /// @return Half-open element index ranges to extract.
    const std::vector<Range>& ranges() const { return ranges_; }

    /// @brief Access the MARS request string.
    /// @return Request string identifying the field(s).
    const std::string& requestString() const { return request_; }

    /// @brief Set the MARS request string.
    /// @param s New request string.
    void requestString(const std::string& s) { request_ = s; }

    /// @brief Access optional grid hash.
    /// @return Grid hash string, or empty string if unspecified.
    const std::string& gridHash() const { return gridHash_; }

private:  // methods

    /// @brief Print a human-readable representation.
    /// @param[out] Output stream.
    void print(std::ostream&) const;

    /// @brief Encode this request into a serialisation stream.
    /// @param s Output stream.
    void encode(eckit::Stream& s) const;

    /// @brief Stream serialisation helper.
    /// @param s Stream to write into.
    /// @param o Request object to serialise.
    /// @return Reference to @p s.
    friend eckit::Stream& operator<<(eckit::Stream& s, const ExtractionRequest& o);

    /// @brief Stream-print helper.
    /// @param s Output stream.
    /// @param r Request object to print.
    /// @return Reference to @p s.
    friend std::ostream& operator<<(std::ostream& s, const ExtractionRequest& r);

private:  // members

    std::vector<Range> ranges_;
    std::string request_;
    std::string gridHash_;
};

/// @brief Extraction request targeting a specific file location.
///
/// Extends ExtractionRequest with URI path metadata and message location details
/// (scheme, host, port, and offset) for direct file access.
class PathExtractionRequest : public ExtractionRequest {
public:

    /// @brief Construct a path-based extraction request.
    /// @param filename File path.
    /// @param scheme URI scheme (for example "file").
    /// @param offset Byte offset of the target GRIB message.
    /// @param host Host for remote access.
    /// @param port Port for remote access.
    /// @param ranges Element index ranges to extract.
    /// @param gridHash Optional grid identifier used for grid compatibility checks.
    PathExtractionRequest(const std::string& filename, const std::string& scheme, size_t offset,
                          const std::string& host, int port, const std::vector<Range>& ranges,
                          const std::string& gridHash = "");

    /// @brief Access the request file path.
    /// @return File path.
    const std::string& path() const { return path_; }

    /// @brief Access URI scheme.
    /// @return URI scheme string.
    const std::string& scheme() const { return scheme_; }

    /// @brief Access byte offset of the target GRIB message.
    /// @return Byte offset.
    size_t offset() const { return offset_; }

    /// @brief Access host used for remote access.
    /// @return Host string.
    const std::string& host() const { return host_; }

    /// @brief Access port used for remote access.
    /// @return Port number.
    int port() const { return port_; }

private:

    std::string path_;
    std::string scheme_;
    std::string host_;
    int port_;
    size_t offset_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
