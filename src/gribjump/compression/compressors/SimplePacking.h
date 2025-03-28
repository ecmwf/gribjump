/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <cstddef>
#include <vector>
#include "eckit/io/Buffer.h"

namespace gribjump::mc {

template <typename T>
struct DecodeParameters {
    using ValueType = T;

    ValueType reference_value;
    ValueType binary_scale_factor;
    ValueType decimal_scale_factor;
    long bits_per_value;
    size_t n_vals;

    DecodeParameters() :
        reference_value(0), binary_scale_factor(0), decimal_scale_factor(0), bits_per_value(0), n_vals(0) {}
};

template <typename T>
class SimplePacking {
public:

    using ValueType = T;
    using Values    = std::vector<ValueType>;

    SimplePacking()  = default;
    ~SimplePacking() = default;

    Values unpack(const DecodeParameters<ValueType>& params, const eckit::Buffer& encoded, long bitp = 0);
};

}  // namespace gribjump::mc