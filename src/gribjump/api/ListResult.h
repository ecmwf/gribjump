/*
 * (C) Copyright 2026- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gribjump/api/ResultIterator.h"

namespace gribjump {

class ListResult {
public:
    virtual ~ListResult() = default;
    virtual std::string marsRequest() const = 0;
    virtual std::string json() const        = 0;  
};


// A concrete ListResult backed by owned strings.
// TODO I think ListResult itself should probably just be a concrete class.
class ListResultItem : public ListResult {
public:

    ListResultItem(std::string marsRequest, std::string json) :
        marsRequest_(std::move(marsRequest)), json_(std::move(json)) {}

    std::string marsRequest() const override { return marsRequest_; }
    std::string json() const override { return json_; }

private:

    std::string marsRequest_;
    std::string json_;
};


using ListSource   = ResultSource<ListResult>;
using ListIterator = ResultIterator<ListResult>;

}  // namespace gribjump