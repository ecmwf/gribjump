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

#include <cstddef>
#include <string>
#include <map>
#include <utility>

namespace metkit::mars{
    class MarsRequest;
}


namespace gribjump {


// Internal representation is a map of key-value pairs. 
class ListRequest {
public:
    explicit ListRequest(const std::string& request) : request_(parseRequest(request)) {}
    explicit ListRequest(std::map<std::string, std::string> request) : request_(std::move(request)) {}
    explicit ListRequest(metkit::mars::MarsRequest marsRequest);

    const std::map<std::string, std::string>& map() const;
    std::string string() const;

private:

    static std::map<std::string, std::string> parseRequest(const std::string& request);

private:
    std::map<std::string, std::string> request_;
};


}  // namespace gribjump