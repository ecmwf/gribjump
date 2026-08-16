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

#include <cstddef>
#include <stdexcept>
#include <string>
#include <map>
#include <vector>

#include "eckit/utils/StringTools.h"
#include "metkit/mars/MarsRequest.h"


#include "gribjump/api/ListRequest.h"


namespace gribjump {

const std::map<std::string, std::string>& ListRequest::map() const { return request_; }

std::string ListRequest::string() const {
    std::string result;
    for (const auto& kv : request_) {
        if (!result.empty()) {
            result += ",";
        }
        result += kv.first + "=" + kv.second;
    }
    return result;
}


std::map<std::string, std::string> ListRequest::parseRequest(const std::string& request) {
    // string of "key1=value1,key2=value2,..."
    std::map<std::string, std::string> result;

    std::vector<std::string> pairs = eckit::StringTools::split(",", eckit::StringTools::trim(request));

    // tolerate the 0th element being mars verbs "retrieve" or "list"
    if (pairs.size() > 0 && (pairs[0] == "retrieve" || pairs[0] == "list")) {
        pairs.erase(pairs.begin());
    }

    // parse each pair into key and value
    for (const auto& pair : pairs) {
        size_t eq = pair.find('=');
        if (eq == std::string::npos) {
            throw std::invalid_argument("Failed to parse request: " + request);
        }
        std::string key = pair.substr(0, eq);
        std::string value = pair.substr(eq + 1);
        result[key] = value;
    }

    return result;
}

ListRequest::ListRequest(metkit::mars::MarsRequest marsRequest) {

    // Convert MarsRequest to map of key-value pairs
    for (const auto& key : marsRequest.params()) {
        const std::vector<std::string>& values = marsRequest.values(key);

        if (!values.empty()) {
            std::string valueStr = eckit::StringTools::join("/", values);
            request_[key] = valueStr;
        }
    }
   
}


}  // namespace gribjump