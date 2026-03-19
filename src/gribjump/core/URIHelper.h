/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/URI.h"
#pragma once

namespace gribjump {

// Helper class to extend eckit::URI
class URIHelper {
public:

    static eckit::Offset offset(const eckit::URI& uri) {
        const std::string& fragment = uri.fragment();
        eckit::Offset offset;
        try {
            offset = std::stoll(fragment);
        }
        catch (std::invalid_argument& e) {
            throw eckit::BadValue("Invalid offset: '" + fragment + "' in URI: " + uri.asString(), Here());
        }
        return offset;
    }

    static bool isRemote(const eckit::URI& uri) { return uri.scheme() == "fdb"; }
};

}  // namespace gribjump