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

#pragma once


#include "eckit/exception/Exceptions.h"

namespace gribjump {


class JumpException : public eckit::Exception {
public:

    JumpException(const std::string& msg, const eckit::CodeLocation& here) :
        eckit::Exception(std::string("JumpException: ") + msg, here) {}

};

} // namespace gribjump
