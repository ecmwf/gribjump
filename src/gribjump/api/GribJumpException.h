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


class GribJumpException : public eckit::Exception {
public:

    GribJumpException(const std::string& msg) : eckit::Exception(std::string("GribJumpException: ") + msg) {}

    GribJumpException(const std::string& msg, const eckit::CodeLocation& here) :
        eckit::Exception(std::string("GribJumpException: ") + msg, here) {}
};

// For requests matching data that is not found
class DataNotFoundException : public GribJumpException {
public:

    DataNotFoundException(const std::string& msg) : GribJumpException("DataNotFound. " + msg) {}

    DataNotFoundException(const std::string& msg, const eckit::CodeLocation& here) :
        GribJumpException("DataNotFound. " + msg, here) {}
};

class JumpInfoExtractionDisabled : public GribJumpException {
public:

    JumpInfoExtractionDisabled(const std::string& msg) :
        GribJumpException("Lazy JumpInfo extraction has been disabled. " + msg) {}

    JumpInfoExtractionDisabled(const std::string& msg, const eckit::CodeLocation& here) :
        GribJumpException("Lazy JumpInfo extraction has been disabled. " + msg, here) {}
};


}  // namespace gribjump
