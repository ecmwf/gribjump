/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @file   FDBTool.h
/// @author Baudouin Raoult
/// @author Tiago Quintino
/// @date   Mar 2016

#pragma once

#include <string>
#include <vector>
#include "eckit/exception/Exceptions.h"
#include "eckit/runtime/Tool.h"
#include "gribjump/config/Metrics.h"

namespace eckit {
class CodeLocation;
}
namespace eckit {
namespace option {
class CmdArgs;
}
}  // namespace eckit
namespace eckit {
namespace option {
class Option;
}
}  // namespace eckit

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

class GribJumpTool : public eckit::Tool {

protected:  // methods

    GribJumpTool(int argc, char** argv, const std::string& toolname);
    ~GribJumpTool() override {}

    void run() override;

public:  // methods

    virtual void usage(const std::string& tool) const;

protected:  // methods

    virtual void init(const eckit::option::CmdArgs& args);
    virtual void finish(const eckit::option::CmdArgs& args);

private:  // methods

    virtual void execute(const eckit::option::CmdArgs& args) = 0;

    virtual int numberOfPositionalArguments() const { return -1; }
    virtual int minimumPositionalArguments() const { return -1; }


protected:  // members

    std::vector<eckit::option::Option*> options_;
    LogContext ctx_;
};

//----------------------------------------------------------------------------------------------------------------------


class GribJumpToolException : public eckit::Exception {
public:

    GribJumpToolException(const std::string&);
    GribJumpToolException(const std::string&, const eckit::CodeLocation&);
};


//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
