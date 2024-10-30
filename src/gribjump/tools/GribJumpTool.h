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

#include <vector>

#include "eckit/runtime/Tool.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"

namespace eckit {
    namespace option {
    class Option;
    class CmdArgs;
    }
}

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

class GribJumpTool : public eckit::Tool {

protected: // methods

    GribJumpTool(int argc, char **argv);
    ~GribJumpTool() override {}

    void run() override;

public: // methods

    virtual void usage(const std::string &tool) const;

protected: // members

    std::vector<eckit::option::Option *> options_;

protected: // methods

    virtual void init(const eckit::option::CmdArgs& args);
    virtual void finish(const eckit::option::CmdArgs& args);

private: // methods

    virtual void execute(const eckit::option::CmdArgs& args) = 0;

    virtual int numberOfPositionalArguments() const { return -1; }
    virtual int minimumPositionalArguments() const { return -1; }
};

//----------------------------------------------------------------------------------------------------------------------


class GribJumpToolException : public eckit::Exception {
public:
    GribJumpToolException(const std::string&);
    GribJumpToolException(const std::string&, const eckit::CodeLocation&);
};


//----------------------------------------------------------------------------------------------------------------------

} // namespace gribjump
