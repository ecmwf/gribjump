/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "gribjump/tools/GribJumpTool.h"
#include "gribjump/LibGribJump.h"

using eckit::Log;

namespace gribjump {

//----------------------------------------------------------------------------------------------------------------------

static GribJumpTool* instance_ = nullptr;

GribJumpTool::GribJumpTool(int argc, char** argv, const std::string& toolname) : eckit::Tool(argc, argv, "GRIBJUMP_HOME") {
    ASSERT(instance_ == nullptr);
    instance_ = this;

    std::stringstream ss;
    eckit::JSON j{ss};
    j.startObject();
    j << "origin" << toolname;
    j << "description" << "Command line tool";
    j.endObject();
    
    LogContext ctx_(ss.str());
    
}

static void usage(const std::string& tool) {
    ASSERT(instance_);
    instance_->usage(tool);
}

void GribJumpTool::run() {
    eckit::option::CmdArgs args(&gribjump::usage, options_, numberOfPositionalArguments(),
                                minimumPositionalArguments());

    init(args);
    execute(args);
    finish(args);
}

void GribJumpTool::usage(const std::string&) const {}

void GribJumpTool::init(const eckit::option::CmdArgs&) {}

void GribJumpTool::finish(const eckit::option::CmdArgs&) {}

//----------------------------------------------------------------------------------------------------------------------

GribJumpToolException::GribJumpToolException(const std::string& w) : Exception(w) {}

GribJumpToolException::GribJumpToolException(const std::string& w, const eckit::CodeLocation& l) : Exception(w, l) {}


//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump
