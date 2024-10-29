/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */
/// @author Christopher Bradley

#include "gribjump/GribJump.h"
#include "gribjump/tools/GribJumpTool.h"
#include "gribjump/info/InfoCache.h"

namespace gribjump::tool {

class DumpInfo: public GribJumpTool {

    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int numberOfPositionalArguments() const { return -1; }

public:
    DumpInfo(int argc, char **argv): GribJumpTool(argc, argv) {}

private:
    std::vector<eckit::option::Option*> options_;
};

void DumpInfo::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " <path1> <path2> ..." << std::endl;
                       ;

    GribJumpTool::usage(tool);
}

void DumpInfo::execute(const eckit::option::CmdArgs &args) {
    for (size_t i = 0; i < args.count(); i++) {
        eckit::PathName file(args(i));
        FileCache cache = FileCache(file);
        cache.print(std::cout);
    }
}

} // namespace gribjump::tool

int main(int argc, char **argv) {
    gribjump::tool::DumpInfo app(argc, argv);
    return app.start();
}

