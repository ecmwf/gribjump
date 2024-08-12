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

#include <fstream>
#include <memory>

#include "eckit/option/CmdArgs.h"
#include "eckit/runtime/Tool.h"
#include "eckit/option/SimpleOption.h"

#include "gribjump/GribJump.h"
#include "gribjump/info/InfoCache.h"

namespace gribjump {

static void usage(const std::string &tool) {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " <path1> <path2> ..." << std::endl;
}

class DumpInfoTool : public eckit::Tool{

    virtual int numberOfPositionalArguments() const { return -1; }

    virtual void run(const eckit::option::CmdArgs &args);

    virtual void run() override {
        eckit::option::CmdArgs args(usage, options_, numberOfPositionalArguments());
        run(args);
    }

public:
    DumpInfoTool(int argc, char **argv): eckit::Tool(argc, argv, "GRIBJUMP_HOME") {}

private:
    std::vector<eckit::option::Option*> options_;
};

void DumpInfoTool::run(const eckit::option::CmdArgs &args) {
    for (size_t i = 0; i < args.count(); i++) {
        eckit::PathName file(args(i));
        FileCache cache = FileCache(file);
        cache.print(std::cout);
    }
}

} // namespace gribjump

int main(int argc, char **argv) {
    gribjump::DumpInfoTool app(argc, argv);
    return app.start();
}

