/*
 * (C) Copyright 2026- ECMWF.
 * Licensed under the Apache Licence Version 2.0. ECMWF does not waive its
 * privileges and immunities or submit to any jurisdiction.
 */
#include <iostream>

#include "gribjump/GribJump.h"
#include "gribjump/tools/GribJumpTool.h"

namespace gribjump::tool {
class GribJumpStatus : public GribJumpTool {
public:

    GribJumpStatus(int argc, char** argv) : GribJumpTool(argc, argv, "gribjump-status") {
        options_.push_back(new eckit::option::SimpleOption<double>(
            "timeout", "Per-server connect/I/O timeout in seconds (default 5)"));
        options_.push_back(new eckit::option::SimpleOption<std::string>("config", "GribJump YAML configuration file"));
    }

private:

    int numberOfPositionalArguments() const override { return 0; }
    int minimumPositionalArguments() const override { return 0; }
    void usage(const std::string& tool) const override {
        std::cerr << "Usage: " << tool << " [--config=file.yaml] [--timeout=5]\n";
    }
    void execute(const eckit::option::CmdArgs& args) override {
        std::unique_ptr<GribJump> gj;
        if (args.has("config"))
            gj = std::make_unique<GribJump>(Config(eckit::PathName(args.getString("config"))));
        else
            gj = std::make_unique<GribJump>();
        std::cout << gj->status(args.getDouble("timeout", 5.0), ctx_) << std::endl;
    }
};
}  // namespace gribjump::tool

int main(int argc, char** argv) {
    gribjump::tool::GribJumpStatus app(argc, argv);
    return app.start();
}
