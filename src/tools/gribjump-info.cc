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

#include "gribjump/GribJump.h"
#include "gribjump/tools/GribJumpTool.h"
#include "gribjump/info/InfoCache.h"

namespace gribjump::tool {

class Info: public GribJumpTool {

    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int numberOfPositionalArguments() const { return 0; }

public:
    Info(int argc, char **argv): GribJumpTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<bool>("all", "Print all information (default behaviour if no options are set)"));
        options_.push_back(new eckit::option::SimpleOption<bool>("sha1", "Print the git sha1"));
        options_.push_back(new eckit::option::SimpleOption<bool>("version", "Print the version"));
        options_.push_back(new eckit::option::SimpleOption<bool>("home", "Print the library home"));
        options_.push_back(new eckit::option::SimpleOption<bool>("library", "Print the library path"));
        options_.push_back(new eckit::option::SimpleOption<bool>("config", "Print the config path"));
    }

private:
    void logInfo(bool condition, const std::string& label, const std::string& value);

private:
    bool all_;
};

void Info::usage(const std::string &tool) const {
    GribJumpTool::usage(tool);
}

void Info::logInfo(bool condition, const std::string& label, const std::string& value) {
    if (all_ || condition) {
        eckit::Log::info() << (all_ ? label : "") << value << std::endl;
    }
}

void Info::execute(const eckit::option::CmdArgs &args) {

    all_ = args.getBool("all", false);
    bool version_ = args.getBool("version", false);
    bool sha1_ = args.getBool("sha1", false);
    bool home_ = args.getBool("home", false);
    bool library_ = args.getBool("library", false);
    bool config_ = args.getBool("config", false);

    // set all_ to true if no specific options are set
    all_ = all_ || (!version_ && !sha1_ && !home_ && !library_ && !config_);

    logInfo(version_, "Version: ", LibGribJump::instance().version());
    logInfo(sha1_, "gitsha1: ", LibGribJump::instance().gitsha1());
    logInfo(home_, "Home: ", LibGribJump::instance().libraryHome());
    logInfo(library_, "Library path: ", LibGribJump::instance().libraryPath());

    std::string path = LibGribJump::instance().config().path();
    if (path.empty()) {
        path = "No config file found (use default config)";
    }
    logInfo(config_, "Config: ", path);

}

} // namespace gribjump::tool

int main(int argc, char **argv) {
    gribjump::tool::Info app(argc, argv);
    return app.start();
}
