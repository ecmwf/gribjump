/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "fdb5/api/FDB.h"

#include "gribjump/GribJump.h"
#include "gribjump/tools/GribJumpTool.h"

/// @author Christopher Bradley

/// Tool to execute to scan a set of files to built a GribJump info index.
/// Output directory is specified in the configuration file.

namespace gribjump::tool {

class ScanFiles : public GribJumpTool {  // dont use fdb tool

    virtual void execute(const eckit::option::CmdArgs& args);
    virtual void usage(const std::string& tool) const;
    virtual int numberOfPositionalArguments() const { return -1; }

public:

    ScanFiles(int argc, char** argv) : GribJumpTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<bool>(
            "overwrite", "If true, overwrite existing .gribjump files instead of skipping. Default false."));
        options_.push_back(new eckit::option::SimpleOption<bool>(
            "merge", "If true, merge jumpinfos with existing .gribjump files instead of skipping. Default false."));
        options_.push_back(new eckit::option::SimpleOption<bool>(
            "dry-run", "If true, do not write the .gribjump files. Default false."));
    }
};

void ScanFiles::usage(const std::string& tool) const {
    eckit::Log::info() << std::endl << "Usage: " << tool << " <list of files>" << std::endl;

    GribJumpTool::usage(tool);
}

void ScanFiles::execute(const eckit::option::CmdArgs& args) {

    bool overwrite = args.getBool("overwrite", false);
    bool merge     = args.getBool("merge", false);
    bool dryrun    = args.getBool("dry-run", false);
    std::vector<std::string> files_in(args.begin(), args.end());

    if (overwrite && merge) {
        throw eckit::UserError("Cannot specify both --overwrite and --merge");
    }

    if (merge || overwrite) {
        NOTIMP;  // later...
    }

    if (files_in.empty()) {
        usage("gribjump-scan-files");
        return;
    }

    // Check each file exists, and also check if corresponding the .gribjump file exists.
    std::vector<eckit::PathName> files_scan;
    std::vector<eckit::PathName> files_skip;

    for (const std::string& file : files_in) {
        eckit::PathName path(file);
        if (!path.exists()) {
            throw eckit::UserError("File does not exist: " + path);
        }

        eckit::PathName index = path + ".gribjump";
        if (index.exists() && !overwrite && !merge) {
            files_skip.push_back(path);
        }
        else {
            files_scan.push_back(path);
        }
    }

    if (!files_skip.empty()) {
        eckit::Log::info() << "Skipping files with existing .gribjump files (use --overwrite option to regenerate):"
                           << std::endl;
        for (const eckit::PathName& path : files_skip) {
            eckit::Log::info() << "  " << path << std::endl;
        }
    }

    if (files_scan.empty()) {
        eckit::Log::info() << "No files to scan" << std::endl;
        return;
    }

    eckit::Log::info() << "Scanning files:" << std::endl;
    for (const eckit::PathName& path : files_scan) {
        eckit::Log::info() << "  " << path << std::endl;
    }

    if (dryrun)
        return;

    GribJump gj;
    gj.scan(files_scan);  // take merge/overwrite into account?
}

}  // namespace gribjump::tool

int main(int argc, char** argv) {
    gribjump::tool::ScanFiles app(argc, argv);
    return app.start();
}
