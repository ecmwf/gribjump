/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/exception/Exceptions.h"
#include "fdb5/api/FDB.h"

#include "gribjump/GribJump.h"
#include "gribjump/tools/GribJumpTool.h"

/// @author Christopher Bradley

/// Tool to scan a set of files to built a GribJump info index. Assumes that the files are local.
/// Output directory is specified in the configuration file.

namespace gribjump::tool {

class ScanFiles : public GribJumpTool {  // dont use fdb tool

    virtual void execute(const eckit::option::CmdArgs& args);
    virtual void usage(const std::string& tool) const;
    virtual int numberOfPositionalArguments() const { return -1; }

public:

    ScanFiles(int argc, char** argv) : GribJumpTool(argc, argv, "gribjump-scan-files") {
        options_.push_back(
            new eckit::option::SimpleOption<bool>("skipExisting",
                                                  "If true, ignore existing .gribjump files (otherwise we check "
                                                  "existing .gribjump files for missing fields). Default false."));
        // options_.push_back(new eckit::option::SimpleOption<bool>(
        //     "regenerate", "If true, regenerate .gribjump files from scratch, potentially overwriting existing
        //     .gribjump files. Default false."));
        options_.push_back(new eckit::option::SimpleOption<bool>(
            "dry-run", "If true, do not scan anything, just list the files we would scan. Default false."));
    }
};

void ScanFiles::usage(const std::string& tool) const {
    eckit::Log::info() << std::endl << "Usage: " << tool << " <list of files>" << std::endl;

    GribJumpTool::usage(tool);
}

void ScanFiles::execute(const eckit::option::CmdArgs& args) {

    bool skipExisting = args.getBool("skipExisting", false);
    // bool regenerate       = args.getBool("regenerate", false);
    bool regenerate = false;  // NOTIMP
    bool dryrun     = args.getBool("dry-run", false);
    std::vector<std::string> files_in(args.begin(), args.end());

    if (skipExisting && regenerate) {
        throw eckit::UserError("Cannot use both --skipExisting and --regenerate options at the same time.");
    }

    if (files_in.empty()) {
        usage("gribjump-scan-files");
        return;
    }

    // Check each file exists, and also check if corresponding the .gribjump file exists.
    std::vector<eckit::PathName> files_scan;
    std::vector<eckit::PathName> files_existing;

    for (const std::string& file : files_in) {
        eckit::PathName path(file);
        if (!path.exists()) {
            throw eckit::UserError("File does not exist: " + path);
        }

        eckit::PathName index = path + ".gribjump";
        if (index.exists()) {
            files_existing.push_back(path);
        }
        else {
            files_scan.push_back(path);
        }
    }

    if (!files_existing.empty()) {
        eckit::Log::info() << ".gribjump files exist for the following files:" << std::endl;
        for (const eckit::PathName& path : files_existing) {
            eckit::Log::info() << "  " << path << std::endl;
        }

        if (skipExisting) {
            eckit::Log::info() << "Skipping these files as --skipExisting option is set." << std::endl;
        }
        else if (regenerate) {
            eckit::Log::info() << "These will be overwritten as --regenerate option is set." << std::endl;
            files_scan.insert(files_scan.end(), files_existing.begin(), files_existing.end());
        }
        else {
            eckit::Log::info() << "These files will be modified if they are found to be missing fields." << std::endl;
            files_scan.insert(files_scan.end(), files_existing.begin(), files_existing.end());
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

    if (regenerate) {
        /// @todo: Instruction to regenerate needs to be propagated to the InfoCache.
        NOTIMP;
    }

    GribJump gj;
    gj.scan(files_scan, ctx_);  // take merge/overwrite into account?
}

}  // namespace gribjump::tool

int main(int argc, char** argv) {
    gribjump::tool::ScanFiles app(argc, argv);
    return app.start();
}
