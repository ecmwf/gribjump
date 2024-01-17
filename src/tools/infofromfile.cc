/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "eckit/runtime/Application.h"
#include "eckit/serialisation/FileStream.h"
#include "eckit/log/Log.h"
#include "eckit/log/TimeStamp.h"
#include "eckit/types/Date.h"
#include "eckit/option/CmdArgs.h"
#include "eckit/option/SimpleOption.h"
#include "eckit/value/Value.h"
#include "eckit/utils/StringTools.h"


#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBTool.h"

#include "gribjump/GribHandleData.h"
#include "gribjump/GribInfoCache.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/Config.h"

namespace gribjump {


// TODO Probably doesn't need to be an FDBTool
class FileCacher : public fdb5::FDBTool {
    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int numberOfPositionalArguments() const { return 2; }
  public:
    FileCacher(int argc, char **argv): fdb5::FDBTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<std::string>("outdir", "Directory to write cache files to"));
    }
};

void FileCacher::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " filename.data 100,200" << std::endl;

    fdb5::FDBTool::usage(tool);
}

void FileCacher::execute(const eckit::option::CmdArgs &args) {
    // This tool should do the following:
    // 1. Take a filepath as input.
    //    Also take a list of offsets, corresponding to the start of each field in the file.
    // 2. Create a map of filename+offset : GribInfos for each field in the file.
    // 3. Write the GribInfos to disk, with a filename based on the input filename.

    eckit::PathName fdbfilename(args(0));
    std::vector<std::string> offsets = eckit::StringTools::split(",", args(1));

    std::map<std::string, JumpInfo>infos;
    int nfields = 0;

    eckit::DataHandle* handle = fdbfilename.fileHandle();
    JumpHandle dataSource(handle);

    for (const auto& offset : offsets) {
        size_t off = std::stoll(offset);
        dataSource.seek(off);
        JumpInfo info = dataSource.extractInfo();
        std::string key = fdbfilename.baseName() + "." + offset;
        infos[key] = info;
        nfields++;
    }
    eckit::PathName outdir = args.getString("outdir", ".");

    const std::string infosfilename = fdbfilename.baseName() + ".gj";
    const eckit::PathName cachepath = outdir/infosfilename;
    eckit::FileStream s(cachepath, "w");
    s << infos;
    s.close();

    eckit::Log::debug<LibGribJump>() << "Generated GribInfo for " << nfields << " new fields in " << cachepath << std::endl;

    ASSERT(nfields == offsets.size());

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump


int main(int argc, char** argv) {
    gribjump::FileCacher app(argc, argv);
    app.start();
    return 0;
}