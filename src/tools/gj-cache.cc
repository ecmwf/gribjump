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


#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"
#include "fdb5/tools/FDBTool.h"

#include "gribjump/GribHandleData.h"
#include "gribjump/GribInfoCache.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/Config.h"

namespace gribjump {


// TODO Probably doesn't need to be an FDBTool
class CacheTool : public fdb5::FDBTool {
    virtual void execute(const eckit::option::CmdArgs &args);
    virtual void usage(const std::string &tool) const;
    virtual int numberOfPositionalArguments() const { return 1; }
  public:
    CacheTool(int argc, char **argv): fdb5::FDBTool(argc, argv) {
        options_.push_back(new eckit::option::SimpleOption<long>(
            "lifetime", "Clean up cache files older than this many days."));
    }
};

void CacheTool::usage(const std::string &tool) const {
    eckit::Log::info() << std::endl
                       << "Usage: " << tool << " req_str" << std::endl;

    fdb5::FDBTool::usage(tool);
}

void CacheTool::execute(const eckit::option::CmdArgs &args) {
    // This tool should do the following:
    // 1. Use fdb list to get all fieldlocations of levtype=sfc
    // 2. For each fieldlocation, get the gribinfo
    // 3. Add each gribinfo to the cache object
    // 4. Write the cache object to disk

    if (!getenv("GRIBJUMP_CONFIG_FILE")) {
        eckit::Log::error() << "GRIBJUMP_CONFIG_FILE not set." << std::endl;
        return;
    }
    
    eckit::PathName configPath(getenv("GRIBJUMP_CONFIG_FILE"));
    Config config(configPath);

    std::string str;
    config.get("cache", str);
    eckit::PathName cacheDir(str);
    eckit::PathName manifestpath = cacheDir / eckit::PathName("manifest.gj");

    if (!cacheDir.exists()) {
        eckit::Log::error() << "Cache directory " << cacheDir << " does not exist." << std::endl;
        return;
    }
    GribInfoCache gribinfoCache(cacheDir);
    unsigned long lifetime = args.getInt("lifetime", 5);
    gribinfoCache.removeOld(lifetime);
    fdb5::FDB fdb;
    std::string req = args(0);
    std::vector<fdb5::FDBToolRequest> x = fdb5::FDBToolRequest::requestsFromString(req);    

    ASSERT(x.size() == 1);

    fdb5::ListIterator it = fdb.list(x[0]);
    fdb5::ListElement el;
    std::map<std::string, std::map<std::string, JumpInfo>> newInfos;
    int nfields = 0;
    while (it.next(el)) {
        const fdb5::FieldLocation& loc = el.location();
        std::string fdbfilename = loc.uri().path().baseName();

        // check if this file is already in cache
        if (gribinfoCache.lookup(fdbfilename)) {
            continue;
        }

        eckit::DataHandle* handle = loc.dataHandle();
        JumpHandle dataSource(handle);
        JumpInfo info = dataSource.extractInfo();
        std::string offset = std::to_string(loc.offset());
        std::string key = fdbfilename + "." + offset;
        newInfos[fdbfilename][key] = info;
        nfields++;
    }

    // Write GribInfo files to disk, and update manifest
    for (const auto& kv : newInfos) {
        const std::string& fieldfilename = kv.first;
        const std::map<std::string, JumpInfo>& cache = kv.second;
        // const std::string cachefilename = "2023-12-01" + fieldfilename + ".gj";
        const std::string cachefilename = std::string(eckit::TimeStamp("%Y-%m-%d.")) + fieldfilename + ".gj";
        const eckit::PathName cachepath = cacheDir / cachefilename;
        eckit::FileStream s(cachepath, "w");
        s << cache;
        s.close();

        gribinfoCache.append(fieldfilename, cachefilename);
    }

    // persist manifest
    gribinfoCache.dump(); 

    eckit::Log::debug<LibGribJump>() << "Generated GribInfo for " << nfields << " new fields in " <<  newInfos.size() << " files." << std::endl;


    // Debug: Try reading it back in and creating GribInfoCache
    // GribInfoCache cache(cacheDir);
    // cache.print(std::cout);
    // cache.preload();
    // cache.print(std::cout);

}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump


int main(int argc, char** argv) {
    gribjump::CacheTool app(argc, argv);
    app.start();
    return 0;
}