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


#include "fdb5/api/FDB.h"
#include "fdb5/api/helpers/FDBToolRequest.h"

#include "gribjump/GribHandleData.h"
#include "gribjump/GribInfoCache.h"
#include "gribjump/LibGribJump.h"

namespace gribjump {


class CacheTool : public eckit::Application {
public:
    CacheTool(int argc, char** argv) : Application(argc, argv) {
    }
    ~CacheTool() {}

private:
    CacheTool(const CacheTool&);

    virtual void run() override {
        // This tool should do the following:
        // 1. Use fdb list to get all fieldlocations of levtype=sfc
        // 2. For each fieldlocation, get the gribinfo
        // 3. Add each gribinfo to the cache object
        // 4. Write the cache object to disk
        if (!getenv("GRIBJUMP_CACHE_DIR")) {
            eckit::Log::error() << "GRIBJUMP_CACHE_DIR not set." << std::endl;
            return;
        }

        eckit::PathName cacheDir(getenv("GRIBJUMP_CACHE_DIR"));
        if (!cacheDir.exists()) {
            // create it
            cacheDir.mkdir();
        }


        fdb5::FDB fdb;
        // hardcode yesterday's operational forecast, 1200, sfc fields for now (Tiago's Demo.)
        std::vector<fdb5::FDBToolRequest> x = fdb5::FDBToolRequest::requestsFromString("levtype=sfc,time=1200,class=od,expver=0001,stream=oper,date=-2");

        ASSERT(x.size() == 1);

        fdb5::ListIterator it = fdb.list(x[0]);
        fdb5::ListElement el;
        // std::map<std::string, GribInfoCache> caches;
        std::map<std::string, std::map<std::string, JumpInfo>> caches;
        int nfields = 0;
        while (it.next(el)) {
            const fdb5::FieldLocation& loc = el.location();
            eckit::DataHandle* handle = loc.dataHandle();
            JumpHandle dataSource(handle);
            JumpInfo info = dataSource.extractInfo();

            // Fields in the same file share a cache on disk.
            std::string fdbfilename = loc.uri().path().baseName();
            std::string offset = std::to_string(loc.offset());
            std::string key = fdbfilename + "." + offset;
            caches[fdbfilename][key] = info;
            nfields++;

        }

        eckit::Log::debug<LibGribJump>() << "Cached " << nfields << " fields in " <<  caches.size() << " files." << std::endl;

        std::map<std::string, std::string> manifest;
        for (const auto& kv : caches) {
            const std::string& fieldfilename = kv.first;
            const std::map<std::string, JumpInfo>& cache = kv.second;
            eckit::PathName cachepath = cacheDir / eckit::PathName(fieldfilename + ".gj");
            eckit::FileStream s(cachepath, "w");
            s << cache;
            s.close();

            manifest[fieldfilename] = cachepath;
        }

        eckit::PathName manifestpath = cacheDir / eckit::PathName("manifest.gj");
        eckit::FileStream s(manifestpath, "w");
        s << manifest;
        s.close();

        // Try reading it back in and creating GribInfoCache
        // GribInfoCache cache(cacheDir);
        // cache.print(std::cout);
        // cache.preload();
        // cache.print(std::cout);

    }
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump


int main(int argc, char** argv) {
    gribjump::CacheTool app(argc, argv);
    app.start();
    return 0;
}