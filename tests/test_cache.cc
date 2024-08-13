/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#include <cmath>

#include "eckit/testing/Test.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/filesystem/TmpDir.h"

#include "gribjump/GribJump.h"
#include "gribjump/info/InfoCache.h"
#include "gribjump/info/JumpInfo.h"
#include "gribjump/info/InfoExtractor.h"

#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsExpension.h"
using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------

CASE( "test_cache" ){

    // --------------------------------------------------------------------------------------------

    // prep

    std::string s = eckit::LocalPathName::cwd();

    eckit::TmpDir tmpdir(s.c_str());
    tmpdir.mkdir();
    eckit::testing::SetEnv env("GRIBJUMP_CACHE_DIR", tmpdir.asString().c_str());

    eckit::PathName path = "extract_ranges.grib"; // TODO, use a file with mixed ccsds and simple grids
    InfoExtractor extractor;

    std::vector<std::pair<eckit::Offset, std::unique_ptr<JumpInfo>>>  offsetInfos = extractor.extract(path);

    // --------------------------------------------------------------------------------------------
    
    // pre-populate the cache
    for (size_t i = 0; i < offsetInfos.size(); i++) {
        std::unique_ptr<JumpInfo> info(extractor.extract(path, offsetInfos[i].first));
        InfoCache::instance().insert(path, offsetInfos[i].first, std::move(info));
    }
    
    // Test 1: getting fields from cache.
    
    for (size_t i = 0; i < offsetInfos.size(); i++) {
        std::shared_ptr<JumpInfo> info = InfoCache::instance().get(path, offsetInfos[i].first);
        EXPECT(info);
        EXPECT(*info == *offsetInfos[i].second);
    }

    InfoCache::instance().flush(false);
    InfoCache::instance().clear();

    // Test 2: Get fields, reanimated from disk.

    for (size_t i = 0; i < offsetInfos.size(); i++) {
        std::shared_ptr<JumpInfo> info = InfoCache::instance().get(path, offsetInfos[i].first);
        EXPECT(info);
        EXPECT(*info == *offsetInfos[i].second);
    }

}
//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump


int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
