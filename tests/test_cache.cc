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
    std::vector<JumpInfo*> infos = extractor.extract(path);

    std::vector<std::pair<eckit::PathName, eckit::Offset>> locations;
    for (size_t i = 0; i < infos.size(); i++) {
        locations.push_back(std::make_pair(path, infos[i]->msgStartOffset()));
    }

    // --------------------------------------------------------------------------------------------
    
    // pre-populate the cache
    for (size_t i = 0; i < locations.size(); i++) {
        JumpInfo* info = extractor.extract(locations[i].first, locations[i].second);
        InfoCache::instance().insert(locations[i].first, locations[i].second, info);
    }
    
    // Test 1: getting fields from cache.
    
    for (size_t i = 0; i < locations.size(); i++) {
        JumpInfo* info = InfoCache::instance().get(locations[i].first, locations[i].second);
        EXPECT(info);
        EXPECT(*info == *infos[i]);
    }

    InfoCache::instance().persist();
    InfoCache::instance().clear();

    // Test 2: Get fields, reanimated from disk.

    for (size_t i = 0; i < locations.size(); i++) {
        JumpInfo* info = InfoCache::instance().get(locations[i].first, locations[i].second);
        EXPECT(info);
        EXPECT(*info == *infos[i]);
    }

}
//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump


int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
