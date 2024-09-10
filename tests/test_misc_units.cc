/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#include <cmath>
#include <fstream>

#include "eckit/testing/Test.h"
#include "gribjump/info/LRUCache.h"


#include "metkit/mars/MarsParser.h"
#include "metkit/mars/MarsExpension.h"
using namespace eckit::testing;

namespace gribjump {
namespace test {

// Miscellanous unit tests
//-----------------------------------------------------------------------------
CASE( "test_lru" ){
    
    LRUCache<std::string, int> cache(3);

    // Check basic functionality
    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);

    EXPECT(cache.get("a") == 1);
    EXPECT(cache.get("b") == 2);
    EXPECT(cache.get("c") == 3);

    cache.put("d", 4);

    EXPECT_THROWS_AS(cache.get("a"), eckit::BadValue);
    EXPECT(cache.get("d") == 4);

    // Check recency is updated with get
    cache.put("x", 1);
    cache.put("y", 2);
    cache.put("z", 3);

    EXPECT(cache.get("z") == 3);
    EXPECT(cache.get("y") == 2);
    EXPECT(cache.get("x") == 1);

    // z should now be the least recently used
    cache.put("w", 1);

    EXPECT_THROWS_AS(cache.get("z"), eckit::BadValue);
}
//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump


int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
