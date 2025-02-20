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
#include "gribjump/compression/NumericCompressor.h"


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
CASE( "test buckets" ){
    using namespace gribjump::mc;
    BlockBuckets buckets;
    for (size_t i = 100; i < 1000; i += 100) {
        mc::Block r{i, 10};
        buckets << r;
    }

    // i.e. {100, 10}, {200, 10}, ..., {900, 10}
    EXPECT_EQUAL(buckets.size(), 9);

    // Add a bucket that extends {100, 10} forward
    buckets << Block{205, 20};

    // Add a bucket that extends {100, 10} backward
    buckets << Block{195, 10};

    EXPECT_EQUAL(buckets.size(), 9); // We've only grown existing bucket
    EXPECT_EQUAL(buckets[1].second.size(), 3); // The second bucket now has 3 subblocks

    // Check that it has the correct extents. Should be from 195 to 225 i.e. {195, 30}
    EXPECT_EQUAL(buckets[1].first.first, 195);
    EXPECT_EQUAL(buckets[1].first.second, 30);

    // Add a bucket that overlaps with nothing to the beginning, middle and end
    buckets << Block{0, 10} << Block{150, 10} << Block{1500, 10};
    EXPECT_EQUAL(buckets.size(), 12); // We've added 3 new buckets

    // Add a bucket that merges two adjacent buckets
    buckets << Block{305, 100};
    std::cout << buckets << std::endl;
    EXPECT_EQUAL(buckets.size(), 11); // We've merged two buckets

    // Add a bucket that completely overlaps with all buckets
    buckets << Block{0, 2000};
    EXPECT_EQUAL(buckets.size(), 1); // We've merged all buckets
    EXPECT_EQUAL(buckets[0].first.first, 0);
    EXPECT_EQUAL(buckets[0].first.second, 2000);
}

}  // namespace test
}  // namespace gribjump


int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
