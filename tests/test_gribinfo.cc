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
#include <fstream>

#include "eckit/testing/Test.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/io/AutoCloser.h"

#include "gribjump/GribInfo.h"
#include "gribjump/JumpHandle.h"


#include "eccodes.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/DataHandle.h"
#include "eckit/serialisation/FileStream.h"

#include "metkit/codes/GribHandle.h"

#include "gribjump/info/JumpInfoFactory.h"

#include "gribjump/LibGribJump.h"

using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------

CASE( "test_reanimate_info" ) {
    
    // Extract jumpinfos from a grib file, write to disk, reanimate and compare

    std::vector<eckit::PathName> paths = {
        "2t_O1280.grib",   // simple packed
        "ceil_O1280.grib", // ccsds
    };

    for (auto path : paths) {

        eckit::FileHandle fh(path);
        fh.openForRead();

        // Info* info =  InfoFactory::create(fh, 0);
        Info* info = InfoFactory::instance().build(fh, 0);
        EXPECT(info);
        fh.close();

        // Write to file
        eckit::PathName filename = "xyx";
        {
            eckit::FileStream sout(filename.asString().c_str(), "w");
            auto c = eckit::closer(sout);
            sout << *info;
        }

        // Reanimate
        {
            eckit::FileStream sin(filename.asString().c_str(), "r");
            auto c             = eckit::closer(sin);
            Info* info_in = eckit::Reanimator<Info>::reanimate(sin);

            EXPECT(*info_in == *info);
        }
        
        if (filename.exists()) filename.unlink();
    }
}

//-----------------------------------------------------------------------------

CASE ("test_combine") {

    // XXX: Note, not a test, just here to see how to generate multiple messages in one file
    // remove when this is reimplemented.

    eckit::PathName path = "combine.grib";

    grib_context* c = nullptr;
    int n = 0;
    off_t* offsets;
    int err = codes_extract_offsets_malloc(c, path.asString().c_str(), PRODUCT_GRIB, &offsets, &n, 1);
    ASSERT(!err);

    eckit::FileHandle fh(path);
    fh.openForRead();

    std::vector<Info*> infos;

    for (size_t i = 0; i < n; i++) {
        
        Info* info = InfoFactory::instance().build(fh, 0);
        ASSERT(info);
        infos.push_back(info);
    }

    fh.close();

    free(offsets);

    eckit::PathName filename = "xyx";
    std::string filepath = filename.asString();
    {
        eckit::FileStream sout(filepath.c_str(), "w");
        auto c = eckit::closer(sout);
        sout << *infos[0];
    }
    {
        eckit::FileStream sin(filepath.c_str(), "r");
        auto c             = eckit::closer(sin);
        Info* t2 = eckit::Reanimator<Info>::reanimate(sin);

        ASSERT(*t2 == *infos[0]);
    }
    
    if (filename.exists()) filename.unlink();
}
//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump


int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
