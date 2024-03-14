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

#include "gribjump/jumper/SimpleJumper.h"
#include "gribjump/jumper/CcsdsJumper.h"
#include "gribjump/jumper/JumperFactory.h"

#include "gribjump/tools/EccodesExtract.h"

#include "gribjump/ExtractionItem.h"


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
        /* Todo: A file with a mix of both in a different test */
    };

    std::vector<std::string> expectedPacking = {
        "grid_simple",
        "grid_ccsds",
    };

    uint32_t count = 0;
    for (auto path : paths) {

        eckit::FileHandle fh(path);
        fh.openForRead();

        eckit::Offset offset = 0;

        std::unique_ptr<NewJumpInfo> info(InfoFactory::instance().build(fh, offset));
        EXPECT(info);
        EXPECT(info->packingType() == expectedPacking[count++]);
        
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
            std::unique_ptr<NewJumpInfo> info_in(eckit::Reanimator<NewJumpInfo>::reanimate(sin));
            EXPECT(*info_in == *info);
        }
        
        if (filename.exists()) filename.unlink();
    }
}

//-----------------------------------------------------------------------------

CASE ("test_jumpers") {

    std::vector<eckit::PathName> paths = {
        "2t_O1280.grib",   // simple packed
        "ceil_O1280.grib", // ccsds
    };

    for (auto path : paths) {
        std::cout << "Path: " << path << std::endl;

        eckit::FileHandle fh(path);
        fh.openForRead();

        eckit::Offset offset = 0;
        std::unique_ptr<NewJumpInfo> info(InfoFactory::instance().build(fh, offset));

        auto intervals = std::vector<Interval>{{0, 10}, {3000000, 3000010}, {6599670, 6599680}};

        std::unique_ptr<Jumper> jumper(JumperFactory::instance().build(*info));
        std::unique_ptr<ExtractionResult> res(jumper->extract(fh, *info, intervals));

        fh.close();

        // Check correct values 
        std::vector<std::vector<double>> comparisonValues = eccodesExtract(path, {offset}, intervals)[0];
        EXPECT(comparisonValues.size() == 3);

        for (size_t i = 0; i < comparisonValues.size(); i++) {
            EXPECT(comparisonValues[i].size() == 10);
            for (size_t j = 0; j < comparisonValues[i].size(); j++) {
                EXPECT(comparisonValues[i][j] == res->values()[i][j]);
            }
        }
    }
}

CASE ("test_wrong_jumper") {
    // Negative test: intentionally use the wrong jumper, make sure it throws correctly

    {
         // simple packed grib, use ccsds jumper
        eckit::PathName path = "2t_O1280.grib";

        eckit::FileHandle fh(path);
        fh.openForRead();

        eckit::Offset offset = 0;
        std::unique_ptr<NewJumpInfo> info(InfoFactory::instance().build(fh, offset));

        auto intervals = std::vector<Interval>{{0, 10}, {10, 20}, {20, 30}};

        std::unique_ptr<Jumper> jumper(new CcsdsJumper());

        try {
            std::unique_ptr<ExtractionResult> res(jumper->extract(fh, *info, intervals));
            EXPECT(false);
        } catch (BadJumpInfoException& e) {
            // As expected!
        }

        fh.close();
    }

    {
        // ccsds packed grib, use simple jumper
        eckit::PathName path = "ceil_O1280.grib"; 

        eckit::FileHandle fh(path);
        fh.openForRead();

        eckit::Offset offset = 0;
        std::unique_ptr<NewJumpInfo> info(InfoFactory::instance().build(fh, offset));  // make this use jumphandle?

        auto intervals = std::vector<Interval>{{0, 10}, {10, 20}, {20, 30}};

        std::unique_ptr<Jumper> jumper(new SimpleJumper());

        try {
            std::unique_ptr<ExtractionResult> res(jumper->extract(fh, *info, intervals));
            EXPECT(false);
        } catch (BadJumpInfoException& e) {
            // As expected!
        }

        fh.close();
    }
}
//-----------------------------------------------------------------------------
// Testing the extract functionality using ExtractionItem
// ~ i.e. internals of FileExtractionTask
CASE ("test_ExtractionItem_extract") {
    metkit::mars::MarsRequest request("none");
    auto intervals = std::vector<Interval>{{0, 10}, {3000000, 3000010}, {6599670, 6599680}};
    ExtractionItem exItem(request, intervals );

    eckit::PathName path = "2t_O1280.grib";

    std::cout << "Setting URI" << std::endl;
    exItem.URI(new eckit::URI(path));

    eckit::FileHandle fh(path);
    fh.openForRead();

    eckit::Offset offset = 0;

    std::cout << "Building info" << std::endl;
    std::unique_ptr<NewJumpInfo> info(InfoFactory::instance().build(fh, offset));
    EXPECT(info);

    std::unique_ptr<Jumper> jumper(JumperFactory::instance().build(*info));

    std::cout << "Extracting: " << exItem.intervals() << std::endl;
    jumper->extract(fh, *info, exItem);

    exItem.debug_print();
    
    // Check correct values 
    std::vector<std::vector<double>> comparisonValues = eccodesExtract(path, {offset}, intervals)[0];
    EXPECT(comparisonValues.size() == 3);

    for (size_t i = 0; i < comparisonValues.size(); i++) {
        EXPECT(comparisonValues[i].size() == 10);
        for (size_t j = 0; j < comparisonValues[i].size(); j++) {
            EXPECT(comparisonValues[i][j] == exItem.values()[i][j]);
        }
    }
    
    std::cout << "end" << std::endl;
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

    std::vector<std::unique_ptr<NewJumpInfo>> infos;

    for (size_t i = 0; i < n; i++) {
        
        std::unique_ptr<NewJumpInfo> info(InfoFactory::instance().build(fh, offsets[i]));
        ASSERT(info);
        infos.push_back(std::move(info));
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
        std::unique_ptr<NewJumpInfo> t2(eckit::Reanimator<NewJumpInfo>::reanimate(sin));

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
