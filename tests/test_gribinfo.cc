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

#include "eccodes.h"

#include "eckit/io/AutoCloser.h"
#include "eckit/serialisation/MemoryStream.h"
#include "eckit/testing/Test.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/DataHandle.h"
#include "eckit/io/FileHandle.h"
#include "eckit/serialisation/FileStream.h"

#include "metkit/codes/CodesContent.h"
#include "metkit/codes/GribHandle.h"

#include "gribjump/Engine.h"
#include "gribjump/ExtractionItem.h"
#include "gribjump/LibGribJump.h"
#include "gribjump/info/InfoFactory.h"
#include "gribjump/jumper/CcsdsJumper.h"
#include "gribjump/jumper/JumperFactory.h"
#include "gribjump/jumper/SimpleJumper.h"
#include "gribjump/tools/EccodesExtract.h"

using namespace eckit::testing;

namespace gribjump {
namespace test {

//-----------------------------------------------------------------------------

CASE("test_reanimate_info") {

    // Extract jumpinfos from a grib file, write to disk, reanimate and compare

    // TODO! Add spectral.grib to nexus and to cmake!

    std::vector<eckit::PathName> paths = {
        "2t_O1280.grib",    // simple packed
        "ceil_O1280.grib",  // ccsds
        // "spectral.grib", // NB: Should build an UnsupportedInfo
        /* Todo: A file with a mix of both in a different test */
    };

    std::vector<std::string> expectedPacking = {
        "grid_simple", "grid_ccsds",
        // "spectral_complex",
    };

    uint32_t count = 0;
    for (auto path : paths) {

        eckit::FileHandle fh(path);
        fh.openForRead();

        eckit::Offset offset = 0;

        std::unique_ptr<JumpInfo> info(InfoFactory::instance().build(fh, offset));
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
            auto c = eckit::closer(sin);
            std::unique_ptr<JumpInfo> info_in(eckit::Reanimator<JumpInfo>::reanimate(sin));
            EXPECT(*info_in == *info);
        }

        if (filename.exists())
            filename.unlink();
    }
}

//-----------------------------------------------------------------------------

CASE("test_build_from_message") {

    // Build from an eckit message, compare with the same info built from a file

    std::vector<eckit::PathName> paths = {
        "2t_O1280.grib",    // simple packed
        "ceil_O1280.grib",  // ccsds
    };
    for (auto path : paths) {

        // Make the message.
        eckit::AutoStdFile f(path);
        int err         = 0;
        codes_handle* h = codes_handle_new_from_file(nullptr, f, PRODUCT_GRIB, &err);
        EXPECT(err == 0);
        metkit::codes::CodesContent* content = new metkit::codes::CodesContent(h, true);
        eckit::message::Message msg(content);

        std::unique_ptr<JumpInfo> infoFromMessage(InfoFactory::instance().build(msg));


        eckit::FileHandle fh(path);
        fh.openForRead();

        eckit::Offset offset = 0;

        std::unique_ptr<JumpInfo> infoFromFile(InfoFactory::instance().build(fh, offset));

        std::cout << "from Message: " << *infoFromMessage << std::endl;
        std::cout << "from File: " << *infoFromFile << std::endl;

        EXPECT(*infoFromMessage == *infoFromFile);

        fh.close();
    }
}
//-----------------------------------------------------------------------------
const size_t O1280_size = 6599680;  // O1280

CASE("test_jumpers_filehandle") {

    std::vector<eckit::PathName> paths = {
        "2t_O1280.grib",    // simple packed
        "ceil_O1280.grib",  // ccsds
    };

    for (auto path : paths) {
        eckit::FileHandle fh(path);
        fh.openForRead();

        eckit::Offset offset = 0;
        std::unique_ptr<JumpInfo> info(InfoFactory::instance().build(fh, offset));

        EXPECT(info->numberOfDataPoints() == O1280_size);

        auto intervals = std::vector<Interval>{{0, 10}, {3000000, 3000010}, {6599670, 6599680}};

        std::unique_ptr<Jumper> jumper(JumperFactory::instance().build(*info));
        ExtractionItem extractionItem(intervals);
        jumper->extract(fh, offset, *info, extractionItem);

        fh.close();

        // Check correct values
        std::vector<std::vector<double>> comparisonValues = eccodesExtract(path, {offset}, intervals)[0];
        std::vector<std::vector<double>> values           = extractionItem.values();
        EXPECT(comparisonValues.size() == 3);
        EXPECT(values.size() == comparisonValues.size());

        for (size_t i = 0; i < comparisonValues.size(); i++) {
            EXPECT(comparisonValues[i].size() == 10);
            EXPECT(comparisonValues[i].size() == values[i].size());
            for (size_t j = 0; j < comparisonValues[i].size(); j++) {
                EXPECT(comparisonValues[i][j] == extractionItem.values()[i][j]);
            }
        }
    }
}

CASE("test_wrong_jumper") {
    // Negative test: intentionally use the wrong jumper, make sure it throws correctly

    {
        // simple packed grib, use ccsds jumper
        eckit::PathName path = "2t_O1280.grib";

        eckit::FileHandle fh(path);
        fh.openForRead();

        eckit::Offset offset = 0;
        std::unique_ptr<JumpInfo> info(InfoFactory::instance().build(fh, offset));

        auto intervals = std::vector<Interval>{{0, 10}, {10, 20}, {20, 30}};

        std::unique_ptr<Jumper> jumper(new CcsdsJumper());

        try {
            ExtractionItem item(intervals);
            jumper->extract(fh, offset, *info, item);
            EXPECT(false);  // Reaching here is an error!
        }
        catch (BadJumpInfoException& e) {
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
        std::unique_ptr<JumpInfo> info(InfoFactory::instance().build(fh, offset));

        auto intervals = std::vector<Interval>{{0, 10}, {10, 20}, {20, 30}};

        std::unique_ptr<Jumper> jumper(new SimpleJumper());

        try {
            ExtractionItem item(intervals);
            jumper->extract(fh, offset, *info, item);
            EXPECT(false);
        }
        catch (BadJumpInfoException& e) {
            // As expected!
        }

        fh.close();
    }
}
//-----------------------------------------------------------------------------
// Testing the extract functionality using ExtractionItem
// ~ i.e. internals of FileExtractionTask
CASE("test_ExtractionItem_extract") {
    auto intervals = std::vector<Interval>{{0, 10}, {3000000, 3000010}, {6599670, 6599680}};
    ExtractionItem exItem(intervals);

    eckit::PathName path = "2t_O1280.grib";

    exItem.URI(eckit::URI(path));

    eckit::FileHandle fh(path);
    fh.openForRead();

    eckit::Offset offset = 0;

    std::unique_ptr<JumpInfo> info(InfoFactory::instance().build(fh, offset));
    EXPECT(info);

    std::unique_ptr<Jumper> jumper(JumperFactory::instance().build(*info));

    jumper->extract(fh, offset, *info, exItem);

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
}

//-----------------------------------------------------------------------------

}  // namespace test
}  // namespace gribjump


int main(int argc, char** argv) {
    return run_tests(argc, argv);
}
