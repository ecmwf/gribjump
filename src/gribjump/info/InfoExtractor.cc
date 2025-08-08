/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Christopher Bradley

#include "gribjump/info/InfoExtractor.h"

#include "gribjump/info/InfoFactory.h"
#include "gribjump/info/JumpInfo.h"

#include "eccodes.h"
#include "eckit/config/Resource.h"
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/Offset.h"
#include "eckit/message/Message.h"

#include <cstddef>
#include <cstdlib>

namespace gribjump {

namespace {

//@note: Only used when scanning corrupted files (where we cannot rely on eccodes)
eckit::OffsetList findGRIBOffsets(const std::string& filepath) {
    const std::string pattern = "GRIB";
    const size_t plen         = pattern.size();
    const size_t BUFFER_SIZE  = 1024 * 1024;
    const size_t OVERLAP      = plen - 1;

    eckit::OffsetList offsets;
    std::vector<char> buffer(BUFFER_SIZE + OVERLAP);
    std::vector<char> carryover(OVERLAP, 0);
    eckit::FileHandle file(filepath);
    file.openForRead();

    size_t filePos = 0;
    while (true) {

        std::memcpy(buffer.data(), carryover.data(), OVERLAP);
        size_t bytesRead = file.read(buffer.data() + OVERLAP, BUFFER_SIZE);
        if (bytesRead < 0) {
            throw eckit::SeriousBug("Error reading file: " + filepath);
        }
        if (bytesRead == 0) {
            break;  // EOF
        }

        // Search for GRIB in the current buffer
        for (size_t i = 0; i <= bytesRead + OVERLAP - plen; ++i) {
            if (std::memcmp(&buffer[i], pattern.data(), plen) == 0) {
                offsets.push_back(filePos + i - OVERLAP);
            }
        }

        filePos += bytesRead;
        if (bytesRead >= OVERLAP) {
            std::memcpy(carryover.data(), &buffer[bytesRead], OVERLAP);
        }
        else {
            break;
        }
    }

    // Remove the last offset, which we assume is an incomplete GRIB message
    if (!offsets.empty()) {
        offsets.pop_back();
    }

    return offsets;
}
}  // namespace


InfoExtractor::InfoExtractor() {}

InfoExtractor::~InfoExtractor() {}

std::vector<std::pair<eckit::Offset, std::unique_ptr<JumpInfo>>> InfoExtractor::extract(
    const eckit::PathName& path) const {

    eckit::OffsetList eckit_offsets = offsets(path);

    std::vector<std::unique_ptr<JumpInfo>> infos = extract(path, eckit_offsets);

    std::vector<std::pair<eckit::Offset, std::unique_ptr<JumpInfo>>> result;

    for (size_t i = 0; i < infos.size(); i++) {
        result.push_back(std::make_pair(eckit_offsets[i], std::move(infos[i])));
    }

    return result;
}

std::vector<std::unique_ptr<JumpInfo>> InfoExtractor::extract(const eckit::PathName& path,
                                                              const std::vector<eckit::Offset>& offsets) const {

    eckit::FileHandle fh(path);
    std::vector<std::unique_ptr<JumpInfo>> infos;
    infos.reserve(offsets.size());

    for (size_t i = 0; i < offsets.size(); i++) {
        fh.openForRead();

        std::unique_ptr<JumpInfo> info(InfoFactory::instance().build(fh, offsets[i]));
        ASSERT(info);
        infos.push_back(std::move(info));

        fh.close();  // Required due to strange metkit/gribhandle behaviour.
    }
    return infos;
}

std::unique_ptr<JumpInfo> InfoExtractor::extract(const eckit::PathName& path, const eckit::Offset& offset) const {

    eckit::FileHandle fh(path);

    fh.openForRead();

    std::unique_ptr<JumpInfo> info(InfoFactory::instance().build(fh, offset));
    ASSERT(info);

    fh.close();

    return info;
}

std::unique_ptr<JumpInfo> InfoExtractor::extract(const eckit::message::Message& msg) const {
    return InfoFactory::instance().build(msg);
}

eckit::OffsetList InfoExtractor::offsets(const eckit::PathName& path) const {
    grib_context* c  = nullptr;
    int n            = 0;
    off_t* offsets_c = nullptr;
    int err          = codes_extract_offsets_malloc(c, path.asString().c_str(), PRODUCT_GRIB, &offsets_c, &n, 1);

    bool scan_corrupted = eckit::Resource<bool>("$GRIBJUMP_SCAN_CORRUPTED", false);

    if (err && scan_corrupted) {
        eckit::Log::warning() << "Error extracting offsets from " << path
                              << ". Attempting workaround for corrupted files." << std::endl;
        free(offsets_c);
        return findGRIBOffsets(path);
    }

    ASSERT(!err);

    // convert to eckit offsets
    eckit::OffsetList eckit_offsets;
    eckit_offsets.reserve(n);
    for (int i = 0; i < n; i++) {
        eckit_offsets.push_back(offsets_c[i]);
    }

    free(offsets_c);
    return eckit_offsets;
}

}  // namespace gribjump
