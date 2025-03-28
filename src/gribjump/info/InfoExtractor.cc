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
#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/FileHandle.h"
#include "eckit/io/Offset.h"
#include "eckit/message/Message.h"

#include <cstddef>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>

namespace gribjump {

InfoExtractor::InfoExtractor() {}

InfoExtractor::~InfoExtractor() {}

std::vector<std::pair<eckit::Offset, std::unique_ptr<JumpInfo>>> InfoExtractor::extract(const eckit::PathName& path) {

    grib_context* c = nullptr;
    int n           = 0;
    off_t* offsets_c;
    int err = codes_extract_offsets_malloc(c, path.asString().c_str(), PRODUCT_GRIB, &offsets_c, &n, 1);
    ASSERT(!err);

    // convert to eckit offsets
    std::vector<eckit::Offset> eckit_offsets;
    eckit_offsets.reserve(n);
    for (int i = 0; i < n; i++) {
        eckit_offsets.push_back(offsets_c[i]);
    }

    free(offsets_c);

    std::vector<std::unique_ptr<JumpInfo>> infos = extract(path, eckit_offsets);

    std::vector<std::pair<eckit::Offset, std::unique_ptr<JumpInfo>>> result;

    for (size_t i = 0; i < infos.size(); i++) {
        result.push_back(std::make_pair(eckit_offsets[i], std::move(infos[i])));
    }

    return result;
}

std::vector<std::unique_ptr<JumpInfo>> InfoExtractor::extract(const eckit::PathName& path,
                                                              const std::vector<eckit::Offset>& offsets) {

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

std::unique_ptr<JumpInfo> InfoExtractor::extract(const eckit::PathName& path, const eckit::Offset& offset) {

    eckit::FileHandle fh(path);

    fh.openForRead();

    std::unique_ptr<JumpInfo> info(InfoFactory::instance().build(fh, offset));
    ASSERT(info);

    fh.close();

    return info;
}

std::unique_ptr<JumpInfo> InfoExtractor::extract(const eckit::message::Message& msg) {
    return InfoFactory::instance().build(msg);
}

}  // namespace gribjump
