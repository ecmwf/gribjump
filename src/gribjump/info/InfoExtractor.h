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

#pragma once

#include "eckit/filesystem/PathName.h"
#include "eckit/message/Message.h"
#include "gribjump/info/JumpInfo.h"

namespace gribjump {

class InfoExtractor {

public:
    InfoExtractor();
    ~InfoExtractor();


    std::vector<JumpInfo*> extract(const eckit::PathName& path);
    std::vector<JumpInfo*> extract(const eckit::PathName& path, const std::vector<eckit::Offset>& offsets);
    JumpInfo* extract(const eckit::PathName& path, const eckit::Offset& offset);

    JumpInfo* extract(const eckit::message::Message& msg, bool anyPacking=false);
};

}  // namespace gribjump
