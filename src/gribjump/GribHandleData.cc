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

#include <iomanip>
#include <fstream>

#include "eccodes.h"

#include "eckit/exception/Exceptions.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/io/DataHandle.h"
#include "eckit/serialisation/FileStream.h"

#include "metkit/codes/GribHandle.h"

#include "gribjump/GribHandleData.h"
#include "gribjump/GribInfo.h"

namespace gribjump {

JumpHandle::JumpHandle(const eckit::PathName& path):
    handle_(path.fileHandle()),
    ownsHandle_(true),
    opened_(false),
    path_(path)
    {}

JumpHandle::JumpHandle(eckit::DataHandle *handle):
    handle_(handle),
    ownsHandle_(true),
    opened_(false) {}

JumpHandle::~JumpHandle() {
    if (opened_) handle_->close();
    if (ownsHandle_) delete handle_;
}

void JumpHandle::open() const {
    if (!opened_) {
        handle_->openForRead();
        opened_ = true;
    }
}

void JumpHandle::close() const {
    if (opened_) {
        handle_->close();
        opened_ = false;
    }
}

eckit::Offset JumpHandle::seek(const eckit::Offset& offset) const {
    open();
    eckit::Offset pos = handle_->seek(offset);
    return (long long)pos;
}

long JumpHandle::read(void* buffer, long len) const {
    open();
    return handle_->read(buffer, len);
}

eckit::Offset JumpHandle::position(){
    open();
    return handle_->position();
}

eckit::Length JumpHandle::size(){
    open();
    return handle_->size();
}

std::vector<JumpInfo*> JumpHandle::extractInfoFromFile() {

    ASSERT(path_.asString().size() > 0);

    // count number of messages in file
    grib_context* c = nullptr;
    int n = 0;
    int err = codes_count_in_filename(c, path_.asString().c_str(), &n);
    ASSERT(!err);

    // extract metadata from each message to a binary file
    eckit::Offset offset = 0;
    std::vector<JumpInfo*> infos;
    for (size_t i = 0; i < n; i++) {
        
        open();

        metkit::grib::GribHandle h(*handle_, offset);
        JumpInfo* info = new JumpInfo(h);
        unsigned long fp = handle_->position();
        info->setStartOffset(fp - info->length());
        offset = handle_->position();
        info->updateCcsdsOffsets(*this); // XXX Pretty inelgeant. Honestly all of this is.
        infos.push_back(info);

        // XXX: On linux, fp is wrong if handle is not closed and reopened.
        close();
    }

    return infos;
}

void write_jumpinfos_to_file(const std::vector<JumpInfo*> infos, const eckit::PathName& path) {
    eckit::FileStream out(path, "w");
    size_t nInfo = infos.size();
    out << nInfo;
    for (const auto& info : infos) {
        out << *info;
        eckit::Log::debug() << "Wrote info to file: " << *info << std::endl;
    }
    out.close();    
}

JumpInfo* JumpHandle::extractInfo() {
    // Note: Requires handle at start of message, and will advance handle to end of message.
    open();
    eckit::Offset initialPos = handle_->position();
    metkit::grib::GribHandle h(*handle_, initialPos);

    JumpInfo* info = new JumpInfo(h);
    info->setStartOffset(0);
    info->updateCcsdsOffsets(*this); 
    return info;
}

void JumpHandle::print(std::ostream& s) const {
    s << "JumpHandle[" << *handle_ << "]" << std::endl;
}

//----------------------------------------------------------------------------------------------------------------------
} // namespace gribjump

