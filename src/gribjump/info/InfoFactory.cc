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


#include "metkit/codes/GribAccessor.h"
#include "eckit/io/DataHandle.h"

#include "gribjump/info/InfoFactory.h"

static metkit::grib::GribAccessor<std::string>   packingType("packingType");

namespace gribjump {

// --------------------------------------------------------------------------------------------

InfoBuilderBase::InfoBuilderBase(const std::string& name) : name_(name) {
    InfoFactory::instance().enregister(name, this);
}

InfoBuilderBase::~InfoBuilderBase() {
    InfoFactory::instance().deregister(name_);
}

// --------------------------------------------------------------------------------------------

InfoFactory::InfoFactory() {}

InfoFactory::~InfoFactory() {}

InfoFactory& InfoFactory::instance() {
    static InfoFactory instance;
    return instance;
}

std::unique_ptr<JumpInfo> InfoFactory::build(eckit::DataHandle& h, const eckit::Offset& msgOffset) {
    
    metkit::grib::GribHandle gh(h, msgOffset); // Note: eccodes will read message into memory

    InfoBuilderBase* builder = get(packingType(gh));

    if (!builder) return nullptr;

    return builder->make(h, gh, msgOffset);
}

std::unique_ptr<JumpInfo> InfoFactory::build(const eckit::message::Message& msg) {
    
    InfoBuilderBase* builder = get(msg.getString("packingType"));

    if (!builder) return nullptr;

    return builder->make(msg);
}

void InfoFactory::enregister(const std::string& name, InfoBuilderBase* builder) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (builders_.find(name) != builders_.end()) {
        throw eckit::SeriousBug("Duplicate entry in InfoFactory: " + name, Here());
    }

    builders_[name] = builder;
}

void InfoFactory::deregister(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = builders_.find(name);
    if (it == builders_.end()) {
        throw eckit::SeriousBug("No entry in InfoFactory: " + name, Here());
    }

    builders_.erase(it);
}

InfoBuilderBase* InfoFactory::get(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = builders_.find(name);

    if (it == builders_.end()) {
        // Not found, build the unsupported info.
        it = builders_.find("unsupported");
        ASSERT(it != builders_.end());
    }

    return it->second;
}

// --------------------------------------------------------------------------------------------

}  // namespace gribjump