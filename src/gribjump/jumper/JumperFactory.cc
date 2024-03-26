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

#include "gribjump/jumper/JumperFactory.h"

namespace gribjump {

// --------------------------------------------------------------------------------------------

JumperBuilderBase::JumperBuilderBase(const std::string& name) : name_(name) {
    JumperFactory::instance().enregister(name, this);
}

JumperBuilderBase::~JumperBuilderBase() {
    JumperFactory::instance().deregister(name_);
}

// --------------------------------------------------------------------------------------------

JumperFactory::JumperFactory() {}

JumperFactory::~JumperFactory() {}

JumperFactory& JumperFactory::instance() {
    static JumperFactory instance;
    return instance;
}

Jumper* JumperFactory::build(const JumpInfo& info) {
    return build(info.packingType());
}

Jumper* JumperFactory::build(const std::string packingType) {
    JumperBuilderBase* builder = get(packingType);
    return builder->make();
}

void JumperFactory::enregister(const std::string& name, JumperBuilderBase* builder) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (builders_.find(name) != builders_.end()) {
        throw eckit::SeriousBug("Duplicate entry in JumperFactory: " + name, Here());
    }

    builders_[name] = builder;
}

void JumperFactory::deregister(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = builders_.find(name);
    if (it == builders_.end()) {
        throw eckit::SeriousBug("No entry in JumperFactory: " + name, Here());
    }

    builders_.erase(it);
}

JumperBuilderBase* JumperFactory::get(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = builders_.find(name);
    if (it == builders_.end()) {
        throw eckit::SeriousBug("No entry in JumperFactory: " + name, Here());
    }

    return it->second;
}

// --------------------------------------------------------------------------------------------

} // namespace gribjump