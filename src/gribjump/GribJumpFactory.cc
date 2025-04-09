/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "eckit/exception/Exceptions.h"
#include "eckit/thread/AutoLock.h"

#include "gribjump/GribJumpFactory.h"
#include "gribjump/LibGribJump.h"


namespace gribjump {

static eckit::Mutex* local_mutex                  = 0;
static std::map<std::string, GribJumpFactory*>* m = 0;
static pthread_once_t once                        = PTHREAD_ONCE_INIT;

static void init() {
    local_mutex = new eckit::Mutex();
    m           = new std::map<std::string, GribJumpFactory*>();
}


GribJumpFactory::GribJumpFactory(const std::string& name) : name_(name) {
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    ASSERT(m->find(name) == m->end());  // check name is not already registered
    (*m)[name] = this;
}

GribJumpFactory::~GribJumpFactory() {
    // if(LibGribJump::instance().dontDeregisterFactories()) return; // TODO...?
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);
    m->erase(name_);
}

GribJumpBase* GribJumpFactory::build(const Config& config) {

    std::string name = config.getString("type", "local");
    pthread_once(&once, init);
    eckit::AutoLock<eckit::Mutex> lock(local_mutex);

    std::map<std::string, GribJumpFactory*>::const_iterator j = m->find(name);

    if (j == m->end()) {
        eckit::Log::error() << "No GribJumpFactory for [" << name << "]" << std::endl;
        eckit::Log::error() << "names are:" << std::endl;
        for (j = m->begin(); j != m->end(); ++j)
            eckit::Log::error() << "   " << (*j).first << std::endl;
        throw eckit::SeriousBug(std::string("No GribJumpFactory called ") + name);
    }

    return (*j).second->make(config);
}

}  // namespace gribjump
