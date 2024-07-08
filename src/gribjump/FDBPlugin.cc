
/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */


#include "eckit/io/MemoryHandle.h"
#include "eckit/message/Message.h"
#include "eckit/message/Reader.h"
#include "eckit/config/Resource.h"
#include "eckit/config/YAMLConfiguration.h"

#include "gribjump/LibGribJump.h"
#include "gribjump/FDBPlugin.h"

using namespace fdb5;

namespace gribjump {

// In principle, we could make this non-static, which would make multiple FDB instances possible. But then who owns it?
FDBPlugin& FDBPlugin::instance(FDB& fdb) { 
    static FDBPlugin instance_(fdb);
    return instance_;
}

FDBPlugin::FDBPlugin(FDB& fdb) : fdb_(fdb) {

    parseConfig(eckit::Resource<eckit::PathName>("gribjumpFdbConfigFile;$GRIBJUMP_FDB_CONFIG_FILE", ""));

    fdb_.registerCallback([this](const fdb5::Key& key, const fdb5::FieldLocation& location) {
        if (!matches(key)) return;
        
        LOG_DEBUG_LIB(LibGribJump) << "Archive callback for key " << key << std::endl;
        aggregator_.add(key, location.fullUri());
    });

    fdb_.registerCallback([this](const fdb5::Key& key, const void* data, size_t length) {
        if (!matches(key)) return;

        LOG_DEBUG_LIB(LibGribJump) << "Post-archive callback for key " << key << std::endl;
        
        /* Can we assume at this stage that "data" is a GRIB? We are not explicitly checking this. */
        eckit::MemoryHandle handle(data, length);
        aggregator_.add(key, handle);
    });

    fdb_.registerCallback([this]() {
        LOG_DEBUG_LIB(LibGribJump) << "Flush callback" << std::endl;
        aggregator_.flush();
    });
}

void FDBPlugin::parseConfig(eckit::PathName path){
    
    if (!path.exists()) {
        std::stringstream ss;
        ss << "Config file " << path << " does not exist" << std::endl;
        throw eckit::UserError(ss.str(), Here());
    }

    LOG_DEBUG_LIB(LibGribJump) << "Parsing config file " << path << std::endl;

    eckit::YAMLConfiguration config(path);

    std::string select = config.getString("select", "");

    std::vector<std::string> select_key_values;
    eckit::Tokenizer(',')(select, select_key_values);
    eckit::Tokenizer equalsTokenizer('=');

    for (const std::string& key_value : select_key_values) {
        std::vector<std::string> kv;
        equalsTokenizer(key_value, kv);

        if (kv.size() != 2 || selectDict_.find(kv[0]) != selectDict_.end()) {
            std::stringstream ss;
            ss << "Invalid select condition in gribjump-fdb config: " << select << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }

        selectDict_[kv[0]] = eckit::Regex(kv[1]);
    }

    if (LibGribJump::instance().debug()){
        LOG_DEBUG_LIB(LibGribJump) << "Select dictionary:" << std::endl;
        for (const auto& kv : selectDict_) {
            LOG_DEBUG_LIB(LibGribJump) << "    " << kv.first << " => " << kv.second << std::endl;
        }
    }

}

// Check if selectDict matches key
bool FDBPlugin::matches(const fdb5::Key& key) const {

    for (const auto& kv : selectDict_) {
        std::string value = key.get(kv.first);
        eckit::Regex regex(kv.second);
        if (!regex.match(value)) return false;
    }

    return true;
}

FDBPlugin::~FDBPlugin() {
}

} // namespace gribjump
