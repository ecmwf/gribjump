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

#include "fdb5/LibFdb5.h"

#include "gribjump/LibGribJump.h"
#include "gribjump/FDBPlugin.h"

using namespace fdb5;

namespace gribjump {

namespace {
    // Register the constructor callback immediately.
    FDBPlugin& pluginInstance = FDBPlugin::instance();
}

FDBPlugin& FDBPlugin::instance() { 
    static FDBPlugin instance_;
    return instance_;
}

FDBPlugin::FDBPlugin() {
    // NB: Can't access eckit::Resource outside the callback because eckit::main has not finished initialising
    fdb5::LibFdb5::instance().registerConstructorCallback([](fdb5::CallbackRegistry& fdb) {
        static bool enableGribjump = eckit::Resource<bool>("fdbEnableGribjump;$FDB_ENABLE_GRIBJUMP", false); 
        static bool disableGribjump = eckit::Resource<bool>("fdbDisableGribjump;$FDB_DISABLE_GRIBJUMP", false); // Emergency off-switch
        if (enableGribjump && !disableGribjump) {
            FDBPlugin::instance().addFDB(fdb);
        }
    });
}

void FDBPlugin::addFDB(fdb5::CallbackRegistry& fdb) {

    parseConfig();
    std::lock_guard<std::mutex> lock(mutex_);

    aggregators_.emplace_back(std::make_unique<std::optional<InfoAggregator>>());
    std::optional<InfoAggregator>& aggregator = *aggregators_.back();

    fdb.registerArchiveCallback([this, &aggregator](const fdb5::Key& key, const void* data, const size_t length, std::future<std::shared_ptr<const FieldLocation>> future)  {
        if (!matches(key) || length < 4)
            return;

        LOG_DEBUG_LIB(LibGribJump) << "archive callback for selected key " << key << std::endl;

        if (!aggregator) {
            aggregator.emplace();
        }
        eckit::MemoryHandle handle(data, length);
        aggregator->add(std::move(future), handle, 0);
    });


    fdb.registerFlushCallback([&aggregator]() {
        if (!aggregator) return; // It's possible that no keys ever matched, so the aggregator was never created.
        LOG_DEBUG_LIB(LibGribJump) << "Flush callback" << std::endl;
        aggregator->flush();
        aggregator.reset();
    });

}

// TODO: Look also at the multio select functionality, which is more complete.
// Which can specify match and exclusions, for instance. Which is probably nicer.
void FDBPlugin::parseConfig() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (configParsed_) return;
    configParsed_ = true;

    const Config& config = LibGribJump::instance().config();

    std::string select = config.getString("plugin.select", "");

    std::vector<std::string> select_key_values;
    eckit::Tokenizer(',')(select, select_key_values);
    eckit::Tokenizer equalsTokenizer('=');

    for (const std::string& key_value : select_key_values) {
        std::vector<std::string> kv;
        equalsTokenizer(key_value, kv);

        if (kv.size() != 2 || selectDict_.find(kv[0]) != selectDict_.end()) {
            std::stringstream ss;
            ss << "Invalid select condition in gribjump config: " << select << std::endl;
            throw eckit::UserError(ss.str(), Here());
        }

        selectDict_[kv[0]] = eckit::Regex(kv[1]);
    }

    if (LibGribJump::instance().debug()) {
        LOG_DEBUG_LIB(LibGribJump) << "FDBPlugin Select dictionary:" << std::endl;
        for (const auto& kv : selectDict_) {
            LOG_DEBUG_LIB(LibGribJump) << "    " << kv.first << " => " << kv.second << std::endl;
        }
    }

}

// Check if selectDict matches key
bool FDBPlugin::matches(const fdb5::Key& key) const {

    if (selectDict_.empty()) return false;

    for (const auto& kv : selectDict_) {
        std::string value = key.get(kv.first);
        eckit::Regex regex(kv.second);
        if (!regex.match(value)) return false;
    }

    return true;
}

} // namespace gribjump
