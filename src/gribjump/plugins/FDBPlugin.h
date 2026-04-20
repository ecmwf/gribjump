/*
 * (C) Copyright 2024- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
#include "eckit/utils/Regex.h"
#include "gribjump/info/InfoAggregator.h"

namespace fdb5 {
class CallbackRegistry;
}
namespace fdb5 {
class Key;
}

namespace gribjump {

// For when GribJump is a plugin to FDB
class FDBPlugin {
public:

    static FDBPlugin& instance();
    void addFDB(fdb5::CallbackRegistry& fdb);

private:

    FDBPlugin();

    void parseConfig();
    bool matches(const fdb5::Key& key) const;

private:

    bool configParsed_ = false;
    std::map<std::string, eckit::Regex> selectDict_;

    std::mutex mutex_;

    std::vector<std::unique_ptr<std::optional<InfoAggregator>>> aggregators_;
};

}  // namespace gribjump