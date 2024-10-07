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

#include "gribjump/Config.h"
#include "eckit/config/YAMLConfiguration.h"
#include "eckit/filesystem/PathName.h"

namespace gribjump {   

// Config options:
// - type          // Whether GribJump will work locally or forward work to a remote server. Allowed values are `local` or `remote`.
// - server        // Configuration for gribjump-server.
//   - port        // The port to listen on for incoming work.
// - uri           // host:port of remote server to forward work to (requires type:remote)
// - threads       // The number of worker threads for gribjump.extract. Default is 1.
// - cache         // Configuration of the cache.
//   - shadowfdb   // If true, the cache files will be stored in the same directory as data files. DEFAULT=true
//   - directory   // The directory where the cache will be stored, instead of shadowing the FDB.
//   - enable      // Whether to look at the cache at all. DEFAULT=true
// - plugin        // Configuration for using GribJump as a plugin to FDB, which generates jumpinfos on the fly for fdb.archive()
//                 // NOTE Plugin cannot be enabled from config, one must set the envar FDB_ENABLE_GRIBJUMP
//                 // NOTE Setting env FDB_DISABLE_GRIBJUMP will override this setting and disable the plugin.
//   - select      // Defines regex for selecting which FDB keys to generate jumpinfo for. If unset, no jumpinfos will be generated.
//                 // example `select: date=(20*),stream=(oper|test)`.

Config::Config() {
}

Config::Config(const eckit::PathName path) :
    eckit::LocalConfiguration(eckit::YAMLConfiguration(path)) {
}

std::map<std::string, std::string> Config::getMap(const std::string& key) const {

    std::map<std::string, std::string> map;
    eckit::LocalConfiguration conf = getSubConfiguration(key);
    for (const auto& k : conf.keys()) {
        map[k] = conf.getString(k);
    }
    return map;
}

} // namespace gribjump
