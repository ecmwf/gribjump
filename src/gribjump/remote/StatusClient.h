/*
 * (C) Copyright 2026- ECMWF.
 * Licensed under the Apache Licence Version 2.0. ECMWF does not waive its
 * privileges and immunities or submit to any jurisdiction.
 */
#pragma once

#include "gribjump/Config.h"
#include "gribjump/Metrics.h"

namespace gribjump {

class StatusUnsupported : public eckit::Exception {
public:

    explicit StatusUnsupported(const std::string& reason) :
        eckit::Exception("gribjump.status unsupported: " + reason) {}
};

/// Query the union of uri and servermap destinations, once per host/port.
/// At most eight concurrent queries. The timeout covers connect + wire I/O
/// per endpoint; hostname resolution is subject to the system resolver timeout.
std::string remoteStatus(const ConfigOptions&, double timeoutSeconds, const LogContext&);

}  // namespace gribjump
