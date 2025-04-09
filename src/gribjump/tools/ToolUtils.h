/*
 * (C) Copyright 2024- ECMWF.
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
#include "gribjump/ExtractionData.h"
#include "metkit/mars/MarsRequest.h"

namespace gribjump {

std::vector<std::vector<Range>> parseRangesFile(eckit::PathName fname);
std::vector<metkit::mars::MarsRequest> flattenRequest(const metkit::mars::MarsRequest& request);


}  // namespace gribjump
