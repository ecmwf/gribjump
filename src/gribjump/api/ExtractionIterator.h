/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

/// @author Caragh Bradley

#pragma once

#include "gribjump/ExtractionData.h"
#include "gribjump/api/ResultIterator.h"

namespace gribjump {

using ExtractionSource   = ResultSource<ExtractionResult>;
using ExtractionIterator = ResultIterator<ExtractionResult>;

}  // namespace gribjump