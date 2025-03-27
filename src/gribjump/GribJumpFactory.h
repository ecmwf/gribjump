/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "gribjump/GribJump.h"

#pragma once

namespace gribjump {

class GribJumpFactory {
    virtual GribJumpBase* make(const Config& config) const = 0;

protected:

    explicit GribJumpFactory(const std::string&);
    virtual ~GribJumpFactory();

    std::string name_;

public:

    static GribJumpBase* build(const Config& config);
};

/// Templated specialisation of the self-registering factory,
/// that does the self-registration, and the construction of each object.

template <class T>
class GribJumpBuilder : public GribJumpFactory {
    GribJumpBase* make(const Config& config) const override { return new T(config); }

public:

    explicit GribJumpBuilder(const std::string& name) : GribJumpFactory(name) {}
};

}  // namespace gribjump
