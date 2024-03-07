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

#pragma once

#include "gribjump/info/JumpInfo.h"

namespace gribjump {

class InfoBuilderBase {
    std::string name_;

public:
    InfoBuilderBase(const std::string& name);
    virtual ~InfoBuilderBase();

    virtual Info* make(eckit::DataHandle& h, const metkit::grib::GribHandle& gh) const = 0;

};

template <class T>
class InfoBuilder : public InfoBuilderBase {
   
    Info* make(eckit::DataHandle& h, const metkit::grib::GribHandle& gh) const override {
        return new T(h, gh);
    }

public:
    InfoBuilder(const std::string& name) : InfoBuilderBase(name) {}
    ~InfoBuilder() override = default;
};

class InfoFactory {
public:

    static InfoFactory& instance();

    Info* build(eckit::DataHandle& h, const eckit::Offset& offset);

    void enregister(const std::string& name, InfoBuilderBase* builder);
    void deregister(const std::string& name);

    InfoBuilderBase* get(const std::string& name);

private:

    InfoFactory();
    ~InfoFactory();

private:

    std::map<std::string, InfoBuilderBase*> builders_;
    std::mutex mutex_;

};

}  // namespace gribjump
