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
#include <memory>
#include "gribjump/info/JumpInfo.h"

namespace gribjump {

class InfoBuilderBase {
    std::string name_;

public:

    InfoBuilderBase(const std::string& name);
    virtual ~InfoBuilderBase();

    virtual std::unique_ptr<JumpInfo> make(eckit::DataHandle& handle, const metkit::grib::GribHandle& h,
                                           const eckit::Offset startOffset) const    = 0;
    virtual std::unique_ptr<JumpInfo> make(const eckit::message::Message& msg) const = 0;
};

template <class T>
class InfoBuilder : public InfoBuilderBase {

    std::unique_ptr<JumpInfo> make(eckit::DataHandle& h, const metkit::grib::GribHandle& gh,
                                   eckit::Offset startOffset) const override {
        return std::make_unique<T>(h, gh, startOffset);
    }

    std::unique_ptr<JumpInfo> make(const eckit::message::Message& msg) const override {
        return std::make_unique<T>(msg);
    }

public:

    InfoBuilder(const std::string& name) : InfoBuilderBase(name) {}
    ~InfoBuilder() override = default;
};

class InfoFactory {
public:

    static InfoFactory& instance();

    std::unique_ptr<JumpInfo> build(eckit::DataHandle& h, const eckit::Offset& offset);
    std::unique_ptr<JumpInfo> build(const eckit::message::Message& msg);

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
