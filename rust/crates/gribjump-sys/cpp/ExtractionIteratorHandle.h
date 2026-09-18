// gribjump ExtractionIteratorHandle bridge — wraps
// `gribjump::ExtractionIterator`.
#pragma once

#include "ExtractionResultHandle.h"

#include "gribjump/api/ExtractionIterator.h"

#include <memory>

namespace gribjump_bridge {

//----------------------------------------------------------------------------------------------------------------------

class ExtractionIteratorHandle {
public:

    explicit ExtractionIteratorHandle(gribjump::ExtractionIterator&& it);
    ~ExtractionIteratorHandle() = default;

    ExtractionIteratorHandle(const ExtractionIteratorHandle&)            = delete;
    ExtractionIteratorHandle& operator=(const ExtractionIteratorHandle&) = delete;
    ExtractionIteratorHandle(ExtractionIteratorHandle&&)                 = default;
    ExtractionIteratorHandle& operator=(ExtractionIteratorHandle&&)      = default;

    bool hasNext() const;
    std::unique_ptr<ExtractionResultHandle> next();

private:

    gribjump::ExtractionIterator impl_;
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump_bridge
