// gribjump ExtractionIteratorHandle bridge — implementation.

#include "gribjump_exceptions.h"

#include "ExtractionIteratorHandle.h"
#include "gribjump-sys/src/lib.rs.h"

#include "eckit/exception/Exceptions.h"

namespace gribjump_bridge {

//----------------------------------------------------------------------------------------------------------------------

ExtractionIteratorHandle::ExtractionIteratorHandle(gribjump::ExtractionIterator&& it) : impl_(std::move(it)) {}

bool ExtractionIteratorHandle::hasNext() const {
    return impl_.hasNext();
}

std::unique_ptr<ExtractionResultHandle> ExtractionIteratorHandle::next() {
    if (!impl_.hasNext()) {
        throw eckit::SeriousBug("Iterator exhausted: next() called after hasNext() returned false");
    }
    auto result_ptr = impl_.next();
    return std::make_unique<ExtractionResultHandle>(std::move(*result_ptr));
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump_bridge
