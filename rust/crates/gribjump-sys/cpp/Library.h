// gribjump library metadata + runtime initialisation bridge.
#pragma once

#include "rust/cxx.h"

namespace gribjump_bridge {

//----------------------------------------------------------------------------------------------------------------------

class Library {
public:

    /// Initialise the gribjump library (sets up `eckit::Main`). Idempotent.
    static void initialise();

    /// Get the gribjump library version string.
    static rust::String version();

    /// Get the gribjump git SHA1 hash.
    static rust::String git_sha1();
};

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump_bridge
