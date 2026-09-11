// gribjump library metadata + runtime initialisation bridge — implementation.

#include "gribjump_exceptions.h"

#include "Library.h"

#include "gribjump/LibGribJump.h"

#include "eckit/runtime/Main.h"

#include <mutex>

namespace gribjump_bridge {

//----------------------------------------------------------------------------------------------------------------------

namespace {

std::once_flag g_init_flag;

}  // namespace

void Library::initialise() {
    std::call_once(g_init_flag, []() {
        if (!eckit::Main::ready()) {
            static const char* argv[] = {"gribjump", nullptr};
            eckit::Main::initialise(1, const_cast<char**>(argv));
        }
    });
}

rust::String Library::version() {
    return rust::String(gribjump::LibGribJump::instance().version());
}

rust::String Library::git_sha1() {
    return rust::String(gribjump::LibGribJump::instance().gitsha1());
}

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump_bridge
