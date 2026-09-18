// gribjump C++ bridge — forward declarations for the cxx-shared data structs.
//
// All structs are defined on the Rust side via the cxx bridge in `lib.rs`;
// this header just exposes them to the C++ wrapper code that consumes them.
#pragma once

namespace gribjump_bridge {

//----------------------------------------------------------------------------------------------------------------------

struct Range;
struct ExtractionRequestData;
struct PathExtractionRequestData;
struct ExtractionResultData;
struct AxisEntry;
struct FileExtractionData;

//----------------------------------------------------------------------------------------------------------------------

}  // namespace gribjump_bridge
