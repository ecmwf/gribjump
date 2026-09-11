// gribjump C++ bridge for Rust FFI — umbrella header pulled in by the
// cxx-generated bridge (`include!("GribJumpBridge.h")` in lib.rs) and by
// downstream `-sys` crates. Real declarations live in the per-topic headers
// below.
#pragma once

#include "ExtractionIteratorHandle.h"
#include "ExtractionResultHandle.h"
#include "GribJumpHandle.h"
#include "Library.h"
#include "Types.h"
