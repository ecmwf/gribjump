#include "Ccsds.h"
#include "Aec.h"
//#include "serializer.h"
//#include "libaec.h"

#include <unordered_map>
#include <memory>
#include <cassert>
#include <algorithm>

namespace mc {

bool is_big_endian()
{
    unsigned char is_big_endian = 0;
    unsigned short endianess_test = 1;
    return reinterpret_cast<const char*>(&endianess_test)[0] == is_big_endian;
}

size_t modify_aec_flags(size_t flags)
{
  // ECC-1602: Performance improvement: enabled the use of native data types
  flags &= ~AEC_DATA_3BYTE;  // disable support for 3-bytes per value
  if (is_big_endian())
    flags |= AEC_DATA_MSB; // enable big-endian
  else
    flags &= ~AEC_DATA_MSB;  // enable little-endian
  return flags;
}

} // namespace mc
