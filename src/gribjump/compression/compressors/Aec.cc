/*
 * (C) Copyright 2023- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include "Aec.h"

namespace mc {

void print_aec_stream_info(struct aec_stream* strm, const char* func)
{
  fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.flags=%u\n",           func, strm->flags);
  fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.bits_per_sample=%u\n", func, strm->bits_per_sample);
  fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.block_size=%u\n",      func, strm->block_size);
  fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.rsi=%u\n",             func, strm->rsi);
  fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.avail_out=%lu\n",      func, strm->avail_out);
  fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.avail_in=%lu\n",       func, strm->avail_in);
  fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.next_out=%p\n",        func, strm->next_out);
  fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.next_in=%p\n",         func, strm->next_in);
  fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.total_in=%lu\n",       func, strm->total_in);
  fprintf(stderr, "ECCODES DEBUG CCSDS %s aec_stream.total_out=%lu\n",      func, strm->total_out);
  fprintf(stderr, "\n");
}

void print_binary(const char* name, const void* data, size_t n_bytes) {
  std::cout << name << ": ";
  for (size_t i = 0; i < n_bytes; ++i) {
    std::cout << std::bitset<8>(static_cast<const char*>(data)[i]) << " ";
  }
  std::cout << std::endl;
}

} // namespace mc
