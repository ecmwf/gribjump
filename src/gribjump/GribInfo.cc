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

#include <queue>
#include <memory>
#include <cassert>
#include <cmath> // isnan. Temp, only for debug, remove later.
#include "eckit/io/DataHandle.h"
#include "eckit/utils/MD5.h"
#include "metkit/codes/GribAccessor.h"
#include "gribjump/GribHandleData.h"
#include "gribjump/GribInfo.h"
#include "gribjump/GribJumpException.h"

#include <compressor/compressor.h>
//#include <compressor/simple_compressor.h>
#include <compressor/ccsds_compressor.h>


using namespace eckit;
using namespace metkit::grib;

extern "C" {
    unsigned long grib_decode_unsigned_long(const unsigned char* p, long* offset, int bits);
    double grib_power(long s, long n);
}

namespace gribjump {

static GribAccessor<long>          editionNumber("editionNumber");
static GribAccessor<long>          bitmapPresent("bitmapPresent");
static GribAccessor<long>          binaryScaleFactor("binaryScaleFactor");
static GribAccessor<long>          decimalScaleFactor("decimalScaleFactor");
static GribAccessor<unsigned long> bitsPerValue("bitsPerValue");
static GribAccessor<unsigned long> ccsdsFlags("ccsdsFlags");
static GribAccessor<unsigned long> ccsdsBlockSize("ccsdsBlockSize");
static GribAccessor<unsigned long> ccsdsRsi("ccsdsRsi");
static GribAccessor<double>        referenceValue("referenceValue");
static GribAccessor<unsigned long> offsetBeforeData("offsetBeforeData");
static GribAccessor<unsigned long> offsetAfterData("offsetAfterData");
static GribAccessor<unsigned long> offsetBeforeBitmap("offsetBeforeBitmap");
static GribAccessor<unsigned long> numberOfValues("numberOfValues");
static GribAccessor<unsigned long> numberOfDataPoints("numberOfDataPoints");
static GribAccessor<long>          sphericalHarmonics("sphericalHarmonics");
static GribAccessor<unsigned long> totalLength("totalLength");
static GribAccessor<unsigned long> offsetBSection6("offsetBSection6");
static GribAccessor<std::string> md5GridSection("md5GridSection");
static GribAccessor<std::string> packingType("packingType");

static Mutex mutex;

// GRIB does not in general specify what do use in place of missing value.
constexpr double MISSING_VALUE = std::numeric_limits<double>::quiet_NaN();
constexpr size_t MISSING_INDEX = -1;

static int bits[65536] = {
#include "metkit/pointdb/bits.h"
};

// clang, gcc 10+
#if defined(__has_builtin)
    #if __has_builtin(__builtin_popcountll)
        #define POPCOUNT_AVAILABLE 1
    #else
        #define POPCOUNT_AVAILABLE 0
    #endif
// gcc 3.4+
#elif defined(__GNUC__) && (__GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ >= 4))
    #define POPCOUNT_AVAILABLE 1
#else
    #define POPCOUNT_AVAILABLE 0
#endif

static inline int count_bits(unsigned long long n) {
    if (POPCOUNT_AVAILABLE) {
        return __builtin_popcountll(n);
    }
    // TODO(Chris): see also _mm_popcnt_u64 in <immintrin.h>, but not suitable for ARM
    // fallback: lookup table
    return bits[n         & 0xffffu]
           +  bits[(n >> 16) & 0xffffu]
           +  bits[(n >> 32) & 0xffffu]
           +  bits[(n >> 48) & 0xffffu];
}

static inline uint64_t reverse_bytes(uint64_t n) {
    return ((n & 0x00000000000000FF) << 56) |
           ((n & 0x000000000000FF00) << 40) |
           ((n & 0x0000000000FF0000) << 24) |
           ((n & 0x00000000FF000000) << 8) |
           ((n & 0x000000FF00000000) >> 8) |
           ((n & 0x0000FF0000000000) >> 24) |
           ((n & 0x00FF000000000000) >> 40) |
           ((n & 0xFF00000000000000) >> 56);
}

JumpInfo::JumpInfo():numberOfValues_(0){
}

JumpInfo::JumpInfo(const GribHandle& h):numberOfValues_(0){
    update(h);
}

void JumpInfo::update(const GribHandle& h) {
    editionNumber_      = editionNumber(h);
    if (editionNumber_ != 1 && editionNumber_ != 2) {
        std::stringstream ss;
        ss << "Unsupported GRIB edition number: " << editionNumber_;
        throw JumpException(ss.str(), Here());
    }
    binaryScaleFactor_  = binaryScaleFactor(h);
    decimalScaleFactor_ = decimalScaleFactor(h);
    bitsPerValue_       = bitsPerValue(h);
    ccsdsFlags_         = ccsdsFlags(h);
    ccsdsBlockSize_     = ccsdsBlockSize(h);
    ccsdsRsi_           = ccsdsRsi(h);
    referenceValue_     = referenceValue(h);
    offsetBeforeData_   = offsetBeforeData(h);
    offsetAfterData_    = offsetAfterData(h);
    numberOfDataPoints_ = numberOfDataPoints(h);
    numberOfValues_     = numberOfValues(h);
    sphericalHarmonics_ = sphericalHarmonics(h); // todo: make quiet
    totalLength_        = totalLength(h);
    md5GridSection_     = md5GridSection(h);
    packingType_        = packingType(h);
    bitmapPresent_ = bitmapPresent(h);
    if (bitmapPresent_)
        offsetBeforeBitmap_ = editionNumber_ == 1? offsetBeforeBitmap(h) : offsetBSection6(h);
    else
        offsetBeforeBitmap_ = 0;

    binaryMultiplier_ = grib_power(binaryScaleFactor_, 2);
    decimalMultiplier_ = grib_power(-decimalScaleFactor_, 10);
}

void JumpInfo::print(std::ostream& s) const {
    s << "JumpInfo[";
    s << "version=" << +version_ << ",";
    s << "editionNumber=" << editionNumber_ << ",";
    s << "binaryScaleFactor=" << binaryScaleFactor_ << ",";
    s << "decimalScaleFactor=" << decimalScaleFactor_ << ",";
    s << "bitsPerValue=" << bitsPerValue_ << ",";
    s << "ccsdsFlags=" << ccsdsFlags_ << ",";
    s << "ccsdsBlockSize=" << ccsdsBlockSize_ << ",";
    s << "ccsdsRsi=" << ccsdsRsi_ << ",";
    s << "referenceValue=" << referenceValue_ << ",";
    s << "offsetBeforeData=" << offsetBeforeData_ << ",";
    s << "offsetAfterData=" << offsetAfterData_ << ",";
    s << "numberOfDataPoints=" << numberOfDataPoints_ << ",";
    s << "numberOfValues=" << numberOfValues_ << ",";
    s << "offsetBeforeBitmap=" << offsetBeforeBitmap_ << ",";
    s << "sphericalHarmonics=" << sphericalHarmonics_ << ",";
    s << "binaryMultiplier=" << binaryMultiplier_ << ",";
    s << "decimalMultiplier=" << decimalMultiplier_ << ",";
    s << "totalLength=" << totalLength_ << ",";
    s << "msgStartOffset=" << msgStartOffset_ << ",";
    s << "md5GridSection=" << md5GridSection_  << ",";
    s << "packingType=" << packingType_;
    s << "]";
    s << std::endl;
}

void JumpInfo::toFile(eckit::PathName pathname, bool append){
    std::unique_ptr<DataHandle> dh(pathname.fileHandle());
    version_ = currentVersion_;

    append ? dh->openForAppend(0) : dh->openForWrite(0);

    dh->write(&version_, sizeof(version_));
    dh->write(&editionNumber_, sizeof(editionNumber_));
    dh->write(&binaryScaleFactor_, sizeof(binaryScaleFactor_));
    dh->write(&decimalScaleFactor_, sizeof(decimalScaleFactor_));
    dh->write(&bitsPerValue_, sizeof(bitsPerValue_));
    dh->write(&ccsdsFlags_, sizeof(ccsdsFlags_));
    dh->write(&ccsdsBlockSize_, sizeof(ccsdsBlockSize_));
    dh->write(&ccsdsRsi_, sizeof(ccsdsRsi_));
    dh->write(&referenceValue_, sizeof(referenceValue_));
    dh->write(&offsetBeforeData_, sizeof(offsetBeforeData_));
    dh->write(&offsetAfterData_, sizeof(offsetAfterData_));
    dh->write(&numberOfDataPoints_, sizeof(numberOfDataPoints_));
    dh->write(&numberOfValues_, sizeof(numberOfValues_));
    dh->write(&offsetBeforeBitmap_, sizeof(offsetBeforeBitmap_));
    dh->write(&sphericalHarmonics_, sizeof(sphericalHarmonics_));
    dh->write(&binaryMultiplier_, sizeof(binaryMultiplier_));
    dh->write(&decimalMultiplier_, sizeof(decimalMultiplier_));
    dh->write(&totalLength_, sizeof(totalLength_));
    dh->write(&msgStartOffset_, sizeof(msgStartOffset_));
    dh->write(md5GridSection_.data(), md5GridSection_.size());
    dh->write(packingType_.data(), packingType_.size());
    dh->close();
}
void JumpInfo::fromFile(eckit::PathName pathname, uint16_t msg_id){
    std::unique_ptr<DataHandle> dh(pathname.fileHandle());

    dh->openForRead();
    dh->seek(msg_id*metadataSize);
    // make sure we aren't reading past the end of the file
    ASSERT(dh->position() + eckit::Offset(metadataSize) <= dh->size());
    dh->read(&version_, sizeof(version_));
    if (version_ != currentVersion_) {
        std::stringstream ss;
        ss << "Bad JumpInfo version found in " << pathname;
        dh->close();
        throw JumpException(ss.str(), Here());
    }
    dh->read(&editionNumber_, sizeof(editionNumber_));
    dh->read(&binaryScaleFactor_, sizeof(binaryScaleFactor_));
    dh->read(&decimalScaleFactor_, sizeof(decimalScaleFactor_));
    dh->read(&bitsPerValue_, sizeof(bitsPerValue_));
    dh->read(&ccsdsFlags_, sizeof(ccsdsFlags_));
    dh->read(&ccsdsBlockSize_, sizeof(ccsdsBlockSize_));
    dh->read(&ccsdsRsi_, sizeof(ccsdsRsi_));
    dh->read(&referenceValue_, sizeof(referenceValue_));
    dh->read(&offsetBeforeData_, sizeof(offsetBeforeData_));
    dh->read(&offsetAfterData_, sizeof(offsetAfterData_));
    dh->read(&numberOfDataPoints_, sizeof(numberOfDataPoints_));
    dh->read(&numberOfValues_, sizeof(numberOfValues_));
    dh->read(&offsetBeforeBitmap_, sizeof(offsetBeforeBitmap_));
    dh->read(&sphericalHarmonics_, sizeof(sphericalHarmonics_));
    dh->read(&binaryMultiplier_, sizeof(binaryMultiplier_));
    dh->read(&decimalMultiplier_, sizeof(decimalMultiplier_));
    dh->read(&totalLength_, sizeof(totalLength_));
    dh->read(&msgStartOffset_, sizeof(msgStartOffset_));
    dh->read(md5GridSection_.data(), md5GridSection_.size());
    dh->read(packingType_.data(), packingType_.size());
    dh->close();

}

void accumulateIndexes(uint64_t &n, size_t &count, std::vector<size_t> &newIndex, std::queue<size_t> &edges, bool &inRange, size_t &bp) {
    // Used by extractRanges to parse bitmap and calculate new indexes.
    // Counts set bits in n, and pushes new indexes to newIndex.

    ASSERT(!edges.empty());
    ASSERT(bp%64 == 0); // at start of new word

    constexpr uint64_t msb64 = 0x8000000000000000; // 0b100...0
    size_t endbit = bp + 64;
    while (bp < endbit) {
        if (bp == edges.front()) {
            inRange = !inRange;
            edges.pop();
            if (edges.empty()) break;
        }
        bool set = n & msb64;
        if (inRange) newIndex.push_back(set ? count : MISSING_INDEX);
        count += set ? 1 : 0;
        n <<= 1;
        ++bp;
    }
}

ExtractionResult
JumpInfo::extractRanges(const JumpHandle& f, std::vector<std::tuple<size_t, size_t>> intervals) const {

    ASSERT(!sphericalHarmonics_);
    std::vector<std::vector<std::bitset<64>>> masks;

    std::cerr << "flags = " << ccsdsFlags_ << std::endl;
    std::cerr << "block size = " << ccsdsBlockSize_ << std::endl;
    std::cerr << "rsi = " << ccsdsRsi_ << std::endl;
    std::cerr << "binary scale factor = " << binaryScaleFactor_ << std::endl;
    std::cerr << "decimal scale factor = " << decimalScaleFactor_ << std::endl;
    std::cerr << "bits per value = " << bitsPerValue_ << std::endl;
    std::cerr << "reference value = " << referenceValue_ << std::endl;
    std::cerr << "offset before data = " << offsetBeforeData_ << std::endl;
    std::cerr << "offset after data = " << offsetAfterData_ << std::endl;
    std::cerr << "number of data points = " << numberOfDataPoints_ << std::endl;
    std::cerr << "number of values = " << numberOfValues_ << std::endl;


    std::vector<mc::Range> ranges;
    std::transform(intervals.begin(), intervals.end(), std::back_inserter(ranges), [](auto interval){
        auto [begin, end] = interval;
        begin *= 8;
        end *= 8;
        //std::cout << "begin = " << begin << std::endl;
        //std::cout << "end = " << end << std::endl;
        return mc::Range{begin, end - begin + 1};
    });

    std::shared_ptr<mc::DataAccessor> data_accessor = std::make_shared<GribJumpDataAccessor>(&f, Range{offsetBeforeData_, offsetAfterData_ - offsetBeforeData_ + 1});
    std::cout << "EOF = " << data_accessor->eof() << std::endl;
    size_t eof = data_accessor->eof();
    mc::CcsdsCompressor<double>::CompressedData encoded = data_accessor->read({0, eof});

    assert(encoded.size() != 0);
    if (encoded.size() == 0) {
        std::cout << "encoded.size() == 0" << std::endl;
        throw std::runtime_error("encoded.size() == 0");
    }
    else {
        std::cout << "encoded.size() = " << encoded.size() << std::endl;
    }

    std::cout << "Decompressing " << numberOfValues_ << " values from " << offsetBeforeData_ << " to " << offsetAfterData_ << std::endl;
    mc::CcsdsDecompressor<double> ccsds{};
    ccsds
        .flags(ccsdsFlags_)
        .bits_per_sample(bitsPerValue_)
        .block_size(ccsdsBlockSize_)
        .rsi(ccsdsRsi_)
        .reference_value(referenceValue_)
        .binary_scale_factor(binaryScaleFactor_)
        .decimal_scale_factor(decimalScaleFactor_);



    ccsds.decode(encoded);

    auto offsets = ccsds.offsets();
    if (offsets) {
        std::cout << "offsets = " << offsets->size() << std::endl;
        for (size_t i = 0; i < offsets->size(); i++) {
            std::cout << "offsets[" << i << "] = " << (*offsets)[i] << std::endl;
        }
        ccsds.offsets(offsets.value());
    }
    else {
        std::cout << "offsets = None" << std::endl;
        throw std::runtime_error("offsets is None");
    }

    std::vector<std::vector<double>> result;
    mc::NumericDecompressor<double> *nd = &ccsds;
    std::vector<mc::CcsdsDecompressor<double>::Values> doubles_list = ccsds.decode(data_accessor, ranges);
    for (auto &doubles : doubles_list) {
        std::vector<double> values((double*) doubles.data(), (double*) doubles.data() + doubles.size() / 8);
        result.push_back(values);
    }

    return ExtractionResult(result, masks);



//    // NOTE: Ranges are treated as half-open intervals [start, end)
    
//    ASSERT(!sphericalHarmonics_);
    
//    // Sanity check intervals: Assert intervals are sorted and non-overlapping
//    size_t nValues = 0;
//    for (size_t i = 0; i < intervals.size(); ++i) {
//        const size_t start = std::get<0>(intervals[i]);
//        const size_t end = std::get<1>(intervals[i]);
//        ASSERT(start < end);
//        ASSERT(end <= numberOfDataPoints_);

//        // Assert no overlap with next interval
//        if (i < intervals.size()-1) {
//            const size_t nextstart = std::get<0>(intervals[i+1]);
//            ASSERT(end <= nextstart);
//        }
//        nValues += end - start;
//    }

//    // construct vectors for result values and mask.
//    std::vector<std::vector<double>> result;
//    std::vector<std::vector<std::bitset<64>>> masks;
//    constexpr size_t nBytes = sizeof(uint64_t);
//    constexpr size_t nBits = nBytes * 8;
//    for (auto r : intervals) {
//        const size_t size = std::get<1>(r) - std::get<0>(r);
//        result.push_back(std::vector<double>());
//        result.back().reserve(size);

//        std::vector<std::bitset<64>> mask;
//        mask.reserve(size/nBits + 1);
//        for (size_t i = 0; i < size/nBits + 1; ++i) {
//            mask.push_back(std::bitset<64>(0xffffffffffffffff));
//        }
//        masks.push_back(mask);
//    }

//    if (bitsPerValue_ == 0) {
//        // XXX can constant fields have a bitmap? We explicitly assuming not. TODO handle constant fields with bitmaps
//        // ASSERT(!offsetBeforeBitmap_);
//        for (size_t i = 0; i < intervals.size(); ++i) {
//            size_t size = std::get<1>(intervals[i]) - std::get<0>(intervals[i]);
//            result[i].insert(result[i].end(), size, referenceValue_);
//        }
//        return ExtractionResult(result, masks);
//    }

//    // set bufferSize equal to minimum bytes that will hold the largest interval.
//    size_t bufferSize = 0;
//    for (auto r : intervals) {
//        size_t i0 = std::get<0>(r);
//        size_t i1 = std::get<1>(r);
//        bufferSize = std::max(bufferSize, 1 + ((i1 - i0) * bitsPerValue_ + 7) / 8);
//    }

//    if (!offsetBeforeBitmap_) {
//        // no bitmap, just read the values
//        std::unique_ptr<unsigned char[]> buf(new unsigned char[bufferSize]);

//        for (size_t i = 0; i < intervals.size(); ++i) {
//            const auto r = intervals[i];
//            const size_t start = std::get<0>(r);
//            const size_t end = std::get<1>(r);

//            const Offset offset = msgStartOffset_ + offsetBeforeData_  + start * bitsPerValue_ / 8;
//            ASSERT(f.seek(offset) == offset);

//            const long len = 1 + ((end - start)*(bitsPerValue_) + 7) / 8;
//            ASSERT(len <= bufferSize);

//            ASSERT(f.read(buf.get(), len) == len);
//            long bitp = (start * bitsPerValue_) % 8;

//            for (size_t j = start; j < end; ++j) {
//                // TODO(Chris): array version of grib_decode_unsigned_long?
//                unsigned long p = grib_decode_unsigned_long(buf.get(), &bitp, bitsPerValue_);
//                double v = (p*binaryMultiplier_ + referenceValue_) * decimalMultiplier_;
//                result[i].push_back(v);
//            }
//        }
//        return ExtractionResult(result, masks);
//    }
//    // Flatten intervals into a list of edges.
//    // e.g. interval(1, 5), interval(7, 10) interval(10, 20), interval(20, 30) -> edges(1, 5, 7, 30)
//    std::queue<size_t> edges;
//    edges.push(std::get<0>(intervals[0]));
//    size_t prevEnd = std::get<1>(intervals[0]);
//    for (size_t i = 1; i < intervals.size(); ++i) {
//        size_t start = std::get<0>(intervals[i]);
//        if (start != prevEnd) {
//            edges.push(prevEnd);
//            edges.push(start);
//        }
//        prevEnd = std::get<1>(intervals[i]);
//    }
//    edges.push(prevEnd);

//    size_t maxSep = std::get<0>(intervals[0]);
//    for (size_t i = 0; i < intervals.size()-1; ++i) {
//        maxSep = std::max(maxSep, (std::get<0>(intervals[i+1]) - std::get<1>(intervals[i])));
//    }
//    std::unique_ptr<uint64_t[]> bufskip(new uint64_t[maxSep/nBits + 1]);

//    // Read bitmap and find new indexes
//    uint64_t n;
//    std::vector<size_t> newIndex;
//    newIndex.reserve(nValues);
//    size_t bp = 0;
//    size_t count = 0;
//    bool inRange = false;
//    const Offset offset = msgStartOffset_ + offsetBeforeBitmap_;
//    ASSERT(f.seek(offset) == offset);
//    while (!edges.empty()) {
//        if (!inRange){
//            const size_t nWordsToSkip = (edges.front() - bp)/nBits;
//            const size_t nBytesToSkip = nWordsToSkip * nBytes;
//            ASSERT(f.read(bufskip.get(), nBytesToSkip) == nBytesToSkip);
//            for (size_t i = 0; i < nWordsToSkip; ++i) {
//                count += count_bits(bufskip[i]);
//            }
//            bp += nWordsToSkip * nBits;
//        }
//        ASSERT(f.read(&n, nBytes) == nBytes);
//        // TODO(Chris): Endian check?
//        n = reverse_bytes(n);
//        accumulateIndexes(n, count, newIndex, edges, inRange, bp);
//    }

//    // read the values
//    std::unique_ptr<unsigned char[]> buf(new unsigned char[bufferSize]);
//    count = 0;
//    for (size_t ri = 0; ri < intervals.size(); ++ri) {
//        // find index of first and final non-missing values in this interval
//        const auto r = intervals[ri];
//        std::vector<double>& values = result[ri];
//        const size_t size = std::get<1>(r) - std::get<0>(r);
//        size_t start;
//        size_t end;
//        std::vector<std::bitset<64>>& mask = masks[ri];
//        size_t maskCount = 0;

//        // TODO(maee): Make sure size is greater than 0, otherwise start will be uninitialized
//        assert(size > 0);
//        for (size_t i = count; i < count + size; ++i) {
//            start = newIndex[i];
//            if (start != MISSING_INDEX) break;
//        }
//        if (start == MISSING_INDEX){
//            // all values in this interval are missing
//            values.insert(values.end(), size, MISSING_VALUE);
//            for (size_t i = 0; i < size/nBits + 1; ++i) {
//                mask[i].reset();
//            }
//            count += size;
//            continue;
//        }
//        for (size_t i = count + size - 1; i >= count; --i) {
//            end = newIndex[i];
//            if (end != MISSING_INDEX) break;
//        }

//        // Read data interval into buffer
//        const Offset offset = msgStartOffset_ + offsetBeforeData_ + start*bitsPerValue_/8;
//        const long len = 1 + ((end + 1 - start)*bitsPerValue_ + 7) / 8;
//        ASSERT(len <= bufferSize);
//        ASSERT(f.seek(offset) == offset);
//        ASSERT(f.read(buf.get(), len) == len);

//        // Decode values
//        long bitp = (start * bitsPerValue_) % 8;
//        for (size_t i = count; i < count + size; ++i) {
//            const size_t index = newIndex[i];
//            if (index == MISSING_INDEX){
//                values.push_back(MISSING_VALUE);
//                mask[maskCount/nBits].set(maskCount%nBits, 0);
//                ++maskCount;
//                continue;
//            }
//            // TODO: array version of grib_decode_unsigned_long?
//            const unsigned long p = grib_decode_unsigned_long(buf.get(), &bitp, bitsPerValue_); 
//            const double v = (p*binaryMultiplier_ + referenceValue_) * decimalMultiplier_;
//            values.push_back(v);
//            ++maskCount;
//        }
//        count += size;
//    }

//    return ExtractionResult(result, masks);
}

double JumpInfo::extractValue(const JumpHandle& f, size_t index) const {

    if (bitsPerValue_ == 0)
        return referenceValue_;

    ASSERT(!sphericalHarmonics_);

    if (offsetBeforeBitmap_) {
        ASSERT(index < numberOfDataPoints_);

        // Jump to start of bitmap.
        Offset offset = off_t(msgStartOffset_) + off_t(offsetBeforeBitmap_);
        ASSERT(f.seek(offset) == offset);

        // Skip to byte containing the bit we want, counting set bits as we go.
        uint64_t n;
        const size_t nBytes = sizeof(n);
        const size_t nBits = nBytes * 8;
        size_t count = 0;
        size_t skip = index / (8*nBytes);
        for (size_t i = 0; i < skip; ++i) {
            ASSERT(f.read(&n, nBytes) == nBytes);
            count += count_bits(n);
        }
        ASSERT(f.read(&n, nBytes) == nBytes);

        n = reverse_bytes(n);
        n = (n >> (nBits - index%(nBits) -1));
        count += count_bits(n);
        if (!(n & 1)) return MISSING_VALUE;

        // Update index accounting for bitmask
        index = count - 1;
    }
    ASSERT(index < numberOfValues_);

    return readDataValue(f, index);
}

double JumpInfo::readDataValue(const JumpHandle& f, size_t index) const {
    // Read the data value at index.

    // Seek to start of byte containing value and read.
    Offset offset = msgStartOffset_ + offsetBeforeData_  + index*bitsPerValue_/8;
    long len = 1 + (bitsPerValue_ + 7) / 8;
    unsigned char buf[8];
    ASSERT(f.seek(offset) == offset);
    ASSERT(f.read(buf, len) == len);

    // Decode
    long bitp = (index * bitsPerValue_) % 8;
    unsigned long p = grib_decode_unsigned_long(buf, &bitp, bitsPerValue_);
    return ((p * binaryMultiplier_) + referenceValue_) * decimalMultiplier_;
}

} // namespace gribjump
