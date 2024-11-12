/*
 * (C) Copyright 1996- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation
 * nor does it submit to any jurisdiction.
 */

#include <cmath>
#include <fstream>

#include "eckit/testing/Test.h"
#include "eckit/filesystem/PathName.h"
#include "eckit/filesystem/LocalPathName.h"
#include "eckit/serialisation/FileStream.h"
#include "eckit/io/AutoCloser.h"

#include "metkit/mars/MarsRequest.h"

#include "gribjump/Serialiser.h"
#include "gribjump/ExtractionData.h"

#include "eckit/log/Timer.h"


using namespace eckit::testing;

namespace gribjump::test {

// Useful for timing
constexpr bool REPORT_TIMES = true;
constexpr size_t N_VECTOR = 100;
constexpr size_t N_VECTOR_VECTOR = 10;
constexpr size_t N_EXTRACTIONREQUESTS = 10;
constexpr size_t N_EXTRACTIONRESULTS = 10;


void reportTimes(size_t N, double serialiseTime, double deserialiseTime) {
    if (!REPORT_TIMES) return;
    eckit::Log::info() << "   For N=" << N << std::endl;
    eckit::Log::info() << "   Serialisation time: " << serialiseTime << std::endl;
    eckit::Log::info() << "   Deserialisation time: " << deserialiseTime << std::endl;
    eckit::Log::info() << "   Total time: " << serialiseTime + deserialiseTime << std::endl;
}

//-----------------------------------------------------------------------------


// CASE( "Serialisation: Vector<double>" ) {

//     eckit::PathName filename = "test_serialiser.out";

//     std::vector<double> vout(N_VECTOR);
//     for (size_t i = 0; i < N_VECTOR; i++) {
//         vout[i] = i;
//     }

//     eckit::Timer timer_serialise;
//     {
//         eckit::FileStream sout(filename, "w");
//         auto c = eckit::closer(sout);
//         Serialiser::encode(sout, vout);
//     }
//     timer_serialise.stop();

//     eckit::Timer timer_deserialise;
//     std::vector<double> vin;
//     {
//         eckit::FileStream sin(filename, "r");
//         auto c = eckit::closer(sin);
//         vin = Serialiser::decodeVector(sin);
//     }
//     timer_deserialise.stop();

//     EXPECT_EQUAL(vout.size(), vin.size());
//     for (size_t i = 0; i < vout.size(); i++) {
//         EXPECT_EQUAL(vout[i], vin[i]);
//     }

//     reportTimes(vout.size(), timer_serialise.elapsed(), timer_deserialise.elapsed());
// }


// //-----------------------------------------------------------------------------


// CASE( "Serialisation Naive: Vector<string>" ) {
    
//         eckit::PathName filename = "test_serialiser.out";
    
//         std::vector<std::string> vout;
//         for (size_t i = 0; i < N_VECTOR; i++) {
//             vout.push_back("this is a test string look at it go woah " + std::to_string(i));
//         }
    
//         eckit::Timer timer_serialise;
//         {
//             eckit::FileStream sout(filename, "w");
//             auto c = eckit::closer(sout);
//             Serialiser::encodeNaive(sout, vout);
//         }
//         timer_serialise.stop();
    
//         eckit::Timer timer_deserialise;
//         std::vector<std::string> vin;
//         {
//             eckit::FileStream sin(filename, "r");
//             auto c = eckit::closer(sin);
//             vin = Serialiser::decodeVectorStringNaive(sin);
//         }
//         timer_deserialise.stop();
    
//         EXPECT_EQUAL(vout.size(), vin.size());
//         for (size_t i = 0; i < vout.size(); i++) {
//             EXPECT_EQUAL(vout[i], vin[i]);
//         }
    
//         reportTimes(vout.size(), timer_serialise.elapsed(), timer_deserialise.elapsed());
// }

// //-----------------------------------------------------------------------------

// CASE( "Serialisation: Vector<string>" ) {
    
//         eckit::PathName filename = "test_serialiser.out";
    
//         std::vector<std::string> vout;
//         for (size_t i = 0; i < N_VECTOR; i++) {
//             vout.push_back("this is a test string look at it go woah and look its getting even bigger now this is probably big enough " + std::to_string(i));
//         }
    
//         eckit::Timer timer_serialise;
//         {
//             eckit::FileStream sout(filename, "w");
//             auto c = eckit::closer(sout);
//             Serialiser::encode(sout, vout);
//         }
//         timer_serialise.stop();
    
//         eckit::Timer timer_deserialise;
//         std::vector<std::string> vin;
//         {
//             eckit::FileStream sin(filename, "r");
//             auto c = eckit::closer(sin);
//             vin = Serialiser::decodeVectorString(sin);
//         }
//         timer_deserialise.stop();
    
//         EXPECT_EQUAL(vout.size(), vin.size());
//         for (size_t i = 0; i < vout.size(); i++) {
//             EXPECT_EQUAL(vout[i], vin[i]);
//         }
    
//         reportTimes(vout.size(), timer_serialise.elapsed(), timer_deserialise.elapsed());
// }



//-----------------------------------------------------------------------------

CASE( "Serialisation: Ranges" ) {
    
    eckit::PathName filename = "test_serialiser.out";

    bool naive = false;
    for (size_t i = 0; i < 2; i++) {
        Ranges vout;
        for (size_t i = 0; i < N_VECTOR; i++) {
            vout.push_back(Range(i, i+10));
        }

        eckit::Timer timer_serialise;
        {
            eckit::FileStream sout(filename, "w");
            auto c = eckit::closer(sout);
            Serialiser::encode(sout, vout, naive);
        }
        timer_serialise.stop();

        eckit::Timer timer_deserialise;
        Ranges vin;
        {
            eckit::FileStream sin(filename, "r");
            auto c = eckit::closer(sin);
            vin = Serialiser::decodeRanges(sin, naive);
        }
        timer_deserialise.stop();

        EXPECT_EQUAL(vout.size(), vin.size());
        for (size_t i = 0; i < vout.size(); i++) {
            EXPECT_EQUAL(vout[i].first, vin[i].first);
            EXPECT_EQUAL(vout[i].second, vin[i].second);
        }

        eckit::Log::info() << "Naive: " << naive << std::endl;
        reportTimes(vout.size(), timer_serialise.elapsed(), timer_deserialise.elapsed());
        naive = !naive;
    }
}

//-----------------------------------------------------------------------------

// CASE( "Serialisation: Vector<Vector<double>>" ) {
    
//     eckit::PathName filename = "test_serialiser.out";

//     std::vector<std::vector<double>> vout;
//     for (size_t i = 0; i < N_VECTOR_VECTOR; i++) {
//         std::vector<double> inner = {1.0, 2.0, 3.0};
//         vout.push_back(inner);
//     }

//     eckit::Timer timer_serialise;
//     {
//         eckit::FileStream sout(filename, "w");
//         auto c = eckit::closer(sout);
//         Serialiser::encode(sout, vout);
//     }
//     timer_serialise.stop();

//     eckit::Timer timer_deserialise;
//     std::vector<std::vector<double>> vin;
//     {
//         eckit::FileStream sin(filename, "r");
//         auto c = eckit::closer(sin);
//         vin = Serialiser::decodeVectorVector(sin);
//     }
//     timer_deserialise.stop();

//     EXPECT_EQUAL(vout.size(), vin.size());
//     for (size_t i = 0; i < vout.size(); i++) {
//         EXPECT_EQUAL(vout[i].size(), vin[i].size());
//         for (size_t j = 0; j < vout[i].size(); j++) {
//             EXPECT_EQUAL(vout[i][j], vin[i][j]);
//         }
//     }

//     reportTimes(vout.size(), timer_serialise.elapsed(), timer_deserialise.elapsed());
// }

// //-----------------------------------------------------------------------------

CASE( "Serialisation: Vector<ExtractionRequest>" ) {
    
    eckit::PathName filename = "test_serialiser.out";
    bool naive = false;
    for (size_t i = 0; i < 2; i++) {
        std::vector<ExtractionRequest> vout;
        for (size_t i = 0; i < N_EXTRACTIONREQUESTS; i++) {
            std::string s = "retrieve,expver=xxxx,class=od,date=20241110,domain=g,levelist=1000,levtype=pl,param=129,stream=oper,time=1200,type=an,step=" + std::to_string(i);
            metkit::mars::MarsRequest marsrequest = metkit::mars::MarsRequest::parse(s);
            Ranges ranges = {Range(i, i+10), Range(i+11, i+12), Range(i+100, i+200)};
            std::string hash = "testHash";

            ExtractionRequest req(marsrequest, ranges, hash);
            vout.push_back(req);
        }

        eckit::Timer timer_serialise;
        {
            eckit::FileStream sout(filename, "w");
            auto c = eckit::closer(sout);
            Serialiser::encode(sout, vout, naive);
        }
        timer_serialise.stop();

        eckit::Timer timer_deserialise;
        std::vector<ExtractionRequest> vin;
        {
            eckit::FileStream sin(filename, "r");
            auto c = eckit::closer(sin);
            vin = Serialiser::decodeExtractionRequests(sin, naive);
        }
        timer_deserialise.stop();

        EXPECT_EQUAL(vout.size(), vin.size());
        for (size_t i = 0; i < vout.size(); i++) {
            EXPECT_EQUAL(vout[i].request().asString(), vin[i].request().asString());
            EXPECT_EQUAL(vout[i].gridHash(), vin[i].gridHash());
            EXPECT_EQUAL(vout[i].ranges(), vin[i].ranges());
        }

        eckit::Log::info() << "Naive: " << naive << std::endl;
        reportTimes(vout.size(), timer_serialise.elapsed(), timer_deserialise.elapsed());
        naive = !naive;
    }
}

// //-----------------------------------------------------------------------------

// CASE( "Serialisation: Vector<Vector<ExtractionResult>>" ) {

//     eckit::PathName filename = "test_serialiser.out";

//     std::vector<std::vector<ExtractionResult>> vout;
//     for (size_t i = 0; i < N_EXTRACTIONRESULTS; i++) {
//         std::vector<ExtractionResult> inner;
//         for (size_t j = 0; j < 1; j++) {
//             std::vector<std::vector<double>> values = {{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}, {7.0, 8.0, 9.0}};
//             std::vector<std::vector<std::bitset<64>>> mask = {{0, 0, 0}, {0, 0, 0}, {0, 0, 0}};
//             inner.push_back(ExtractionResult(values, mask));
//         }
//         vout.push_back(std::move(inner));
//     }

//     eckit::Timer timer_serialise;
//     {
//         eckit::FileStream sout(filename, "w");
//         auto c = eckit::closer(sout);
//         Serialiser::encode(sout, vout);
//     }
//     timer_serialise.stop();

//     eckit::Timer timer_deserialise;
//     std::vector<std::vector<ExtractionResult>> vin;
//     {
//         eckit::FileStream sin(filename, "r");
//         auto c = eckit::closer(sin);
//         vin = Serialiser::decodeExtractionResults(sin);
//     }
//     timer_deserialise.stop();

//     EXPECT_EQUAL(vout.size(), vin.size());
//     for (size_t i = 0; i < vout.size(); i++) {
//         EXPECT_EQUAL(vout[i].size(), vin[i].size());
//         for (size_t j = 0; j < vout[i].size(); j++) {
//             auto vout_values = vout[i][j].values();
//             auto vin_values = vin[i][j].values();
//             EXPECT_EQUAL(vout_values.size(), vin_values.size());
//             for (size_t k = 0; k < vout_values.size(); k++) {
//                 EXPECT_EQUAL(vout_values[k], vin_values[k]);
//             }

//             auto vout_mask = vout[i][j].mask();
//             auto vin_mask = vin[i][j].mask();
//             EXPECT_EQUAL(vout_mask.size(), vin_mask.size());
//             for (size_t k = 0; k < vout_mask.size(); k++) {
//                 EXPECT_EQUAL(vout_mask[k].size(), vin_mask[k].size());
//                 for (size_t l = 0; l < vout_mask[k].size(); l++) {
//                     EXPECT_EQUAL(vout_mask[k][l], vin_mask[k][l]);
//                 }
//             }
//         }
//     }

//     reportTimes(vout.size(), timer_serialise.elapsed(), timer_deserialise.elapsed());
// }

}  // namespace gribjump


int main(int argc, char **argv)
{
    return run_tests ( argc, argv );
}
