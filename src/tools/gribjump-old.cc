// /*
//  * (C) Copyright 1996- ECMWF.
//  *
//  * This software is licensed under the terms of the Apache Licence Version 2.0
//  * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
//  * In applying this licence, ECMWF does not waive the privileges and immunities
//  * granted to it by virtue of its status as an intergovernmental organisation nor
//  * does it submit to any jurisdiction.
//  */

// #include <chrono>
// #include <fstream>

// #include "eckit/option/SimpleOption.h"
// #include "eckit/option/CmdArgs.h"
// #include "eckit/value/Value.h"
// #include "eckit/utils/StringTools.h"
// #include "eckit/serialisation/FileStream.h"

// #include "metkit/tool/MetkitTool.h"

// // #include "gribjump/JumpHandle.h"

// using namespace gribjump;

// //----------------------------------------------------------------------------------------------------------------------

// class GribJumpTool : public metkit::MetkitTool {

// public:

//     GribJumpTool(int argc, char **argv) : metkit::MetkitTool(argc, argv) {
//         options_.push_back(new eckit::option::SimpleOption<bool>("extract", "Extract info from grib header to write
//         to binary metadata file (set by -o)")); options_.push_back(new
//         eckit::option::SimpleOption<std::string>("meta", "Name of binary metadata file to write/read to/from
//         (default: <input_grib_name>.bin)")); options_.push_back(new eckit::option::SimpleOption<bool>("query", "Query
//         data range from grib file")); options_.push_back(new eckit::option::SimpleOption<std::string>("msgs", "Which
//         message(s) (from 0 to N-1) of the N messages in grib file to query (comma separated string)"));
//         options_.push_back(new eckit::option::SimpleOption<std::string>("time", "Filename to write timing info to
//         (default: none - no timing info written)"));
//     }

// private: // methods

//     virtual int minimumPositionalArguments() const { return 1; }
//     virtual void execute(const eckit::option::CmdArgs& args);
//     virtual void init(const eckit::option::CmdArgs& args);
//     virtual void usage(const std::string& tool) const;

// private: // members
//     bool doExtract_ = false;
//     bool doQuery_ = false;
//     bool doRange_ = false;
//     bool doTime_ = false;
//     eckit::PathName gribFileName_;
//     eckit::PathName binFileName_;
//     eckit::PathName timingFname_;
//     std::vector<size_t> msgids_;

//     std::vector<std::pair<size_t, size_t>> rangesVector_;
// };

// void GribJumpTool::usage(const std::string &tool) const {
//     eckit::Log::info() << std::endl
//                         << "Usage: " << tool << " [options] [input_grib_file] [min0] [max0] [min1] ... "
//                         << std::endl;

//     eckit::Log::info() << "Examples:" << std::endl
//                         << "=========" << std::endl
//                         << std::endl
//                         << "e.g. Process and extract metadata from data.grib to data.grib.bin:" << std::endl
//                         << tool << " --extract data.grib" << std::endl
//                         << std::endl
//                         << "e.g. Retrieve data in range [12, 45) and [56, 789) from the 0th, 1st and 10th, message in
//                         data.grib." << std::endl
//                         << tool << " --query --msgs=0,1,10 data.grib 12 45 56 789" << std::endl
//                         << std::endl;
// }

// struct Timing {
//     std::vector<double> msgTimes;
//     double totalTime = 0;
//     double extractTime = 0;
// };

// void GribJumpTool::init(const eckit::option::CmdArgs& args) {
//     doExtract_ = args.getBool("extract", false);
//     doQuery_ = args.getBool("query", false);
//     timingFname_ = args.getString("time", "");
//     doTime_ = timingFname_ != "";
//     std::string msgs = args.getString("msgs", "0");
//     std::vector<std::string> s = eckit::StringTools::split(",", msgs);
//     for (auto m : s){
//         msgids_.push_back(std::stoi(m));
//     }

//     gribFileName_ = args(0);
//     binFileName_ = args.getString("meta", gribFileName_.baseName() + ".bin");
//     ASSERT(gribFileName_.exists());

//     doExtract_ |= !binFileName_.exists(); // if bin doesn't exist, extract before query

//     if (!doQuery_) return;

//     doRange_ = true;
//     // note ranges must have a start and end
//     ASSERT(args.count() % 2 == 1);
//     std::cout << "Query range(s): ";
//     for (int i = 1; i < args.count(); i+=2){
//         std::cout << args(i) << "-" << args(i+1) << ", ";
//         rangesVector_.push_back({std::make_pair(std::stoi(args(i)), std::stoi(args(i+1)))});
//     }
//     std::cout << std::endl;
// }

// void GribJumpTool::execute(const eckit::option::CmdArgs& args) {

//     auto startTime = std::chrono::high_resolution_clock::now();
//     Timing timing;
//     JumpHandle dataSource(gribFileName_);

//     if (doExtract_) {
//         std::cout << "Build jump info from " << gribFileName_ << std::endl;
//         auto t0 = std::chrono::high_resolution_clock::now();
//         std::vector<JumpInfo*> infos = dataSource.extractInfoFromFile();
//         auto t1 = std::chrono::high_resolution_clock::now();
//         timing.extractTime = std::chrono::duration<double>(t1 - t0).count();

//         for(auto info : infos) {
//             std::cout << *info << std::endl;
//         }

//         write_jumpinfos_to_file(infos, binFileName_);
//     }

//     if (doQuery_){
//         JumpInfo gribInfo;
//         for (auto msg : msgids_){
//             std::cout << "Grib file: " << gribFileName_ << ", jump info file: " << binFileName_ << ", msg id: " <<
//             msg << std::endl; gribInfo = JumpInfo::fromFile(binFileName_, msg);

//             ASSERT(gribInfo.ready());

//             if (doRange_){
//                 auto t0 = std::chrono::high_resolution_clock::now();
//                 ExtractionResult* result = gribInfo.extractRanges(dataSource, rangesVector_);
//                 auto v = result->values(); // TODO(Chris):  Don't make a copy
//                 auto mask = result->mask(); // TODO(Chris): Don't make a copy
//                 auto t1 = std::chrono::high_resolution_clock::now();
//                 timing.msgTimes.push_back(std::chrono::duration<double>(t1 - t0).count());
//                 for (auto r : v){
//                     std::cout << "Value: ";
//                     for (auto v2 : r){
//                         std::cout << v2 << ", ";
//                     }
//                     std::cout << std::endl;
//                 }
//             }
//         }
//     }

//     auto endTime = std::chrono::high_resolution_clock::now();

//     if (doTime_){
//         std::chrono::duration<double> elapsed = endTime - startTime;
//         std::ofstream timingFile;
//         timingFile.open(timingFname_);
//         timingFile << "GRIB fname: " << gribFileName_ << std::endl;
//         timingFile << "No. Ranges: " << rangesVector_.size() << std::endl;
//         size_t count = 0;
//         for (auto r : rangesVector_){
//             count += std::get<1>(r) - std::get<0>(r);
//         }
//         timingFile << "No. Points: " << count << std::endl;
//         timingFile << "No. Msgs: " << msgids_.size() << std::endl;
//         timingFile << "Extract time: " << timing.extractTime << std::endl;
//         for (size_t i = 0; i < timing.msgTimes.size(); i++){
//             timingFile << "Msg " << i << " query time: " << timing.msgTimes[i] << std::endl;
//         }
//         timingFile << "Total execute time: " << elapsed.count() << std::endl;
//         timingFile.close();
//     }
// }

// //----------------------------------------------------------------------------------------------------------------------

// int main(int argc, char **argv)
// {
//     GribJumpTool tool(argc, argv);
//     return tool.start();
// }
