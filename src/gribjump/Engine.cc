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

#include "eckit/log/Plural.h"

#include "gribjump/Engine.h"
#include "gribjump/Lister.h"
#include "gribjump/ExtractionItem.h"

#include "gribjump/remote/WorkQueue.h"

#include "gribjump/info/JumpInfoFactory.h"
#include "gribjump/jumper/JumperFactory.h"

#include "eccodes.h" // xxx temp... for extracting infos. doesnt belong here

namespace gribjump {
    
//----------------------------------------------------------------------------------------------------------------------

// Stringify requests and keys alphabetically
namespace 
{
std::string requestToStr(const metkit::mars::MarsRequest& request) {
    std::stringstream ss;
    std::string separator = "";
    std::vector<std::string> keys = request.params();
    std::sort(keys.begin(), keys.end());
    for(const auto& key : keys) {
        ss << separator << key << "=" << request[key];
        separator = ",";
    }
    return ss.str();
}

std::vector<metkit::mars::MarsRequest> flattenRequest(const metkit::mars::MarsRequest& request) {
    NOTIMP;
}

// Stringify requests, and flatten if necessary
typedef std::map<metkit::mars::MarsRequest, std::vector<std::string>> flattenedKeys_t;

flattenedKeys_t buildFlatKeys(const std::vector<metkit::mars::MarsRequest>& requests, bool flatten) {
    
    flattenedKeys_t keymap;
    
    for (const auto& baseRequest : requests) {

        keymap[baseRequest] = std::vector<std::string>();

        // Assume baseRequest has cardinality >= 1 and may need to be flattened
        if (flatten) {
            std::vector<metkit::mars::MarsRequest> flat = flattenRequest(baseRequest);
            for (const auto& r : flat) {
                keymap[baseRequest].push_back(requestToStr(r));
            }
        }

        // Assume baseRequest has cardinality 1
        else {
            keymap[baseRequest].push_back(requestToStr(baseRequest));
        }

        eckit::Log::debug<LibGribJump>() << "Flattened keys for request " << requestToStr(baseRequest) << ": " << keymap[baseRequest] << std::endl;
    }

    return keymap;
}

metkit::mars::MarsRequest unionRequest(const MarsRequests& requests) {

    /// @todo: we should do some check not to merge on keys like class and stream
    metkit::mars::MarsRequest unionRequest = requests.front();
    for(size_t i = 1; i < requests.size(); ++i) {
        unionRequest.merge(requests[i]);
    }
    
    LOG_DEBUG_LIB(LibGribJump) << "Union request " << unionRequest << std::endl;
    
    return unionRequest;
}


static std::string thread_id_str() {
    auto id = std::this_thread::get_id();
    std::stringstream ss;
    ss << id;
    return ss.str();
}

} // namespace 
//----------------------------------------------------------------------------------------------------------------------

// Class for queueing tasks from a single user's requests.

/// @todo rename and move to a separate file
class TaskGroup {
public:
    TaskGroup() {}

    ~TaskGroup() {
        for (auto& t : tasks_) {
            delete t;
        }
    }

    void notify(size_t taskid) {
        std::lock_guard<std::mutex> lock(m_);
        taskStatus_[taskid] = Task::Status::DONE;
        counter_++;
        cv_.notify_one();
    }

    void enqueueTask(Task* task) {
        taskStatus_.push_back(Task::Status::PENDING);
        WorkItem w(task);
        WorkQueue& queue = WorkQueue::instance();
        queue.push(w);
        LOG_DEBUG_LIB(LibGribJump) << "Queued task " <<  tasks_.size() << std::endl;
    }
    
    void waitForTasks(){
        ASSERT(taskStatus_.size() > 0);
        LOG_DEBUG_LIB(LibGribJump) << "Waiting for " << eckit::Plural(taskStatus_.size(), "task") << "..." << std::endl;
        std::unique_lock<std::mutex> lock(m_);
        cv_.wait(lock, [&]{return counter_ == taskStatus_.size();});
        LOG_DEBUG_LIB(LibGribJump) << "All tasks complete" << std::endl;
    }

private:

    int counter_ = 0;  //< incremented by notify() or notifyError()

    std::mutex m_;
    std::condition_variable cv_;
    
    std::vector<Task*> tasks_;
    std::vector<size_t> taskStatus_;
    std::vector<std::string> errors_; //< stores error messages, empty if no errors
};

//----------------------------------------------------------------------------------------------------------------------

class FileExtractionTask : public Task {
public:

    // Each extraction item is assumed to be for the same file.

    FileExtractionTask(TaskGroup& taskgroup, const size_t id, const eckit::PathName& fname, std::vector<ExtractionItem*>& extractionItems) :
        Task(id, nullptr), // todo, deal with the nullptr
        taskgroup_(taskgroup),
        fname_(fname),
        extractionItems_(extractionItems)
    {
    }


    void execute(GribJump& gj) override { // todo, don't need gj?
        // Timing info
        eckit::Timer timer;
        std::string thread_id = thread_id_str();
        eckit::Timer full_timer("Thread total time. Thread: " + thread_id);

        // Sort extractionItems_ by offset
        std::sort(extractionItems_.begin(), extractionItems_.end(), [](const ExtractionItem* a, const ExtractionItem* b) {
            return a->offset() < b->offset();
        });

        timer.reset("FileExtractionTask : Sorted offsets Thread: " + thread_id);

        // Do the extraction ... this should be in a separate class.
        extract();

        notify();
    }

    void extract() {
        std::vector<NewJumpInfo*> infos = getJumpInfos();
        eckit::FileHandle fh(fname_);

        fh.openForRead();

        for (size_t i = 0; i < infos.size(); i++) {
            NewJumpInfo& info = *infos[i];
            std::unique_ptr<Jumper> jumper(JumperFactory::instance().build(info)); // todo, dont build a new jumper for each info.
            jumper->extract(fh, info, *extractionItems_[i]);
        }

        fh.close();

    }

    std::vector<NewJumpInfo*> getJumpInfos() {
        // Get the info from the cache, or read from the file.
        /// @todo This should probably be a method of the cache class.

        // for now, always read from the file.

        grib_context* c = nullptr;
        int n = 0;
        off_t* offsets;
        int err = codes_extract_offsets_malloc(c, fname_.asString().c_str(), PRODUCT_GRIB, &offsets, &n, 1);
        ASSERT(!err);

        eckit::FileHandle fh(fname_);
        fh.openForRead();

        std::vector<NewJumpInfo*> infos;

        for (size_t i = 0; i < n; i++) {
            
            NewJumpInfo* info(InfoFactory::instance().build(fh, offsets[i]));
            ASSERT(info);
            infos.push_back(info);
        }

        fh.close();

        free(offsets);

        return infos;
    }


    void notify() override { //explicit override because we are not defining a client stream.
        taskgroup_.notify(id());
    }

    void notifyError(const std::string& s) override { //explicit override because we are not defining a client stream.
        NOTIMP;
    }

private:
    TaskGroup& taskgroup_;
    eckit::PathName fname_;
    std::vector<ExtractionItem*>& extractionItems_;
};


//----------------------------------------------------------------------------------------------------------------------

Engine::Engine() {
}

Engine::~Engine() {
}

Results Engine::extract(const MarsRequests& requests, const RangesList& ranges, bool flatten) {

    /*

    - Flatten the requests, and turn into alphabetical requests (for fdb keys later), MULTIREQ_TO_REQ1
        m1     : {a1}
        m2/3/4 : {a2, a3, a4}
    // this could be a vector of vectors of strings instead. (?)
       i=0, {a1}; i=1, {a2, a3, a4}; i=2, {a5, a6, a7, a8}; etc.

    - Create the 1-to-1 map, REQ1_TO_XRR
        a1 : ExtractionItem(a1, ranges1)
        a2 : ExtractionItem(a2, ranges2)
        a3 : ExtractionItem(a2, ranges2)
        a4 : ExtractionItem(a2, ranges2)  ...

    - Create the union request: M = m1/2/3/4/etc. // NB We probably don't want to actually union all keywords (e.g. date). Keep for now...

    - FDB.list(M) and iterate over list elements. Each element can be mapped to a key a1, a2, a3, a4, etc.
        -> uri = elem.location().uri()
        -> a = tostring(elem.key())
        -> map[a].setURI(uri)
    
    By here, ExtractionItem will contain the URI and ranges to extract the data.

    - Construct map of files to ExtractionItem. We Could be creating this map when we set the URI in the previous step.
        f1 : {XRR1, XRR2}
        f2 : {XRR3, XRR4, etc.}
    Ideally in order of offset.

    - Schedule the extraction of the data: a task per file.

    - Wait for the tasks to complete.

    - Return the results.
    NB: In terms of mapping the multi-request to the result, use the map MULTIREQ_TO_REQ1 to map the original request to the result.
    
    i.e. MULTIREQ_TO_REQ1[m1] is a1, REQ1_TO_XRR[a1] is XRR1
    so XRR1 == REQ1_TO_XRR[MULTIREQ_TO_REQ1[m1]]

    So if you want to collect the results into a vector like the original requests, you can do something like:
    for (auto& req : requests) {
        results.push_back(REQ1_TO_XRR[MULTIREQ_TO_REQ1[req]].result());
    }
    */

   // WIP

    typedef std::map<std::string, ExtractionItem*> keyToExItem_t;
    typedef std::map<eckit::PathName, std::vector<ExtractionItem*>> filemap_t;

    keyToExItem_t keyToXRR;

    flattenedKeys_t flatKeys = buildFlatKeys(requests, flatten); // Map from base request to {flattened keys}

    // Create the 1-to-1 map
    for (size_t i = 0; i < requests.size(); i++) {
        const metkit::mars::MarsRequest& basereq = requests[i]; 
        const std::vector<std::string> keys = flatKeys[basereq];
        for (const auto& key : keys) {
            ASSERT(keyToXRR.find(key) == keyToXRR.end()); /// @todo support duplicated requests?
            keyToXRR.emplace(key, new ExtractionItem(basereq, ranges[i])); // 1-to-1-map
        }
    }

    // Create the union request
    const metkit::mars::MarsRequest req = unionRequest(requests);
    
    // Map files to ExtractionItem
    filemap_t filemap = FDBLister::instance().fileMap(req, keyToXRR);

    // Schedule the extraction of the data. Probably in another class.
    {
        TaskGroup taskGroup;

        size_t counter = 0;
        for (auto& [fname, extractionItems] : filemap) {
            taskGroup.enqueueTask(new FileExtractionTask(taskGroup, counter++, fname, extractionItems));
        }
        
        // Wait for the tasks to complete
        taskGroup.waitForTasks();
    }

    std::cout << ">>> Tasks complete" << std::endl;

    // Create map of base request to vector of extraction items
    
    std::map<metkit::mars::MarsRequest, std::vector<ExtractionItem*>> reqToExtractionItems;

    for (auto& [key, ex] : keyToXRR) {
        reqToExtractionItems[ex->request()].push_back(ex);
    }

    // // print contents of map
    // for (auto& [req, exs] : reqToExtractionItems) {
    //     LOG_DEBUG_LIB(LibGribJump) << "Request: " << req << std::endl;
    //     for (auto& ex : exs) {
    //         ex->debug_print();
    //     }
    // }

    return reqToExtractionItems;

}


size_t taskid() {
    static size_t id = 0;
    return id++;
}

} // namespace gribjump
