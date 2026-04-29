/*
 * (C) Copyright 2025- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 * In applying this licence, ECMWF does not waive the privileges and immunities
 * granted to it by virtue of its status as an intergovernmental organisation nor
 * does it submit to any jurisdiction.
 */

#include <unistd.h>

#include "eckit/config/Resource.h"
#include "eckit/net/Port.h"
#include "eckit/runtime/Application.h"

#include "MarsLister.h"

namespace marslister {

class MarsListerServerApp : public eckit::Application, public MarsListerServer {
public:

    MarsListerServerApp(int argc, char** argv) :
        eckit::Application(argc, argv, "MARS_LISTER_HOME"),
        MarsListerServer(eckit::net::Port("marsLister",
            eckit::Resource<int>("$MARS_LISTER_PORT", 9778))) {}

    ~MarsListerServerApp() {}

private:

    MarsListerServerApp(const MarsListerServerApp&);

    void run() override {
        unique();
        for (;;) {
            ::sleep(10);
        }
    }
};

}  // namespace marslister

int main(int argc, char** argv) {
    marslister::MarsListerServerApp app(argc, argv);
    app.start();
    return 0;
}
