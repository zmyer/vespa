// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "thread.h"
#include <vespa/vespalib/util/sync.h>

namespace storage {
namespace framework {

void
Thread::interruptAndJoin(vespalib::Monitor* m)
{
    interrupt();
    if (m != 0) {
        vespalib::MonitorGuard monitorGuard(*m);
        monitorGuard.broadcast();
    }
    join();
}

void
Thread::interruptAndJoin(std::mutex &m, std::condition_variable &cv)
{
    interrupt();
    {
        std::lock_guard<std::mutex> guard(m);
        cv.notify_all();
    }
    join();
}

} // framework
} // storage
