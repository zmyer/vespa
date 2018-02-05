// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#include "dummy_metrics_manager.h"

namespace vespalib {
namespace metrics {

DummyMetricsManager::~DummyMetricsManager() {}

Snapshot
DummyMetricsManager::snapshot()
{
    Snapshot snap(0, 0);
    return snap;
}

Snapshot
DummyMetricsManager::totalSnapshot()
{
    Snapshot snap(0, 0);
    return snap;
}

} // namespace vespalib::metrics
} // namespace vespalib
