// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "bucket.h"

namespace storage::spi::test {

// Helper functions used by unit tests

Bucket makeSpiBucket(document::BucketId bucketId, PartitionId partitionId);
Bucket makeSpiBucket(document::BucketId bucketId);

}
