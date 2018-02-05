// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include <vespa/document/bucket/bucketspace.h>

namespace document::test {

// Helper functions used by unit tests

BucketSpace makeBucketSpace();
BucketSpace makeBucketSpace(const vespalib::string &docTypeName);

}
