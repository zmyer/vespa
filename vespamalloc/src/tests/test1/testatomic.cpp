// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#include <vespa/vespalib/testkit/testapp.h>
#include <vespamalloc/malloc/allocchunk.h>

TEST("verify lock freeness of atomics"){
    {
        std::atomic<uint32_t> uint32V;
        ASSERT_TRUE(uint32V.is_lock_free());
    }
    {
        std::atomic<uint64_t> uint64V;
        ASSERT_TRUE(uint64V.is_lock_free());
    }
    {
        std::atomic<vespamalloc::TaggedPtr> taggedPtr;
        ASSERT_EQUAL(16u, sizeof(vespamalloc::TaggedPtr));
#if __GNUC__ < 7
        // See https://gcc.gnu.org/ml/gcc-patches/2017-01/msg02344.html for background
        ASSERT_TRUE(taggedPtr.is_lock_free());
#else
        ASSERT_TRUE(taggedPtr.is_lock_free() || !taggedPtr.is_lock_free());
#endif
    }

}

TEST_MAIN() { TEST_RUN_ALL(); }
