// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#include <vespa/vespalib/testkit/test_kit.h>

#include <vespa/searchlib/common/fileheadercontext.h>
#include <vespa/searchlib/docstore/filechunk.h>
#include <vespa/searchlib/docstore/writeablefilechunk.h>
#include <vespa/searchlib/test/directory_handler.h>
#include <vespa/vespalib/test/insertion_operators.h>
#include <vespa/vespalib/util/threadstackexecutor.h>
#include <iomanip>
#include <iostream>

#include <vespa/log/log.h>
#include <vespa/vespalib/util/compressionconfig.h>

LOG_SETUP("file_chunk_test");

using namespace search;

using common::FileHeaderContext;
using vespalib::ThreadStackExecutor;

struct MyFileHeaderContext : public FileHeaderContext {
    virtual void addTags(vespalib::GenericHeader &header, const vespalib::string &name) const override {
        (void) header;
        (void) name;
    }
};

struct SetLidObserver : public ISetLid {
    std::vector<uint32_t> lids;
    virtual void setLid(const vespalib::LockGuard &guard, uint32_t lid, const LidInfo &lidInfo) override {
        (void) guard;
        (void) lidInfo;
        lids.push_back(lid);
    }
};

struct BucketizerObserver : public IBucketizer {
    mutable std::vector<uint32_t> lids;
    virtual document::BucketId getBucketOf(const vespalib::GenerationHandler::Guard &guard, uint32_t lid) const override {
        (void) guard;
        lids.push_back(lid);
        return document::BucketId();
    }
    virtual vespalib::GenerationHandler::Guard getGuard() const override {
        return vespalib::GenerationHandler::Guard();
    }
};

vespalib::string
getData(uint32_t lid)
{
    std::ostringstream oss;
    oss << "data_" << std::setw(5) << std::setfill('0') << lid;
    return oss.str();
}

struct FixtureBase {
    test::DirectoryHandler dir;
    ThreadStackExecutor executor;
    uint64_t serialNum;
    TuneFileSummary tuneFile;
    MyFileHeaderContext fileHeaderCtx;
    vespalib::Lock updateLock;
    SetLidObserver lidObserver;
    BucketizerObserver bucketizer;

    uint64_t nextSerialNum() {
        return serialNum++;
    };

    FixtureBase(const vespalib::string &baseName,
                bool dirCleanup = true)
        : dir(baseName),
          executor(1, 0x10000),
          serialNum(1),
          tuneFile(),
          fileHeaderCtx(),
          updateLock(),
          lidObserver(),
          bucketizer()
    {
        dir.cleanup(dirCleanup);
    }
    ~FixtureBase() {}
    void assertLidMap(const std::vector<uint32_t> &expLids) {
        EXPECT_EQUAL(expLids, lidObserver.lids);
    }
    void assertBucketizer(const std::vector<uint32_t> &expLids) {
        EXPECT_EQUAL(expLids, bucketizer.lids);
    }
};

struct ReadFixture : public FixtureBase {
    FileChunk chunk;

    ReadFixture(const vespalib::string &baseName,
                 bool dirCleanup = true)
        : FixtureBase(baseName, dirCleanup),
          chunk(FileChunk::FileId(0),
                FileChunk::NameId(1234),
                baseName,
                tuneFile,
                &bucketizer,
                false)
    {
        dir.cleanup(dirCleanup);
    }
    void updateLidMap(uint32_t docIdLimit) {
        vespalib::LockGuard guard(updateLock);
        chunk.updateLidMap(guard, lidObserver, serialNum, docIdLimit);
    }
};

struct WriteFixture : public FixtureBase {
    WriteableFileChunk chunk;
    using CompressionConfig = vespalib::compression::CompressionConfig;

    WriteFixture(const vespalib::string &baseName,
                 uint32_t docIdLimit,
                 bool dirCleanup = true)
        : FixtureBase(baseName, dirCleanup),
          chunk(executor,
                FileChunk::FileId(0),
                FileChunk::NameId(1234),
                baseName,
                serialNum,
                docIdLimit,
                WriteableFileChunk::Config(CompressionConfig(), 0x1000),
                tuneFile,
                fileHeaderCtx,
                &bucketizer,
                false)
    {
        dir.cleanup(dirCleanup);
    }
    void flush() {
        chunk.flush(true, serialNum);
        chunk.flushPendingChunks(serialNum);
    }
    WriteFixture &append(uint32_t lid) {
        vespalib::string data = getData(lid);
        chunk.append(nextSerialNum(), lid, data.c_str(), data.size());
        return *this;
    }
    void updateLidMap(uint32_t docIdLimit) {
        vespalib::LockGuard guard(updateLock);
        chunk.updateLidMap(guard, lidObserver, serialNum, docIdLimit);
    }

};

TEST_F("require that idx file without docIdLimit in header can be read by FileChunk",
       ReadFixture(TEST_PATH("without_doc_id_limit"), false))
{
    EXPECT_EQUAL(std::numeric_limits<uint32_t>::max(), f.chunk.getDocIdLimit());
}

TEST_F("require that idx file without docIdLimit in header can be read by WriteableFileChunk",
       WriteFixture(TEST_PATH("without_doc_id_limit"), 1000, false))
{
    EXPECT_EQUAL(std::numeric_limits<uint32_t>::max(), f.chunk.getDocIdLimit());
}

TEST("require that docIdLimit is written to and read from idx file header")
{
    {
        WriteFixture f("tmp", 1000, false);
        EXPECT_EQUAL(1000u, f.chunk.getDocIdLimit());
    }
    {
        ReadFixture f("tmp", false);
        f.updateLidMap(std::numeric_limits<uint32_t>::max()); // trigger reading of idx file header
        EXPECT_EQUAL(1000u, f.chunk.getDocIdLimit());
    }
    {
        WriteFixture f("tmp", 0);
        EXPECT_EQUAL(1000u, f.chunk.getDocIdLimit());
    }
}

template <typename FixtureType>
void
assertUpdateLidMap(FixtureType &f)
{
    std::vector<uint32_t> expLids({1,10,100,999,998,999});
    f.assertLidMap(expLids);
    f.assertBucketizer(expLids);
    size_t entrySize = 10 + 8;
    EXPECT_EQUAL(9 * entrySize, f.chunk.getAddedBytes());
    EXPECT_EQUAL(3u, f.chunk.getBloatCount());
    EXPECT_EQUAL(3 * entrySize, f.chunk.getErasedBytes());
}

TEST("require that entries with lid >= docIdLimit are skipped in updateLidMap()")
{
    {
        WriteFixture f("tmp", 0, false);
        f.append(1).append(10).append(100).append(999).append(1000).append(1001).append(998).append(1002).append(999);
        f.flush();
    }
    {
        ReadFixture f("tmp", false);
        f.updateLidMap(1000);
        assertUpdateLidMap(f);
    }
    {
        WriteFixture f("tmp", 0);
        f.updateLidMap(1000);
        assertUpdateLidMap(f);
    }
}

using vespalib::compression::CompressionConfig;

TEST("require that operator == detects inequality") {
    using C = WriteableFileChunk::Config;
    EXPECT_TRUE(C() == C());
    EXPECT_TRUE(C({}, 1) == C({}, 1));
    EXPECT_FALSE(C({}, 2) == C({}, 1));
    EXPECT_FALSE(C({}, 1) == C({}, 2));
    EXPECT_FALSE(C({CompressionConfig::LZ4, 9, 60}, 2) == C({}, 2));
}

TEST_MAIN() { TEST_RUN_ALL(); }

