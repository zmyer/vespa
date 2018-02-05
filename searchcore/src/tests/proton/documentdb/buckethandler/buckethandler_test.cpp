// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#include <vespa/log/log.h>
LOG_SETUP("buckethandler_test");
#include <vespa/searchcore/proton/server/buckethandler.h>
#include <vespa/searchcore/proton/server/ibucketstatechangedhandler.h>
#include <vespa/searchcore/proton/server/ibucketmodifiedhandler.h>
#include <vespa/searchcore/proton/test/test.h>
#include <vespa/persistence/spi/test.h>
#include <vespa/vespalib/testkit/testapp.h>

using namespace proton;
using document::BucketId;
using document::GlobalId;
using storage::spi::Bucket;
using storage::spi::BucketInfo;
using storage::spi::PartitionId;
using storage::spi::Timestamp;
using storage::spi::test::makeSpiBucket;
using vespalib::ThreadStackExecutor;
using proton::test::BucketStateCalculator;

const PartitionId PART_ID(0);
const GlobalId GID_1("111111111111");
const BucketId BUCKET_1(8, GID_1.convertToBucketId().getRawId());
const Timestamp TIME_1(1u);
const uint32_t DOCSIZE_1(4096u);

struct MySubDb
{
    DocumentMetaStore   _metaStore;
    test::UserDocuments _docs;
    MySubDb(std::shared_ptr<BucketDBOwner> bucketDB, SubDbType subDbType)
        : _metaStore(bucketDB,
                     DocumentMetaStore::getFixedName(),
                     search::GrowStrategy(),
                     documentmetastore::IGidCompare::SP(
                             new documentmetastore::DefaultGidCompare),
                     subDbType),
          _docs()
    {
    }
    void insertDocs(const test::UserDocuments &docs_) {
        _docs = docs_;
        for (test::UserDocuments::Iterator itr = _docs.begin(); itr != _docs.end(); ++itr) {
            const test::BucketDocuments &bucketDocs = itr->second;
            for (size_t i = 0; i < bucketDocs.getDocs().size(); ++i) {
                const test::Document &testDoc = bucketDocs.getDocs()[i];
                _metaStore.put(testDoc.getGid(), testDoc.getBucket(),
                               testDoc.getTimestamp(), testDoc.getDocSize(), testDoc.getLid());
            }
        }
    }
    BucketId bucket(uint32_t userId) {
        return _docs.getBucket(userId);
    }
    test::DocumentVector docs(uint32_t userId) {
        return _docs.getGidOrderDocs(userId);
    }
};


struct MyChangedHandler : public IBucketStateChangedHandler
{
    BucketId _bucket;
    BucketInfo::ActiveState _state;
    MyChangedHandler() : _bucket(), _state(BucketInfo::NOT_ACTIVE) {}

    virtual void notifyBucketStateChanged(const document::BucketId &bucketId,
                                          storage::spi::BucketInfo::ActiveState newState) override {
        _bucket = bucketId;
        _state = newState;
    }
};


struct MyModifiedHandler : public IBucketModifiedHandler
{
    virtual void
    notifyBucketModified(const BucketId &bucket) override
    {
        (void) bucket;
    }
};


bool
expectEqual(uint32_t docCount, uint32_t metaCount, size_t docSizes, size_t entrySizes, const BucketInfo &info)
{
    if (!EXPECT_EQUAL(docCount, info.getDocumentCount())) return false;
    if (!EXPECT_EQUAL(metaCount, info.getEntryCount())) return false;
    if (!EXPECT_EQUAL(docSizes, info.getDocumentSize())) return false;
    if (!EXPECT_EQUAL(entrySizes, info.getUsedSize())) return false;
    return true;
}


struct Fixture
{
    test::UserDocumentsBuilder      _builder;
    std::shared_ptr<BucketDBOwner>  _bucketDB;
    MySubDb                         _ready;
    MySubDb                         _removed;
    MySubDb                         _notReady;
    ThreadStackExecutor             _exec;
    BucketHandler                   _handler;
    MyChangedHandler                _changedHandler;
    MyModifiedHandler               _modifiedHandler;
    BucketStateCalculator::SP       _calc;
    test::BucketIdListResultHandler _bucketList;
    test::BucketInfoResultHandler   _bucketInfo;
    test::GenericResultHandler      _genResult;
    Fixture()
        : _builder(),
          _bucketDB(std::make_shared<BucketDBOwner>()),
          _ready(_bucketDB, SubDbType::READY),
          _removed(_bucketDB, SubDbType::REMOVED),
          _notReady(_bucketDB, SubDbType::NOTREADY),
          _exec(1, 64000),
          _handler(_exec),
          _changedHandler(),
          _modifiedHandler(),
          _calc(new BucketStateCalculator()),
          _bucketList(), _bucketInfo(), _genResult()
    {
        // bucket 2 & 3 & 4 & 7 in ready
        _ready.insertDocs(_builder.createDocs(2, 1, 3).  // 2 docs
                                   createDocs(3, 3, 6).  // 3 docs
                                   createDocs(4, 6, 10). // 4 docs
                                   createDocs(7, 10, 11). // 1 doc
                                   getDocs());
        // bucket 2 in removed
        _removed.insertDocs(_builder.clearDocs().
                                     createDocs(2, 16, 20). // 4 docs
                                     getDocs());
        // bucket 4 in not ready
        _notReady.insertDocs(_builder.clearDocs().
                                      createDocs(4, 22, 24). // 2 docs
                                      getDocs());
        _handler.setReadyBucketHandler(_ready._metaStore);
        _handler.addBucketStateChangedHandler(&_changedHandler);
        _handler.notifyClusterStateChanged(_calc);
    }
    ~Fixture()
    {
        _handler.removeBucketStateChangedHandler(&_changedHandler);
    }
    void sync() { _exec.sync(); }
    void handleGetBucketInfo(const BucketId &bucket) {
        _handler.handleGetBucketInfo(makeSpiBucket(bucket, PART_ID), _bucketInfo);
    }
    void
    setNodeUp(bool value)
    {
        _calc->setNodeUp(value);
        _handler.notifyClusterStateChanged(_calc);
    }
};


TEST_F("require that handleListBuckets() returns buckets from all sub dbs", Fixture)
{
    f._handler.handleListBuckets(f._bucketList);
    EXPECT_EQUAL(4u, f._bucketList.getList().size());
    EXPECT_EQUAL(f._ready.bucket(2), f._bucketList.getList()[0]);
    EXPECT_EQUAL(f._ready.bucket(3), f._bucketList.getList()[1]);
    EXPECT_EQUAL(f._ready.bucket(4), f._bucketList.getList()[2]);
    EXPECT_EQUAL(f._ready.bucket(7), f._bucketList.getList()[3]);
    EXPECT_EQUAL(f._removed.bucket(2), f._bucketList.getList()[0]);
    EXPECT_EQUAL(f._notReady.bucket(4), f._bucketList.getList()[2]);
}


TEST_F("require that bucket is reported in handleGetBucketInfo()", Fixture)
{
    f.handleGetBucketInfo(f._ready.bucket(3));
    EXPECT_TRUE(expectEqual(3, 3, 3000, 3000, f._bucketInfo.getInfo()));

    f.handleGetBucketInfo(f._ready.bucket(2)); // bucket 2 also in removed sub db
    EXPECT_TRUE(expectEqual(2, 6, 2000, 6000, f._bucketInfo.getInfo()));
}


TEST_F("require that handleGetBucketInfo() can get cached bucket", Fixture)
{
    {
        BucketDBOwner::Guard db = f._bucketDB->takeGuard();
        db->add(GID_1, BUCKET_1, TIME_1, DOCSIZE_1, SubDbType::READY);
        db->cacheBucket(BUCKET_1);
        db->add(GID_1, BUCKET_1, TIME_1, DOCSIZE_1, SubDbType::NOTREADY);
    }
    f.handleGetBucketInfo(BUCKET_1);
    EXPECT_TRUE(expectEqual(1, 1, DOCSIZE_1, DOCSIZE_1, f._bucketInfo.getInfo()));

    f._bucketDB->takeGuard()->uncacheBucket();

    f.handleGetBucketInfo(BUCKET_1);
    EXPECT_TRUE(expectEqual(2, 2, 2 * DOCSIZE_1, 2 * DOCSIZE_1, f._bucketInfo.getInfo()));
    {
        // Must ensure empty bucket db before destruction.
        BucketDBOwner::Guard db = f._bucketDB->takeGuard();
        db->remove(GID_1, BUCKET_1, TIME_1, DOCSIZE_1, SubDbType::READY);
        db->remove(GID_1, BUCKET_1, TIME_1, DOCSIZE_1, SubDbType::NOTREADY);
    }
}


TEST_F("require that changed handlers are notified when bucket state changes", Fixture)
{
    f._handler.handleSetCurrentState(f._ready.bucket(2), BucketInfo::ACTIVE, f._genResult);
    f.sync();
    EXPECT_EQUAL(f._ready.bucket(2), f._changedHandler._bucket);
    EXPECT_EQUAL(BucketInfo::ACTIVE, f._changedHandler._state);
    f._handler.handleSetCurrentState(f._ready.bucket(3), BucketInfo::NOT_ACTIVE, f._genResult);
    f.sync();
    EXPECT_EQUAL(f._ready.bucket(3), f._changedHandler._bucket);
    EXPECT_EQUAL(BucketInfo::NOT_ACTIVE, f._changedHandler._state);
}


TEST_F("require that unready bucket can be reported as active", Fixture)
{
    f._handler.handleSetCurrentState(f._ready.bucket(4),
                                     BucketInfo::ACTIVE, f._genResult);
    f.sync();
    EXPECT_EQUAL(f._ready.bucket(4), f._changedHandler._bucket);
    EXPECT_EQUAL(BucketInfo::ACTIVE, f._changedHandler._state);
    f.handleGetBucketInfo(f._ready.bucket(4));
    EXPECT_EQUAL(true, f._bucketInfo.getInfo().isActive());
    EXPECT_EQUAL(false, f._bucketInfo.getInfo().isReady());
}


TEST_F("require that node being down deactivates buckets", Fixture)
{
    f._handler.handleSetCurrentState(f._ready.bucket(2),
                                     BucketInfo::ACTIVE, f._genResult);
    f.sync();
    EXPECT_EQUAL(f._ready.bucket(2), f._changedHandler._bucket);
    EXPECT_EQUAL(BucketInfo::ACTIVE, f._changedHandler._state);
    f.handleGetBucketInfo(f._ready.bucket(2));
    EXPECT_EQUAL(true, f._bucketInfo.getInfo().isActive());
    f.setNodeUp(false);
    f.sync();
    f.handleGetBucketInfo(f._ready.bucket(2));
    EXPECT_EQUAL(false, f._bucketInfo.getInfo().isActive());
    f._handler.handleSetCurrentState(f._ready.bucket(2),
                                     BucketInfo::ACTIVE, f._genResult);
    f.sync();
    f.handleGetBucketInfo(f._ready.bucket(2));
    EXPECT_EQUAL(false, f._bucketInfo.getInfo().isActive());
    f.setNodeUp(true);
    f.sync();
    f.handleGetBucketInfo(f._ready.bucket(2));
    EXPECT_EQUAL(false, f._bucketInfo.getInfo().isActive());
    f._handler.handleSetCurrentState(f._ready.bucket(2),
                                     BucketInfo::ACTIVE, f._genResult);
    f.sync();
    f.handleGetBucketInfo(f._ready.bucket(2));
    EXPECT_EQUAL(true, f._bucketInfo.getInfo().isActive());
}


TEST_MAIN()
{
    TEST_RUN_ALL();
}

