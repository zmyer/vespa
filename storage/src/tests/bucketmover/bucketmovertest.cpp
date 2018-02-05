// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include <vespa/storage/bucketdb/storbucketdb.h>
#include <vespa/storage/common/bucketmessages.h>
#include <vespa/storage/bucketmover/bucketmover.h>
#include <vespa/document/test/make_bucket_space.h>
#include <vespa/document/test/make_document_bucket.h>
#include <tests/common/dummystoragelink.h>
#include <tests/common/testhelper.h>
#include <tests/common/teststorageapp.h>
#include <vespa/config/common/exceptions.h>

bool debug = false;

using document::test::makeBucketSpace;
using document::test::makeDocumentBucket;

namespace storage {
namespace bucketmover {

struct BucketMoverTest : public CppUnit::TestFixture {
public:
    void setUp() override;
    void tearDown() override;

    void testNormalUsage();
    void testMaxPending();
    void testErrorHandling();

    CPPUNIT_TEST_SUITE(BucketMoverTest);
    CPPUNIT_TEST(testNormalUsage);
    CPPUNIT_TEST(testMaxPending);
    CPPUNIT_TEST(testErrorHandling);
    CPPUNIT_TEST_SUITE_END();

    std::unique_ptr<TestServiceLayerApp> _node;
    std::unique_ptr<ServiceLayerComponent> _component;
    std::unique_ptr<BucketMover> _bucketMover;
    DummyStorageLink* after;

private:
    void addBucket(const document::BucketId& id, uint16_t idealDiff);
};

CPPUNIT_TEST_SUITE_REGISTRATION(BucketMoverTest);

void
BucketMoverTest::tearDown()
{
    _node.reset(0);
}

void
BucketMoverTest::setUp()
{
    try {
        _node.reset(new TestServiceLayerApp(DiskCount(4)));
        _node->setupDummyPersistence();
    } catch (config::InvalidConfigException& e) {
        fprintf(stderr, "%s\n", e.what());
    }

    _component.reset(new ServiceLayerComponent(_node->getComponentRegister(), "foo"));
    _bucketMover.reset(new BucketMover("raw:", _node->getComponentRegister()));
    after = new DummyStorageLink();
    _bucketMover->push_back(StorageLink::UP(after));
}

void
BucketMoverTest::addBucket(const document::BucketId& id,
                           uint16_t idealDiff)
{
    StorBucketDatabase::WrappedEntry entry(
            _component->getBucketDatabase(makeBucketSpace()).get(
                    id,
                    "",
                    StorBucketDatabase::CREATE_IF_NONEXISTING));

    entry->setBucketInfo(api::BucketInfo(1,1,1));

    uint16_t idealDisk = _component->getIdealPartition(makeDocumentBucket(id));
    entry->disk = (idealDisk + idealDiff) % _component->getDiskCount();
    entry.write();
}

void
BucketMoverTest::testNormalUsage()
{
    for (uint32_t i = 1; i < 4; ++i) {
        addBucket(document::BucketId(16, i), 1);
    }
    for (uint32_t i = 4; i < 6; ++i) {
        addBucket(document::BucketId(16, i), 0);
    }

    _bucketMover->open();
    _bucketMover->tick();

    std::vector<api::StorageMessage::SP> msgs = after->getCommandsOnce();
    CPPUNIT_ASSERT_EQUAL(
            std::string("BucketDiskMoveCommand("
                        "BucketId(0x4000000000000002), source 3, target 2)"),
            msgs[0]->toString());
    CPPUNIT_ASSERT_EQUAL(
            std::string("BucketDiskMoveCommand("
                        "BucketId(0x4000000000000001), source 2, target 1)"),
            msgs[1]->toString());
    CPPUNIT_ASSERT_EQUAL(
            std::string("BucketDiskMoveCommand("
                        "BucketId(0x4000000000000003), source 1, target 0)"),
            msgs[2]->toString());

    for (uint32_t i = 0; i < 2; ++i) {
        after->sendUp(std::shared_ptr<api::StorageMessage>(
                              ((api::StorageCommand*)msgs[i].get())->
                              makeReply().release()));
    }

    _bucketMover->tick();
    CPPUNIT_ASSERT_EQUAL(0, (int)after->getNumCommands());

    _bucketMover->finishCurrentRun();
}

void
BucketMoverTest::testMaxPending()
{
    for (uint32_t i = 1; i < 100; ++i) {
        addBucket(document::BucketId(16, i), 1);
    }
    for (uint32_t i = 101; i < 200; ++i) {
        addBucket(document::BucketId(16, i), 0);
    }

    _bucketMover->open();
    _bucketMover->tick();

    std::vector<api::StorageMessage::SP> msgs = after->getCommandsOnce();
    // 5 is the max pending default config.
    CPPUNIT_ASSERT_EQUAL(5, (int)msgs.size());

    after->sendUp(std::shared_ptr<api::StorageMessage>(
                          ((api::StorageCommand*)msgs[3].get())->
                          makeReply().release()));

    _bucketMover->tick();

    std::vector<api::StorageMessage::SP> msgs2 = after->getCommandsOnce();
    CPPUNIT_ASSERT_EQUAL(1, (int)msgs2.size());
}

void
BucketMoverTest::testErrorHandling()
{
    for (uint32_t i = 1; i < 100; ++i) {
        addBucket(document::BucketId(16, i), 1);
    }
    for (uint32_t i = 101; i < 200; ++i) {
        addBucket(document::BucketId(16, i), 0);
    }

    _bucketMover->open();
    _bucketMover->tick();

    std::vector<api::StorageMessage::SP> msgs = after->getCommandsOnce();
    // 5 is the max pending default config.
    CPPUNIT_ASSERT_EQUAL(5, (int)msgs.size());

    BucketDiskMoveCommand& cmd = static_cast<BucketDiskMoveCommand&>(*msgs[0]);
    uint32_t targetDisk = cmd.getDstDisk();

    std::unique_ptr<api::StorageReply> reply(cmd.makeReply().release());
    reply->setResult(api::ReturnCode(api::ReturnCode::INTERNAL_FAILURE, "foobar"));
    after->sendUp(std::shared_ptr<api::StorageMessage>(reply.release()));

    for (uint32_t i = 1; i < msgs.size(); ++i) {
        after->sendUp(std::shared_ptr<api::StorageMessage>(
                              ((api::StorageCommand*)msgs[i].get())->
                              makeReply().release()));
    }

    _bucketMover->tick();

    std::vector<api::StorageMessage::SP> msgs2 = after->getCommandsOnce();
    CPPUNIT_ASSERT_EQUAL(5, (int)msgs2.size());

    for (uint32_t i = 0; i < msgs2.size(); ++i) {
        BucketDiskMoveCommand& bdm = static_cast<BucketDiskMoveCommand&>(*msgs2[i]);
        CPPUNIT_ASSERT(bdm.getDstDisk() != targetDisk);
    }
}

} // bucketmover
} // storage
