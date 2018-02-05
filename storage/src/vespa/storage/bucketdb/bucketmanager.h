// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
/**
 * @class storage::BucketManager
 * @ingroup bucketdb
 *
 * @brief Storage link handling requests concerning buckets.
 *
 * @author H�kon Humberset
 * @date 2006-01-16
 * @version $Id$
 */

#pragma once

#include "bucketmanagermetrics.h"
#include "storbucketdb.h"
#include <vespa/config/subscription/configuri.h>
#include <vespa/storage/bucketdb/config-stor-bucketdb.h>
#include <vespa/storage/common/bucketmessages.h>
#include <vespa/storage/common/servicelayercomponent.h>
#include <vespa/storage/common/storagelinkqueued.h>
#include <vespa/storageapi/message/bucket.h>
#include <vespa/storageframework/generic/metric/metricupdatehook.h>
#include <vespa/storageframework/generic/status/statusreporter.h>

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <condition_variable>

namespace storage {

class BucketManager : public StorageLinkQueued,
                      public framework::StatusReporter,
                      private framework::Runnable,
                      private framework::MetricUpdateHook
{
public:
    /** Type used for message queues */
    using CommandList = std::list<std::shared_ptr<api::StorageCommand>>;
    using BucketInfoRequestList = std::list<std::shared_ptr<api::RequestBucketInfoCommand>>;
    using BucketInfoRequestMap = std::unordered_map<document::BucketSpace, BucketInfoRequestList, document::BucketSpace::hash>;

private:
    config::ConfigUri _configUri;

    uint32_t _chunkLevel;
    BucketInfoRequestMap _bucketInfoRequests;

    /**
     * We have our own thread running, which we use to send messages down.
     * Take worker lock, add to list and signal for messages to be sent.
     */
    mutable std::mutex      _workerLock;
    std::condition_variable _workerCond;
    /**
     * Lock kept for access to 3 values below concerning cluster state.
     */
    std::mutex         _clusterStateLock;

    mutable std::mutex _queueProcessingLock;
    using ReplyQueue = std::vector<api::StorageReply::SP>;
    using ConflictingBuckets = std::unordered_set<document::BucketId,
                                                  document::BucketId::hash>;
    ReplyQueue _queuedReplies;
    ConflictingBuckets _conflictingBuckets;
    /**
     * Keeps the version number of the first cluster state version seen that
     * after distributor unification is equal to all cluster states seen after.
     */
    uint32_t _firstEqualClusterStateVersion;
    /**
     * The last cluster state version seen. We must ensure we dont answer to
     * cluster states we haven't seen.
     */
    uint32_t _lastClusterStateSeen;
    /**
     * The unified version of the last cluster state.
     */
    std::string _lastUnifiedClusterState;
    std::shared_ptr<BucketManagerMetrics> _metrics;
    bool _doneInitialized;
    size_t _requestsCurrentlyProcessing;
    ServiceLayerComponent _component;
    framework::Thread::UP _thread;

    BucketManager(const BucketManager&);
    BucketManager& operator=(const BucketManager&);

    class ScopedQueueDispatchGuard {
        BucketManager& _mgr;
    public:
        ScopedQueueDispatchGuard(BucketManager&);
        ~ScopedQueueDispatchGuard();

        ScopedQueueDispatchGuard(const ScopedQueueDispatchGuard&) = delete;
        ScopedQueueDispatchGuard& operator=(const ScopedQueueDispatchGuard&) = delete;
    };

public:
    explicit BucketManager(const config::ConfigUri&,
                           ServiceLayerComponentRegister&);
    ~BucketManager();

    void startWorkerThread();
    void print(std::ostream& out, bool verbose, const std::string& indent) const override;

    /** Dump the whole database to the given output. Use for debugging. */
    void dump(std::ostream& out) const;

    /** Get info for given bucket (Used for whitebox testing) */
    StorBucketDatabase::Entry getBucketInfo(const document::Bucket &id) const;

private:
    friend class BucketManagerTest;

    void run(framework::ThreadHandle&) override;

        // Status::Reporter implementation
    vespalib::string getReportContentType(const framework::HttpUrlPath&) const override;
    bool reportStatus(std::ostream&, const framework::HttpUrlPath&) const override;

    /** Event saying node is up and running. We can start to build cache. */
    void onOpen() override;
    void onDoneInit() override { _doneInitialized = true; }
    void onClose() override;
    void onFlush(bool downwards) override;

    void updateMetrics(bool updateDocCount);
    void updateMetrics(const MetricLockGuard &) override { updateMetrics(true); }
    void updateMinUsedBits();

    bool onRequestBucketInfo(const std::shared_ptr<api::RequestBucketInfoCommand>&) override;
    bool processRequestBucketInfoCommands(document::BucketSpace bucketSpace,
                                          BucketInfoRequestList &reqs);

    /**
     * Enqueue reply and add its bucket to the set of conflicting buckets iff
     * a RequestBucketInfo command is currently being processed.
     *
     * Returns whether request was enqueued (and should thus not be forwarded
     * by the caller).
     */
    bool enqueueAsConflictIfProcessingRequest(
            const api::StorageReply::SP& reply);

    /**
     * Signals that code is entering a section where certain bucket tree
     * modifying replies must be enqueued to prevent distributor bucket DB
     * inconsistencies. This does not model a regular mutex; multiple threads
     * concurrently calling this function will not be blocked on each other.
     *
     * A call must always be paired with exactly one subsequent call of
     * leaveQueueProtectedSection()
     *
     * Calls to this function nest so that the queue dispatch only happens
     * when a matching number of calls to leaveQueueProtectedSection have
     * taken place.
     */
    void enterQueueProtectedSection();
    /**
     * Leaves the current protected section and atomically dispatches any and
     * all queued replies iff no threads are in a protected section after this
     * has been done.
     *
     * Precondition: enterQueueProtectedSection must have been called earlier.
     */
    void leaveQueueProtectedSection(ScopedQueueDispatchGuard&);

    /**
     * Used by tests to synchronize against worker thread, as it is not
     * otherwise directly visible to other threads when it's processing
     * requests.
     *
     * Function is thread safe.
     *
     * Precondition: _queueProcessingLock must NOT be held.
     */
    size_t bucketInfoRequestsCurrentlyProcessing() const noexcept;

    /**
     * A bucket is said to have conflicts if a reply has been received that
     * somehow changes that bucket in the bucket tree (split, join or delete)
     * while a bucket info request is ongoing. Such replies must be queued up
     * in order to prevent them from arriving in the wrong order at the
     * distributor relative to the conflicting reply.
     *
     * During bucket info requests, we maintain a temporary conflict set against
     * which all put, remove and update replies are checked. These will be
     * dequeued together with the reply that caused the conflict as soon as the
     * bucket info request is done, ensuring replies are in the original
     * execution order.
     *
     * Not thread safe.
     */
    bool bucketHasConflicts(const document::BucketId& bucket) const noexcept {
        return (_conflictingBuckets.find(bucket) != _conflictingBuckets.end());
    }

    /**
     * Checks whether at least one of the reply's bucket ID or the original
     * (in case of remappings) bucket ID match a bucket in the conflict set.
     *
     * Not thread safe.
     */
    bool replyConflictsWithConcurrentOperation(
            const api::BucketReply& reply) const;

    bool enqueueIfBucketHasConflicts(const api::BucketReply::SP& reply);

    bool onUp(const std::shared_ptr<api::StorageMessage>&) override;
    bool onSetSystemState(
            const std::shared_ptr<api::SetSystemStateCommand>&) override;
    bool onCreateBucket(
            const std::shared_ptr<api::CreateBucketCommand>&) override;
    bool onMergeBucket(
            const std::shared_ptr<api::MergeBucketCommand>&) override;
    bool onRemove(
            const std::shared_ptr<api::RemoveCommand>&) override;
    bool onRemoveReply(
            const std::shared_ptr<api::RemoveReply>&) override;
    bool onPut(
            const std::shared_ptr<api::PutCommand>&) override;
    bool onPutReply(
            const std::shared_ptr<api::PutReply>&) override;
    bool onUpdate(
            const std::shared_ptr<api::UpdateCommand>&) override;
    bool onUpdateReply(
            const std::shared_ptr<api::UpdateReply>&) override;
    bool onNotifyBucketChangeReply(
           const std::shared_ptr<api::NotifyBucketChangeReply>&) override;

    bool verifyAndUpdateLastModified(api::StorageCommand& cmd,
                                     const document::Bucket& bucket,
                                     uint64_t lastModified);
    bool onSplitBucketReply(
            const std::shared_ptr<api::SplitBucketReply>&) override;
    bool onJoinBucketsReply(
            const std::shared_ptr<api::JoinBucketsReply>&) override;
    bool onDeleteBucketReply(
            const std::shared_ptr<api::DeleteBucketReply>&) override;
};

} // storage

