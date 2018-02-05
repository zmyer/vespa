// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "splitoperation.h"
#include <vespa/storage/distributor/idealstatemanager.h>
#include <vespa/storage/common/bucketoperationlogger.h>
#include <vespa/storageapi/message/bucketsplitting.h>
#include <vespa/storage/distributor/distributor_bucket_space.h>
#include <climits>

#include <vespa/log/bufferedlogger.h>
LOG_SETUP(".distributor.operation.idealstate.split");

using namespace storage::distributor;

SplitOperation::SplitOperation(const std::string& clusterName, const BucketAndNodes& nodes,
                               uint32_t maxBits, uint32_t splitCount, uint32_t splitSize)
    : IdealStateOperation(nodes),
      _tracker(clusterName),
      _maxBits(maxBits),
      _splitCount(splitCount),
      _splitSize(splitSize)
{}
SplitOperation::~SplitOperation() {}

void
SplitOperation::onStart(DistributorMessageSender& sender)
{
    _ok = false;

    BucketDatabase::Entry entry = _bucketSpace->getBucketDatabase().get(getBucketId());

    for (uint32_t i = 0; i < entry->getNodeCount(); i++) {
        std::shared_ptr<api::SplitBucketCommand> msg(
                new api::SplitBucketCommand(getBucket()));
        msg->setMaxSplitBits(_maxBits);
        msg->setMinDocCount(_splitCount);
        msg->setMinByteSize(_splitSize);
        msg->setTimeout(INT_MAX);
        setCommandMeta(*msg);
        _tracker.queueCommand(msg, entry->getNodeRef(i).getNode());
        _ok = true;
    }

    if (!_ok) {
        LOGBP(debug, "Unable to split bucket %s, since no copies are available (some in maintenance?)", getBucketId().toString().c_str());
        done();
    } else {
        _tracker.flushQueue(sender);
    }
}

void
SplitOperation::onReceive(DistributorMessageSender&, const api::StorageReply::SP& msg)
{
    api::SplitBucketReply& rep = static_cast<api::SplitBucketReply&>(*msg);

    uint16_t node = _tracker.handleReply(rep);

    if (node == 0xffff) {
        LOG(debug, "Ignored reply since node was max uint16_t for unknown "
                   "reasons");
        return;
    }

    std::ostringstream ost;

    if (rep.getResult().success()) {
        BucketDatabase::Entry entry =
            _bucketSpace->getBucketDatabase().get(rep.getBucketId());

        if (entry.valid()) {
            entry->removeNode(node);

            if (entry->getNodeCount() == 0) {
                LOG(spam, "Removing split bucket %s",
                    getBucketId().toString().c_str());
                _bucketSpace->getBucketDatabase().remove(rep.getBucketId());
            } else {
                _bucketSpace->getBucketDatabase().update(entry);
            }

            ost << getBucketId() << " => ";
        }

        // Add new buckets.
        for (uint32_t i = 0; i < rep.getSplitInfo().size(); i++) {
            const api::SplitBucketReply::Entry& sinfo = rep.getSplitInfo()[i];

            if (!sinfo.second.valid()) {
                LOG(error, "Received invalid bucket %s from node %d as reply "
                           "to split bucket",
                    sinfo.first.toString().c_str(), node);
            }

            ost << sinfo.first << ",";

            BucketCopy copy(
                    BucketCopy(_manager->getDistributorComponent().getUniqueTimestamp(),
                        node,
                        sinfo.second));

            // Must reset trusted since otherwise trustedness of inconsistent
            // copies would be arbitrarily determined by which copy managed
            // to finish its split first.
            _manager->getDistributorComponent().updateBucketDatabase(
                    document::Bucket(msg->getBucket().getBucketSpace(), sinfo.first), copy,
                    (DatabaseUpdate::CREATE_IF_NONEXISTING
                     | DatabaseUpdate::RESET_TRUSTED));

            LOG_BUCKET_OPERATION_NO_LOCK(
                    sinfo.first, vespalib::make_string(
                        "Split from bucket %s: %s",
                        getBucketId().toString().c_str(),
                        copy.toString().c_str()));
        }
    } else if (
            rep.getResult().getResult() == api::ReturnCode::BUCKET_NOT_FOUND
            && _bucketSpace->getBucketDatabase().get(rep.getBucketId())->getNode(node) != 0)
    {
        _manager->getDistributorComponent().recheckBucketInfo(node, getBucket());
        LOGBP(debug, "Split failed for %s: bucket not found. Storage and "
                     "distributor bucket databases might be out of sync: %s",
              getBucketId().toString().c_str(),
              rep.getResult().getMessage().c_str());
        _ok = false;
    } else if (rep.getResult().isBusy()) {
        LOG(debug, "Split failed for %s, node was busy. Will retry later",
            getBucketId().toString().c_str());
        _ok = false;
    } else if (rep.getResult().isCriticalForMaintenance()) {
        LOGBP(warning, "Split failed for %s: %s with error '%s'",
              getBucketId().toString().c_str(), msg->toString().c_str(),
              msg->getResult().toString().c_str());
        _ok = false;
    } else {
        LOG(debug, "Split failed for %s with non-critical failure: %s",
            getBucketId().toString().c_str(),
            rep.getResult().toString().c_str());
    }
#ifdef ENABLE_BUCKET_OPERATION_LOGGING
    if (_ok) {
        LOG_BUCKET_OPERATION_NO_LOCK(
                getBucketId(), vespalib::make_string(
                        "Split OK on node %d: %s. Finished: %s",
                        node, ost.str().c_str(),
                        _tracker.finished() ? "yes" : "no"));
    } else {
        LOG_BUCKET_OPERATION_NO_LOCK(
                getBucketId(), vespalib::make_string(
                        "Split FAILED on node %d: %s. Finished: %s",
                        node, rep.getResult().toString().c_str(),
                        _tracker.finished() ? "yes" : "no"));
    }
#endif

    if (_tracker.finished()) {
        LOG(debug, "Split done on node %d: %s completed operation",
            node, ost.str().c_str());
        done();
    } else {
        LOG(debug, "Split done on node %d: %s still pending on other nodes",
            node, ost.str().c_str());
    }
}

bool
SplitOperation::isBlocked(const PendingMessageTracker& tracker) const
{
    return checkBlockForAllNodes(getBucket(), tracker);
}

bool
SplitOperation::shouldBlockThisOperation(uint32_t msgType,
                                         uint8_t pri) const
{
    if (msgType == api::MessageType::SPLITBUCKET_ID && _priority >= pri) {
        return true;
    }
    if (msgType == api::MessageType::JOINBUCKETS_ID) {
        return true;
    }

    return false;
}
