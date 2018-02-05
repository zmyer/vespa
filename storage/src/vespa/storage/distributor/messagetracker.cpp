// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "messagetracker.h"
#include <vespa/storageapi/messageapi/bucketcommand.h>
#include <vespa/storageapi/messageapi/bucketreply.h>

#include <vespa/log/log.h>
LOG_SETUP(".messagetracker");

namespace storage::distributor {

MessageTracker::MessageTracker(const std::string& clusterName)
    : _clusterName(clusterName)
{}

MessageTracker::~MessageTracker() = default;

void
MessageTracker::flushQueue(MessageSender& sender)
{
    for (uint32_t i = 0; i < _commandQueue.size(); i++) {
        _commandQueue[i]._msg->setAddress(
                api::StorageMessageAddress(_clusterName, lib::NodeType::STORAGE, _commandQueue[i]._target));
        _sentMessages[_commandQueue[i]._msg->getMsgId()] = _commandQueue[i]._target;
        sender.sendCommand(_commandQueue[i]._msg);
    }

    _commandQueue.clear();
}

uint16_t
MessageTracker::handleReply(api::BucketReply& reply)
{
    std::map<uint64_t, uint16_t>::iterator found = _sentMessages.find(reply.getMsgId());
    if (found == _sentMessages.end()) {
        LOG(warning, "Received reply %zu for callback which we have no recollection of", reply.getMsgId());
        return (uint16_t)-1;
    } else {
        uint16_t node = found->second;
        _sentMessages.erase(found);
        return node;
    }
}

bool
MessageTracker::finished()
{
    return _sentMessages.empty();
}

}
