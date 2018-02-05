// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include <vespa/storage/common/messagesender.h>

namespace storage::lib { class NodeType; }
namespace storage::distributor {

class PendingMessageTracker;

class DistributorMessageSender : public MessageSender {
public:
    /**
      Sends the storage command to the given node,
      returns message id.
     */
    virtual uint64_t sendToNode(const lib::NodeType& nodeType, uint16_t node,
                                const std::shared_ptr<api::StorageCommand>& cmd, bool useDocumentAPI = false);

    virtual int getDistributorIndex() const = 0;
    virtual const std::string& getClusterName() const = 0;
    virtual const PendingMessageTracker& getPendingMessageTracker() const = 0;
};

}
