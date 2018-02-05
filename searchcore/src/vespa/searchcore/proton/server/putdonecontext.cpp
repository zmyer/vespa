// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "putdonecontext.h"
#include <vespa/searchcore/proton/common/docid_limit.h>
#include <vespa/searchcore/proton/reference/i_gid_to_lid_change_handler.h>

namespace proton {

PutDoneContext::PutDoneContext(FeedToken token, IGidToLidChangeHandler &gidToLidChangeHandler,
                               const document::GlobalId &gid, uint32_t lid,
                               search::SerialNum serialNum, bool enableNotifyPut)
    : OperationDoneContext(std::move(token)),
      _lid(lid),
      _docIdLimit(nullptr),
      _gidToLidChangeHandler(gidToLidChangeHandler),
      _gid(gid),
      _serialNum(serialNum),
      _enableNotifyPut(enableNotifyPut)
{
}

PutDoneContext::~PutDoneContext()
{
    if (_docIdLimit != nullptr) {
        _docIdLimit->bumpUpLimit(_lid + 1);
    }
    if (_enableNotifyPut) {
        _gidToLidChangeHandler.notifyPutDone(_gid, _lid, _serialNum);
    }
}

}  // namespace proton
