// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "frtconfigresponse.h"
#include <vespa/fnet/frt/rpcrequest.h>

namespace config {

FRTConfigResponse::FRTConfigResponse(FRT_RPCRequest * request)
    : _request(request),
      _responseState(EMPTY),
      _returnValues(_request->GetReturn())
{
    _request->AddRef();
}

FRTConfigResponse::~FRTConfigResponse()
{
    _request->SubRef();
}

bool
FRTConfigResponse::validateResponse()
{
    if (_request->IsError())
        _responseState = ERROR;
    if (_request->GetReturn()->GetNumValues() == 0)
        _responseState = EMPTY;
    if (_request->CheckReturnTypes(getResponseTypes().c_str())) {
        _returnValues = _request->GetReturn();
        _responseState = OK;
    }
    return (_responseState == OK);
}

bool
FRTConfigResponse::hasValidResponse() const
{
    return (_responseState == OK);
}

vespalib::string FRTConfigResponse::errorMessage() const { return _request->GetErrorMessage(); }
int FRTConfigResponse::errorCode() const { return _request->GetErrorCode(); }
bool FRTConfigResponse::isError() const { return _request->IsError(); }

} // namespace config
