// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "datasetcollection.h"
#include "fnet_dataset.h"
#include <vespa/searchcore/fdispatch/common/search.h>
#include <vespa/fnet/fnet.h>

#include <vespa/log/log.h>
LOG_SETUP(".search.datasetcollection");

FastS_DataSetBase *
FastS_DataSetCollection::CreateDataSet(FastS_DataSetDesc *desc)
{
    FastS_DataSetBase *ret = NULL;

    FNET_Transport *transport = _appCtx->GetFNETTransport();
    FNET_Scheduler *scheduler = _appCtx->GetFNETScheduler();
    if (transport != NULL && scheduler != NULL) {
        ret = new FastS_FNET_DataSet(transport, scheduler, _appCtx, desc);
    } else {
        LOG(error, "Non-available dataset transport: FNET");
    }
    return ret;
}


bool
FastS_DataSetCollection::AddDataSet(FastS_DataSetDesc *desc)
{
    uint32_t datasetid = desc->GetID();

    if (datasetid >= _datasets_size) {
        uint32_t newSize = datasetid + 1;

        FastS_DataSetBase **newArray = new FastS_DataSetBase*[newSize];
        FastS_assert(newArray != NULL);

        uint32_t i;
        for (i = 0; i < _datasets_size; i++)
            newArray[i] = _datasets[i];

        for (; i < newSize; i++)
            newArray[i] = NULL;

        delete [] _datasets;
        _datasets = newArray;
        _datasets_size = newSize;
    }
    FastS_assert(_datasets[datasetid] == NULL);
    FastS_DataSetBase *dataset = CreateDataSet(desc);
    if (dataset == NULL)
        return false;
    _datasets[datasetid] = dataset;

    for (FastS_EngineDesc *engineDesc = desc->GetEngineList();
         engineDesc != NULL; engineDesc = engineDesc->GetNext()) {

        dataset->AddEngine(engineDesc);
    }
    dataset->ConfigDone(this);
    return true;
}



FastS_DataSetCollection::FastS_DataSetCollection(FastS_AppContext *appCtx)
    : _nextOld(NULL),
      _configDesc(NULL),
      _appCtx(appCtx),
      _datasets(NULL),
      _datasets_size(0),
      _gencnt(0),
      _frozen(false),
      _error(false)
{
}


FastS_DataSetCollection::~FastS_DataSetCollection()
{
    if (_datasets != NULL) {
        for (uint32_t i = 0; i < _datasets_size; i++) {
            if (_datasets[i] != NULL) {
                _datasets[i]->Free();
                _datasets[i] = NULL;
            }
        }
    }

    delete [] _datasets;
    delete _configDesc;
}


bool
FastS_DataSetCollection::Configure(FastS_DataSetCollDesc *cfgDesc,
                                   uint32_t gencnt)
{
    bool rc = false;

    if (_frozen) {
        delete cfgDesc;
    } else {
        FastS_assert(_configDesc == NULL);
        if (cfgDesc == NULL) {
            _configDesc = new FastS_DataSetCollDesc();
        } else {
            _configDesc = cfgDesc;
        }
        _gencnt     = gencnt;
        _frozen     = true;
        _error      = !_configDesc->Freeze();
        rc          = !_error;

        for (uint32_t i = 0; rc && i < _configDesc->GetMaxNumDataSets(); i++) {
            FastS_DataSetDesc *datasetDesc = _configDesc->GetDataSet(i);
            if (datasetDesc != NULL) {
                FastS_assert(datasetDesc->GetID() == i);
                rc = AddDataSet(datasetDesc);
            }
        }

        _error      = !rc;
    }
    return rc;
}


uint32_t
FastS_DataSetCollection::SuggestDataSet()
{
    FastS_assert(_frozen);

    FastS_DataSetBase *dataset = NULL;

    for (uint32_t i = 0; i < _datasets_size; i++) {
        FastS_DataSetBase *tmp = _datasets[i];
        if (tmp == NULL || tmp->_unitrefcost == 0)
            continue;

        // NB: cost race condition

        if (dataset == NULL ||
            dataset->_totalrefcost + dataset->_unitrefcost >
            tmp->_totalrefcost + tmp->_unitrefcost)
            dataset = tmp;
    }

    return (dataset == NULL)
                     ? FastS_NoID32()
                     : dataset->GetID();
}


FastS_DataSetBase *
FastS_DataSetCollection::GetDataSet(uint32_t datasetid)
{
    FastS_assert(_frozen);

    FastS_DataSetBase *dataset =
        (datasetid < _datasets_size) ?
        _datasets[datasetid] : NULL;

    if (dataset != NULL)
        dataset->AddCost();

    return dataset;
}


FastS_DataSetBase *
FastS_DataSetCollection::GetDataSet()
{
    FastS_assert(_frozen);

    FastS_DataSetBase *dataset = NULL;

    for (uint32_t i = 0; i < _datasets_size; i++) {
        FastS_DataSetBase *tmp = _datasets[i];
        if (tmp == NULL || tmp->_unitrefcost == 0)
            continue;

        // NB: cost race condition

        if (dataset == NULL ||
            dataset->_totalrefcost + dataset->_unitrefcost >
            tmp->_totalrefcost + tmp->_unitrefcost)
            dataset = tmp;
    }

    if (dataset != NULL)
        dataset->AddCost();

    return dataset;
}


bool
FastS_DataSetCollection::AreEnginesReady()
{
    bool ready = true;

    for (uint32_t datasetidx = 0;
         ready && (datasetidx < GetMaxNumDataSets());
         datasetidx++)
    {
        FastS_DataSetBase *dataset = PeekDataSet(datasetidx);
        ready = (dataset != NULL && !dataset->AreEnginesReady());
    }
    return ready;
}


FastS_ISearch *
FastS_DataSetCollection::CreateSearch(uint32_t dataSetID,
                                      FastS_TimeKeeper *timeKeeper)
{
    FastS_ISearch *ret = NULL;
    FastS_DataSetBase *dataset;

    if (dataSetID == FastS_NoID32()) {
        dataset = GetDataSet();
        if (dataset != NULL)
            dataSetID = dataset->GetID();
    } else {
        dataset = GetDataSet(dataSetID);
    }
    if (dataset == NULL) {
        ret = new FastS_FailedSearch(dataSetID, false,
                                     search::engine::ECODE_ILLEGAL_DATASET, NULL);
    } else {
        {
            auto dsGuard(dataset->getDsGuard());
            dataset->SetActiveQuery_HasLock();
        }
        /* XXX: Semantic change: precounted as active in dataset */
        ret = dataset->CreateSearch(this, timeKeeper, /* async = */ false);
    }
    FastS_assert(ret != NULL);
    return ret;
}


void
FastS_DataSetCollection::CheckQueryQueues(FastS_TimeKeeper *timeKeeper)
{
    for (uint32_t datasetidx(0); datasetidx < GetMaxNumDataSets(); datasetidx++) {
        FastS_DataSetBase *dataset = PeekDataSet(datasetidx);

        if (dataset != NULL) {
            auto dsGuard(dataset->getDsGuard());
            dataset->CheckQueryQueue_HasLock(timeKeeper);
        }
    }
}


void
FastS_DataSetCollection::AbortQueryQueues()
{
    for (uint32_t datasetidx(0); datasetidx < GetMaxNumDataSets(); datasetidx++) {
        FastS_DataSetBase *dataset = PeekDataSet(datasetidx);

        if (dataset != NULL) {
            auto dsGuard(dataset->getDsGuard());
            dataset->AbortQueryQueue_HasLock();
        }
    }
}
