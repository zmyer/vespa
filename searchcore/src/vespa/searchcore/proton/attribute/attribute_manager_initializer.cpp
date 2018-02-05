// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "attribute_manager_initializer.h"
#include "attributes_initializer_base.h"
#include "attribute_collection_spec_factory.h"
#include <vespa/searchcorespi/index/i_thread_service.h>
#include <future>

using search::AttributeVector;
using search::GrowStrategy;
using search::SerialNum;
using vespa::config::search::AttributesConfig;

namespace proton {

using initializer::InitializerTask;

namespace {

class AttributeInitializerTask : public InitializerTask
{
private:
    AttributeInitializer::UP _initializer;
    DocumentMetaStore::SP _documentMetaStore;
    InitializedAttributesResult &_result;

public:
    AttributeInitializerTask(AttributeInitializer::UP initializer,
                             DocumentMetaStore::SP documentMetaStore,
                             InitializedAttributesResult &result)
        : _initializer(std::move(initializer)),
          _documentMetaStore(documentMetaStore),
          _result(result)
    {}

    void run() override {
        AttributeInitializerResult result = _initializer->init();
        if (result) {
            AttributesInitializerBase::considerPadAttribute(*result.getAttribute(),
                                                            _initializer->getCurrentSerialNum(),
                                                            _documentMetaStore->getCommittedDocIdLimit());
            _result.add(result);
        }
    }
};

class AttributeManagerInitializerTask : public vespalib::Executor::Task
{
    std::promise<void> _promise;
    search::SerialNum _configSerialNum;
    DocumentMetaStore::SP _documentMetaStore;
    AttributeManager::SP _attrMgr;
    InitializedAttributesResult &_attributesResult;

public:
    AttributeManagerInitializerTask(std::promise<void> &&promise,
                                    search::SerialNum configSerialNum,
                                    DocumentMetaStore::SP documentMetaStore,
                                    AttributeManager::SP attrMgr,
                                    InitializedAttributesResult &attributesResult);
    ~AttributeManagerInitializerTask() override;
    void run() override;
};


AttributeManagerInitializerTask::AttributeManagerInitializerTask(std::promise<void> &&promise,
                                                                 search::SerialNum configSerialNum,
                                                                 DocumentMetaStore::SP documentMetaStore,
                                                                 AttributeManager::SP attrMgr,
                                                                 InitializedAttributesResult &attributesResult)
    : _promise(std::move(promise)),
      _configSerialNum(configSerialNum),
      _documentMetaStore(documentMetaStore),
      _attrMgr(attrMgr),
      _attributesResult(attributesResult)
{
}

AttributeManagerInitializerTask::~AttributeManagerInitializerTask()
{
}

void
AttributeManagerInitializerTask::run()
{
    _attrMgr->addExtraAttribute(_documentMetaStore);
    _attrMgr->addInitializedAttributes(_attributesResult.get());
    _attrMgr->pruneRemovedFields(_configSerialNum);
    _promise.set_value();
}

class AttributeInitializerTasksBuilder : public IAttributeInitializerRegistry
{
private:
    InitializerTask &_attrMgrInitTask;
    InitializerTask::SP _documentMetaStoreInitTask;
    DocumentMetaStore::SP _documentMetaStore;
    InitializedAttributesResult &_attributesResult;

public:
    AttributeInitializerTasksBuilder(InitializerTask &attrMgrInitTask,
                                     InitializerTask::SP documentMetaStoreInitTask,
                                     DocumentMetaStore::SP documentMetaStore,
                                     InitializedAttributesResult &attributesResult);
    ~AttributeInitializerTasksBuilder();
    void add(AttributeInitializer::UP initializer) override;
};

AttributeInitializerTasksBuilder::AttributeInitializerTasksBuilder(InitializerTask &attrMgrInitTask,
                                                                   InitializerTask::SP documentMetaStoreInitTask,
                                                                   DocumentMetaStore::SP documentMetaStore,
                                                                   InitializedAttributesResult &attributesResult)
    : _attrMgrInitTask(attrMgrInitTask),
      _documentMetaStoreInitTask(documentMetaStoreInitTask),
      _documentMetaStore(documentMetaStore),
      _attributesResult(attributesResult)
{ }

AttributeInitializerTasksBuilder::~AttributeInitializerTasksBuilder() {}

void
AttributeInitializerTasksBuilder::add(AttributeInitializer::UP initializer) {
    InitializerTask::SP attributeInitTask =
            std::make_shared<AttributeInitializerTask>(std::move(initializer),
                                                       _documentMetaStore,
                                                       _attributesResult);
    attributeInitTask->addDependency(_documentMetaStoreInitTask);
    _attrMgrInitTask.addDependency(attributeInitTask);
}

}

AttributeCollectionSpec::UP
AttributeManagerInitializer::createAttributeSpec() const
{
    uint32_t docIdLimit = 1; // The real docIdLimit is used after attributes are loaded to pad them
    AttributeCollectionSpecFactory factory(_attributeGrow, _attributeGrowNumDocs, _fastAccessAttributesOnly);
    return factory.create(_attrCfg, docIdLimit, _configSerialNum);
}

AttributeManagerInitializer::AttributeManagerInitializer(SerialNum configSerialNum,
                                                         initializer::InitializerTask::SP documentMetaStoreInitTask,
                                                         DocumentMetaStore::SP documentMetaStore,
                                                         AttributeManager::SP baseAttrMgr,
                                                         const AttributesConfig &attrCfg,
                                                         const GrowStrategy &attributeGrow,
                                                         size_t attributeGrowNumDocs,
                                                         bool fastAccessAttributesOnly,
                                                         searchcorespi::index::IThreadService &master,
                                                         std::shared_ptr<AttributeManager::SP> attrMgrResult)
    : _configSerialNum(configSerialNum),
      _documentMetaStore(documentMetaStore),
      _attrMgr(),
      _attrCfg(attrCfg),
      _attributeGrow(attributeGrow),
      _attributeGrowNumDocs(attributeGrowNumDocs),
      _fastAccessAttributesOnly(fastAccessAttributesOnly),
      _master(master),
      _attributesResult(),
      _attrMgrResult(attrMgrResult)
{
    addDependency(documentMetaStoreInitTask);
    AttributeInitializerTasksBuilder tasksBuilder(*this, documentMetaStoreInitTask, documentMetaStore, _attributesResult);
    AttributeCollectionSpec::UP attrSpec = createAttributeSpec();
    _attrMgr = std::make_shared<AttributeManager>(*baseAttrMgr, *attrSpec, tasksBuilder);
}

void
AttributeManagerInitializer::run()
{
    std::promise<void> promise;
    auto future = promise.get_future();
    /*
     * Attribute manager and some its members (e.g. _attributeFieldWriter) assumes that work is performed
     * by document db master thread and lacks locking to handle calls from multiple threads.
     */
    _master.execute(std::make_unique<AttributeManagerInitializerTask>(std::move(promise),
                                                                      _configSerialNum,
                                                                      _documentMetaStore,
                                                                      _attrMgr,
                                                                      _attributesResult));
    future.wait();
    *_attrMgrResult = _attrMgr;
}

} // namespace proton
