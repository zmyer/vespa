// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include "executorthreadingservice.h"
#include "fast_access_doc_subdb.h"
#include "feedhandler.h"
#include "searchable_doc_subdb_configurer.h"
#include "searchable_feed_view.h"
#include "searchview.h"
#include "summaryadapter.h"
#include <vespa/eval/eval/value_cache/constant_tensor_loader.h>
#include <vespa/eval/eval/value_cache/constant_value_cache.h>
#include <vespa/searchcore/config/config-proton.h>
#include <vespa/searchcore/proton/attribute/attributemanager.h>
#include <vespa/searchcore/proton/common/doctypename.h>
#include <vespa/searchcore/proton/docsummary/summarymanager.h>
#include <vespa/searchcore/proton/documentmetastore/documentmetastorecontext.h>
#include <vespa/searchcore/proton/index/i_index_writer.h>
#include <vespa/searchcore/proton/index/indexmanager.h>
#include <vespa/searchcore/proton/matching/constant_value_repo.h>
#include <vespa/searchcorespi/index/iindexmanager.h>
#include <vespa/vespalib/util/blockingthreadstackexecutor.h>
#include <vespa/vespalib/util/varholder.h>

namespace proton {

class DocumentDBConfig;
class DocumentDBMetrics;
class IDocumentDBReferenceResolver;
class MetricsWireService;
class GidToLidChangeHandler;

/**
 * The searchable sub database supports searching and keeps all attribute fields in memory and
 * inserts all index fields into the memory index in addition to storing documents in the
 * underlying document store.
 *
 * This class is used directly by the "0.ready" sub database for handling all ready documents.
 */
class
SearchableDocSubDB : public FastAccessDocSubDB,
                     public searchcorespi::IIndexManager::Reconfigurer

{
public:
    struct Config {
        const FastAccessDocSubDB::Config _fastUpdCfg;
        const size_t _numSearcherThreads;

        Config(const FastAccessDocSubDB::Config &fastUpdCfg, size_t numSearcherThreads)
            : _fastUpdCfg(fastUpdCfg),
              _numSearcherThreads(numSearcherThreads)
        { }
    };

    struct Context {
        const FastAccessDocSubDB::Context _fastUpdCtx;
        matching::QueryLimiter   &_queryLimiter;
        const vespalib::Clock    &_clock;
        vespalib::ThreadExecutor &_warmupExecutor;

        Context(const FastAccessDocSubDB::Context &fastUpdCtx,
                matching::QueryLimiter &queryLimiter,
                const vespalib::Clock &clock,
                vespalib::ThreadExecutor &warmupExecutor)
            : _fastUpdCtx(fastUpdCtx),
              _queryLimiter(queryLimiter),
              _clock(clock),
              _warmupExecutor(warmupExecutor)
        { }
    };

private:
    typedef FastAccessDocSubDB Parent;
    using IFlushTargetList = std::vector<std::shared_ptr<searchcorespi::IFlushTarget>>;

    searchcorespi::IIndexManager::SP            _indexMgr;
    IIndexWriter::SP                            _indexWriter;
    vespalib::VarHolder<SearchView::SP>         _rSearchView;
    vespalib::VarHolder<SearchableFeedView::SP> _rFeedView;
    vespalib::eval::ConstantTensorLoader        _tensorLoader;
    vespalib::eval::ConstantValueCache          _constantValueCache;
    matching::ConstantValueRepo                 _constantValueRepo;
    SearchableDocSubDBConfigurer                _configurer;
    const size_t                                _numSearcherThreads;
    vespalib::ThreadExecutor                   &_warmupExecutor;
    std::shared_ptr<GidToLidChangeHandler>      _realGidToLidChangeHandler;
    DocumentDBFlushConfig                       _flushConfig;
    bool                                        _nodeRetired;

    // Note: lifetime of indexManager must be handled by caller.
    std::shared_ptr<initializer::InitializerTask>
    createIndexManagerInitializer(const DocumentDBConfig &configSnapshot,
                                  SerialNum configSerialNum,
                                  const vespa::config::search::core::ProtonConfig::Index &indexCfg,
                                  std::shared_ptr<searchcorespi::IIndexManager::SP> indexManager) const;

    void setupIndexManager(searchcorespi::IIndexManager::SP indexManager);
    void initFeedView(const IAttributeWriter::SP &attrWriter, const DocumentDBConfig &configSnapshot);
    void reconfigureMatchingMetrics(const vespa::config::search::RankProfilesConfig &config);

    bool reconfigure(vespalib::Closure0<bool>::UP closure) override;
    void reconfigureIndexSearchable();
    void syncViews();
    void applyFlushConfig(const DocumentDBFlushConfig &flushConfig);
    void propagateFlushConfig();
protected:
    IFlushTargetList getFlushTargetsInternal() override;

    using Parent::updateLidReuseDelayer;

    void updateLidReuseDelayer(const LidReuseDelayerConfig &config) override;
public:
    SearchableDocSubDB(const Config &cfg, const Context &ctx);
    ~SearchableDocSubDB();

    std::unique_ptr<DocumentSubDbInitializer>
    createInitializer(const DocumentDBConfig &configSnapshot, SerialNum configSerialNum,
                      const vespa::config::search::core::ProtonConfig::Index &indexCfg) const override;

    void setup(const DocumentSubDbInitializerResult &initResult) override;
    void initViews(const DocumentDBConfig &configSnapshot, const SessionManagerSP &sessionManager)  override;

    IReprocessingTask::List
    applyConfig(const DocumentDBConfig &newConfigSnapshot, const DocumentDBConfig &oldConfigSnapshot,
                SerialNum serialNum, const ReconfigParams &params, IDocumentDBReferenceResolver &resolver) override;
    virtual void setBucketStateCalculator(const std::shared_ptr<IBucketStateCalculator> &calc) override;

    void clearViews() override;

    proton::IAttributeManager::SP getAttributeManager() const override {
        return _rSearchView.get()->getAttributeManager();
    }

    const searchcorespi::IIndexManager::SP &getIndexManager() const override {
        return _indexMgr;
    }

    const IIndexWriter::SP &getIndexWriter() const override {
        return _indexWriter;
    }

    SerialNum getOldestFlushedSerial() override;
    SerialNum getNewestFlushedSerial() override;
    void setIndexSchema(const Schema::SP &schema, SerialNum serialNum) override;
    size_t getNumActiveDocs() const override;
    search::SearchableStats getSearchableStats() const override ;
    IDocumentRetriever::UP getDocumentRetriever() override;
    matching::MatchingStats getMatcherStats(const vespalib::string &rankProfile) const override;
    void close() override;
    std::shared_ptr<IDocumentDBReference> getDocumentDBReference() override;
    void tearDownReferences(IDocumentDBReferenceResolver &resolver) override;
};

} // namespace proton
