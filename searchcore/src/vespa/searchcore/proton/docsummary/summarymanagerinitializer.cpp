// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "summarymanagerinitializer.h"
#include <vespa/searchcore/proton/common/eventlogger.h>
#include <vespa/vespalib/io/fileutil.h>

namespace proton {

SummaryManagerInitializer::
SummaryManagerInitializer(const search::GrowStrategy &grow,
                          const vespalib::string baseDir,
                          const vespalib::string &subDbName,
                          const DocTypeName &docTypeName,
                          vespalib::ThreadStackExecutorBase &summaryExecutor,
                          const search::LogDocumentStore::Config & storeCfg,
                          const search::TuneFileSummary &tuneFile,
                          const search::common::FileHeaderContext &fileHeaderContext,
                          search::transactionlog::SyncProxy &tlSyncer,
                          IBucketizerSP bucketizer,
                          std::shared_ptr<SummaryManager::SP> result)
    : proton::initializer::InitializerTask(),
      _grow(grow),
      _baseDir(baseDir),
      _subDbName(subDbName),
      _docTypeName(docTypeName),
      _summaryExecutor(summaryExecutor),
      _storeCfg(storeCfg),
      _tuneFile(tuneFile),
      _fileHeaderContext(fileHeaderContext),
      _tlSyncer(tlSyncer),
      _bucketizer(bucketizer),
      _result(result)
{ }

SummaryManagerInitializer::~SummaryManagerInitializer() {}

void
SummaryManagerInitializer::run()
{
    vespalib::mkdir(_baseDir, false);
    fastos::TimeStamp startTime = fastos::ClockSystem::now();
    EventLogger::loadDocumentStoreStart(_subDbName);
    *_result = std::make_shared<SummaryManager>
               (_summaryExecutor, _storeCfg, _grow, _baseDir, _docTypeName,
                _tuneFile, _fileHeaderContext, _tlSyncer, _bucketizer);
    fastos::TimeStamp endTime = fastos::ClockSystem::now();
    int64_t elapsedTimeMs = (endTime - startTime).ms();
    EventLogger::loadDocumentStoreComplete(_subDbName, elapsedTimeMs);
}

} // namespace proton
