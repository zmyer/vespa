// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include <vespa/document/bucket/bucketspace.h>
#include <vespa/vespalib/stllike/string.h>
#include <memory>

namespace vespalib { class ThreadStackExecutorBase; }

namespace proton {

class IDocumentDBConfigOwner;

/*
 * Interface class for owner of a proton configurer, with callback methods
 * for adding/removing document dbs and applying bootstrap config.
 */
class IProtonConfigurerOwner
{
    using InitializeThreads = std::shared_ptr<vespalib::ThreadStackExecutorBase>;
public:
    virtual ~IProtonConfigurerOwner() { }
    virtual IDocumentDBConfigOwner *addDocumentDB(const DocTypeName &docTypeName,
                                                  document::BucketSpace bucketSpace,
                                                  const vespalib::string &configId,
                                                  const std::shared_ptr<BootstrapConfig> &bootstrapConfig,
                                                  const std::shared_ptr<DocumentDBConfig> &documentDBConfig,
                                                  InitializeThreads initializeThreads) = 0;
    virtual void removeDocumentDB(const DocTypeName &docTypeName) = 0;
    virtual void applyConfig(const std::shared_ptr<BootstrapConfig> &bootstrapConfig) = 0;
};


} // namespace proton
