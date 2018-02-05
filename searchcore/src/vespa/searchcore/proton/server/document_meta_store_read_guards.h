// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include <vespa/searchcore/proton/documentmetastore/i_document_meta_store_context.h>

namespace proton {

class DocumentSubDBCollection;

/**
 * Class that takes and owns read guards of the document meta stores of the 3 sub databases.
 * Provides stats regarding the number of documents in the sub databases.
 */
struct DocumentMetaStoreReadGuards
{
    IDocumentMetaStoreContext::IReadGuard::UP readydms;
    IDocumentMetaStoreContext::IReadGuard::UP notreadydms;
    IDocumentMetaStoreContext::IReadGuard::UP remdms;

    DocumentMetaStoreReadGuards(DocumentSubDBCollection &subDBs);
    ~DocumentMetaStoreReadGuards();

    uint32_t numActiveDocs() const {
        return readydms ? readydms->get().getNumActiveLids() : 0;
    }
    uint32_t numIndexedDocs() const {
        return readydms ? readydms->get().getNumUsedLids() : 0;
    }
    uint32_t numStoredDocs() const {
        return numIndexedDocs() + (notreadydms ? notreadydms->get().getNumUsedLids() : 0);
    }
    uint32_t numRemovedDocs() const {
        return remdms ? remdms->get().getNumUsedLids() : 0;
    }
};

} // namespace proton
