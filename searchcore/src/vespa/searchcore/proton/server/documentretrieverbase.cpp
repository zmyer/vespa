// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "documentretrieverbase.h"
#include <vespa/document/repo/documenttyperepo.h>
#include <vespa/document/datatype/documenttype.h>
#include <vespa/vespalib/stllike/lrucache_map.hpp>

using document::DocumentId;
using document::GlobalId;

namespace {

const DocumentId docId("doc:test:1");

}

namespace proton {

DocumentRetrieverBase::DocumentRetrieverBase(
        const DocTypeName &docTypeName,
        const document::DocumentTypeRepo &repo,
        const IDocumentMetaStoreContext &meta_store,
        bool hasFields)
    : IDocumentRetriever(),
      _docTypeName(docTypeName),
      _repo(repo),
      _meta_store(meta_store),
      _selectCache(256u),
      _lock(),
      _emptyDoc(),
      _hasFields(hasFields)
{
    const document::DocumentType *
        docType(_repo.getDocumentType(_docTypeName.getName()));
    _emptyDoc.reset(new document::Document(*docType, docId));
    _emptyDoc->setRepo(_repo);
}

DocumentRetrieverBase::~DocumentRetrieverBase() { }

const document::DocumentTypeRepo &
DocumentRetrieverBase::getDocumentTypeRepo() const {
    return _repo;
}

void
DocumentRetrieverBase::getBucketMetaData(
        const storage::spi::Bucket &bucket,
        search::DocumentMetaData::Vector &result) const {
    IDocumentMetaStoreContext::IReadGuard::UP readGuard = _meta_store.getReadGuard();
    const search::IDocumentMetaStore &meta_store = readGuard->get();
    meta_store.getMetaData(bucket, result);
}

search::DocumentMetaData
DocumentRetrieverBase::getDocumentMetaData(const DocumentId &id) const {
    const GlobalId &gid = id.getGlobalId();
    IDocumentMetaStoreContext::IReadGuard::UP readGuard = _meta_store.getReadGuard();
    const search::IDocumentMetaStore &meta_store = readGuard->get();
    return meta_store.getMetaData(gid);
}


const search::IAttributeManager *
DocumentRetrieverBase::getAttrMgr() const
{
    return NULL;
}

    
CachedSelect::SP
DocumentRetrieverBase::parseSelect(const vespalib::string &selection) const
{
    {
        std::lock_guard<std::mutex> guard(_lock);
        if (_selectCache.hasKey(selection))
            return _selectCache[selection];
    }
    
    CachedSelect::SP nselect(new CachedSelect());
    
    nselect->set(selection,
                 _docTypeName.getName(),
                 *_emptyDoc,
                 getDocumentTypeRepo(),
                 getAttrMgr(),
                 _hasFields);

    std::lock_guard<std::mutex> guard(_lock);
    if (_selectCache.hasKey(selection))
        return _selectCache[selection];
    _selectCache.insert(selection, nselect);
    return nselect;
}


}  // namespace proton
