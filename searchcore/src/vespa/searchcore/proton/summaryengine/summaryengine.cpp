// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#include "summaryengine.h"

#include <vespa/log/log.h>
LOG_SETUP(".proton.summaryengine.summaryengine");

using namespace search::engine;
using namespace proton;

namespace {

class DocsumTask : public vespalib::Executor::Task {
private:
    SummaryEngine       & _engine;
    DocsumClient        & _client;
    DocsumRequest::Source _request;

public:
    DocsumTask(SummaryEngine & engine, DocsumRequest::Source request, DocsumClient & client)
        : _engine(engine),
          _client(client),
          _request(std::move(request))
    {
    }

    void run() override {
        _client.getDocsumsDone(_engine.getDocsums(_request.release()));
    }
};

} // namespace anonymous

namespace proton {

SummaryEngine::SummaryEngine(size_t numThreads)
    : _lock(),
      _closed(false),
      _handlers(),
      _executor(numThreads, 128 * 1024)
{
    // empty
}

SummaryEngine::~SummaryEngine()
{
    _executor.shutdown();
}

void
SummaryEngine::close()
{
    LOG(debug, "Closing summary engine");
    {
        std::lock_guard<std::mutex> guard(_lock);
        _closed = true;
    }
    LOG(debug, "Handshaking with task manager");
    _executor.sync();
}

ISearchHandler::SP
SummaryEngine::putSearchHandler(const DocTypeName &docTypeName, const ISearchHandler::SP & searchHandler)
{
    std::lock_guard<std::mutex> guard(_lock);
    return _handlers.putHandler(docTypeName, searchHandler);
}

ISearchHandler::SP
SummaryEngine::getSearchHandler(const DocTypeName &docTypeName)
{
    std::lock_guard<std::mutex> guard(_lock);
    return _handlers.getHandler(docTypeName);
}

ISearchHandler::SP
SummaryEngine::removeSearchHandler(const DocTypeName &docTypeName)
{
    std::lock_guard<std::mutex> guard(_lock);
    return _handlers.removeHandler(docTypeName);
}

DocsumReply::UP
SummaryEngine::getDocsums(DocsumRequest::Source request, DocsumClient & client)
{
    if (_closed) {
        LOG(warning, "Receiving docsumrequest after engine has been shutdown");
        DocsumReply::UP ret(new DocsumReply());

        // TODO: Notify closed.

        return ret;
    }
    vespalib::Executor::Task::UP task(new DocsumTask(*this, std::move(request), client));
    _executor.execute(std::move(task));
    return DocsumReply::UP();
}

DocsumReply::UP
SummaryEngine::getDocsums(DocsumRequest::UP req)
{
    DocsumReply::UP reply = std::make_unique<DocsumReply>();

    if (req) {
        ISearchHandler::SP searchHandler = getSearchHandler(DocTypeName(*req));
        if (searchHandler) {
            reply = searchHandler->getDocsums(*req);
        } else {
            vespalib::Sequence<ISearchHandler*>::UP snapshot;
            {
                std::lock_guard<std::mutex> guard(_lock);
                snapshot = _handlers.snapshot();
            }
            if (snapshot->valid()) {
                reply = snapshot->get()->getDocsums(*req); // use the first handler
            }
        }
    }
    reply->request = std::move(req);
    return reply;
}


} // namespace proton
