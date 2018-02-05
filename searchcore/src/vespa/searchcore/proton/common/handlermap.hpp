// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#pragma once

#include "doctypename.h"
#include <vespa/vespalib/util/exceptions.h>
#include <vespa/vespalib/util/sequence.h>
#include <vespa/vespalib/stllike/string.h>
#include <vespa/vespalib/stllike/hash_map.h>
#include <map>
#include <vector>

namespace proton {

/**
 * This template defines a mapping from a document type name to a shared pointer
 * to the template argument type.
 */
template <typename T>
class HandlerMap {
private:
    typedef typename std::shared_ptr<T>    HandlerSP;
    typedef std::map<DocTypeName, HandlerSP> StdMap;
    typedef typename StdMap::iterator        MapIterator;

    StdMap _handlers;

public:
    /**
     * A snapshot of the currently registered handlers. This
     * implementation simply takes a copy of all the shared pointers
     * in the map to keep the handlers alive. However, the current
     * abstraction allows as to make a snapshot based on bald pointers
     * using event barriers to resolve visibility constraints in the
     * future without changing the external API.
     **/
    class Snapshot : public vespalib::Sequence<T*>
    {
    private:
        std::vector<HandlerSP> _handlers;
        size_t                 _offset;

    public:
        typedef std::unique_ptr<Snapshot> UP;

        Snapshot(const StdMap &map) : _handlers(), _offset(0) {
            _handlers.reserve(map.size());
            for (auto itr : map) {
                _handlers.push_back(itr.second);
            }
        }
        Snapshot(std::vector<HandlerSP> &&handlers) : _handlers(std::move(handlers)), _offset(0) {}
        bool valid() const override { return (_offset < _handlers.size()); }
        T *get() const override { return _handlers[_offset].get(); }
        HandlerSP getSP() const { return _handlers[_offset]; }
        void next() override { ++_offset; }
    };

    /**
     * Convenience typedefs.
     */
    typedef typename StdMap::iterator       iterator;
    typedef typename StdMap::const_iterator const_iterator;

    /**
     * Constructs a new instance of this class.
     */
    HandlerMap()
        : _handlers()
    {
        // empty
    }

    /**
     * Registers a new handler for the given document type. If another handler
     * was already registered under the same type, this method will return a
     * pointer to that handler.
     *
     * @param docType The document type to register a handler for.
     * @param handler The handler to register.
     * @return The replaced handler, if any.
     */
    HandlerSP
    putHandler(const DocTypeName &docTypeNameVer,
               const HandlerSP &handler)
    {
        if (handler.get() == NULL) {
            throw vespalib::IllegalArgumentException(vespalib::make_string(
                            "Handler is null for docType '%s'",
                            docTypeNameVer.toString().c_str()));
        }
        iterator it = _handlers.find(docTypeNameVer);
        if (it == _handlers.end()) {
            _handlers[docTypeNameVer] = handler;
            return HandlerSP();
        }
        HandlerSP ret = it->second;
        it->second = handler;
        return ret;
    }

    /**
     * Returns the handler for the given document type. If no handler was
     * registered, this method returns an empty shared pointer.
     *
     * @param docType The document type whose handler to return.
     * @return The registered handler, if any.
     */
    HandlerSP
    getHandler(const DocTypeName &docTypeNameVer) const
    {
        const_iterator it = _handlers.find(docTypeNameVer);
        if (it != _handlers.end()) {
            return it->second;
        }
        return HandlerSP();
    }

    bool hasHandler(const HandlerSP &handler) const {
        bool found = false;
        for (const auto &kv : _handlers) {
            if (handler == kv.second) {
                found = true;
                break;
            }
        }
        return found;
    }

    /**
     * Removes and returns the handler for the given document type. If no
     * handler was registered, this method returns an empty shared pointer.
     *
     * @param docType The document type whose handler to remove.
     * @return The removed handler, if any.
     */
    HandlerSP
    removeHandler(const DocTypeName &docTypeNameVer)
    {
        iterator it = _handlers.find(docTypeNameVer);
        if (it == _handlers.end()) {
            return HandlerSP();
        }
        HandlerSP ret = it->second;
        _handlers.erase(it);
        return ret;
    }

    /**
     * Clear all handlers.
     */
    void
    clear()
    {
        _handlers.clear();
    }

    /**
     * Create a snapshot of the handlers currently contained in this
     * map and return it as a sequence. The returned sequence will
     * ensure that all object pointers stay valid until it is deleted.
     *
     * @return handler sequence
     **/
    std::unique_ptr<Snapshot>
    snapshot() const
    {
        return std::unique_ptr<Snapshot>(new Snapshot(_handlers));
    }

// we want to use snapshots rather than direct iteration to reduce locking;
// the below functions should be deprecated when possible.

    /**
     * Returns a bidirectional iterator that points at the first element of the
     * sequence (or just beyond the end of an empty sequence).
     *
     * @return The beginning of this map.
     */
    iterator
    begin()
    {
        return _handlers.begin();
    }

    /**
     * Returns a const bidirectional iterator that points at the first element
     * of the sequence (or just beyond the end of an empty sequence).
     *
     * @return The beginning of this map.
     */
    const_iterator
    begin() const
    {
        return _handlers.begin();
    }

    /**
     * Returns a bidirectional iterator that points just beyond the end of the
     * sequence.
     *
     * @return The end of this map.
     */
    iterator
    end()
    {
        return _handlers.end();
    }

    /**
     * Returns a const bidirectional iterator that points just beyond the end of
     * the sequence.
     *
     * @return The end of this map.
     */
    const_iterator
    end() const
    {
        return _handlers.end();
    }

    /**
     * Returns the number of handlers in this map.
     *
     * @return the number of handlers.
     */
    size_t size() const { return _handlers.size(); }
};

} // namespace proton

