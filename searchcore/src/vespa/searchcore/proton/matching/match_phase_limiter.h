// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#pragma once

#include "match_phase_limit_calculator.h"
#include "attribute_limiter.h"

#include <vespa/searchlib/queryeval/searchable.h>
#include <vespa/vespalib/stllike/string.h>
#include <vespa/searchlib/queryeval/searchiterator.h>
#include <vespa/searchlib/queryeval/blueprint.h>
#include <atomic>

namespace proton {
namespace matching {

class LimitedSearch : public search::queryeval::SearchIterator {
public:
    LimitedSearch(SearchIterator::UP first, SearchIterator::UP second) :
        _first(std::move(first)),
        _second(std::move(second))
    {
    }
    void doSeek(uint32_t docId) override;
    void initRange(uint32_t begin, uint32_t end) override;
    void visitMembers(vespalib::ObjectVisitor &visitor) const override;
    const SearchIterator &  getFirst() const { return *_first; }
    const SearchIterator & getSecond() const { return *_second; }
    SearchIterator &  getFirst() { return *_first; }
    SearchIterator & getSecond() { return *_second; }
private:
    SearchIterator::UP _first;
    SearchIterator::UP _second;
};

/**
 * Interface defining how we intend to use the match phase limiter
 * functionality. The first step is to check whether we should enable
 * this functionality at all. If enabled; we need to match some hits
 * in each match thread for estimation purposes. The total number of
 * matches (hits) and the total document space searched (docs) are
 * aggregated across all match threads and each match thread will use
 * the maybe_limit function to possibly augment its iterator tree to
 * limit the number of matches.
 **/
struct MaybeMatchPhaseLimiter {
    typedef search::queryeval::SearchIterator SearchIterator;
    typedef std::unique_ptr<MaybeMatchPhaseLimiter> UP;
    virtual bool is_enabled() const = 0;
    virtual bool was_limited() const = 0;
    virtual size_t sample_hits_per_thread(size_t num_threads) const = 0;
    virtual SearchIterator::UP maybe_limit(SearchIterator::UP search, double match_freq, size_t num_docs) = 0;
    virtual void updateDocIdSpaceEstimate(size_t searchedDocIdSpace, size_t remainingDocIdSpace) = 0;
    virtual size_t getDocIdSpaceEstimate() const = 0;
    virtual ~MaybeMatchPhaseLimiter() {}
};

/**
 * This class is used when match phase limiting is not configured.
 **/
struct NoMatchPhaseLimiter : MaybeMatchPhaseLimiter {
    bool is_enabled() const override { return false; }
    bool was_limited() const override { return false; }
    size_t sample_hits_per_thread(size_t) const override { return 0; }
    SearchIterator::UP maybe_limit(SearchIterator::UP search, double, size_t) override {
        return search;
    }
    void updateDocIdSpaceEstimate(size_t, size_t) override { }
    size_t getDocIdSpaceEstimate() const override { return std::numeric_limits<size_t>::max(); }
};

/**
 * This class is is used when rank phase limiting is configured.
 **/
class MatchPhaseLimiter : public MaybeMatchPhaseLimiter
{
private:
    class Coverage {
    public:
        Coverage(uint32_t docIdLimit) :
            _docIdLimit(docIdLimit),
            _searched(0)
        { }
        void update(size_t searched, size_t remaining, ssize_t hits) {
            if (hits >= 0) {
                _searched += (searched + (hits*remaining)/_docIdLimit);
            } else {
                _searched += (searched + remaining);
            }
        }
        uint32_t getEstimate() const { return _searched; }
    private:
        const uint32_t        _docIdLimit;
        std::atomic<uint32_t> _searched;
    };
    const double              _postFilterMultiplier;
    const double              _maxFilterCoverage;
    MatchPhaseLimitCalculator _calculator;
    AttributeLimiter          _limiter_factory;
    Coverage                  _coverage;

public:
    MatchPhaseLimiter(uint32_t docIdLimit,
                      search::queryeval::Searchable &searchable_attributes,
                      search::queryeval::IRequestContext & requestContext,
                      const vespalib::string &attribute_name,
                      size_t max_hits, bool descending,
                      double max_filter_coverage,
                      double samplePercentage, double postFilterMultiplier,
                      const vespalib::string &diversity_attribute,
                      uint32_t diversity_min_groups,
                      double diversify_cutoff_factor,
                      AttributeLimiter::DiversityCutoffStrategy diversity_cutoff_strategy);
    bool is_enabled() const override { return true; }
    bool was_limited() const override { return _limiter_factory.was_used(); }
    size_t sample_hits_per_thread(size_t num_threads) const override {
        return _calculator.sample_hits_per_thread(num_threads);
    }
    SearchIterator::UP maybe_limit(SearchIterator::UP search, double match_freq, size_t num_docs) override;
    void updateDocIdSpaceEstimate(size_t searchedDocIdSpace, size_t remainingDocIdSpace) override;
    size_t getDocIdSpaceEstimate() const override;
};

} // namespace proton::matching
} // namespace proton

