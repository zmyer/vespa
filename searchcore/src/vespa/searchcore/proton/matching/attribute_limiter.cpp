// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "attribute_limiter.h"
#include <vespa/vespalib/util/stringfmt.h>
#include <vespa/searchlib/fef/matchdatalayout.h>
#include <vespa/searchlib/query/tree/range.h>
#include <vespa/searchlib/query/tree/simplequery.h>

using namespace search::queryeval;
using namespace search::query;
using vespalib::make_string;
using vespalib::string;

namespace proton {
namespace matching {

AttributeLimiter::AttributeLimiter(Searchable &searchable_attributes,
                                   const IRequestContext & requestContext,
                                   const string &attribute_name,
                                   bool descending,
                                   const string &diversity_attribute,
                                   double diversityCutoffFactor,
                                   DiversityCutoffStrategy diversityCutoffStrategy)
    : _searchable_attributes(searchable_attributes),
      _requestContext(requestContext),
      _attribute_name(attribute_name),
      _descending(descending),
      _diversity_attribute(diversity_attribute),
      _lock(),
      _match_datas(),
      _blueprint(),
      _estimatedHits(-1),
      _diversityCutoffFactor(diversityCutoffFactor),
      _diversityCutoffStrategy(diversityCutoffStrategy)
{
}

AttributeLimiter::~AttributeLimiter() {}

namespace {

vespalib::string STRICT_STR("strict");
vespalib::string LOOSE_STR("loose");

}

AttributeLimiter::DiversityCutoffStrategy
AttributeLimiter::toDiversityCutoffStrategy(const vespalib::stringref & strategy)
{
    return (strategy == STRICT_STR) ? DiversityCutoffStrategy::STRICT : DiversityCutoffStrategy::LOOSE;
}

const vespalib::string &
AttributeLimiter::toString(DiversityCutoffStrategy strategy)
{
    return (strategy == DiversityCutoffStrategy::STRICT) ? STRICT_STR : LOOSE_STR;
}

SearchIterator::UP
AttributeLimiter::create_search(size_t want_hits, size_t max_group_size, bool strictSearch)
{
    std::lock_guard<std::mutex> guard(_lock);
    const uint32_t my_field_id = 0;
    search::fef::MatchDataLayout layout;
    auto my_handle = layout.allocTermField(my_field_id);
    if ( ! _blueprint ) {
        const uint32_t no_unique_id = 0;
        string range_spec = make_string("[;;%s%zu", (_descending)? "-" : "", want_hits);
        if (max_group_size < want_hits) {
            size_t cutoffGroups = (_diversityCutoffFactor*want_hits)/max_group_size;
            range_spec.append(make_string(";%s;%zu;%zu;%s]", _diversity_attribute.c_str(), max_group_size,
                                          cutoffGroups, toString(_diversityCutoffStrategy).c_str()));
        } else {
            range_spec.push_back(']');
        }
        Range range(range_spec);
        SimpleRangeTerm node(range, _attribute_name, no_unique_id, Weight(0));
        FieldSpecList field; // single field API is protected
        field.add(FieldSpec(_attribute_name, my_field_id, my_handle));
        _blueprint = _searchable_attributes.createBlueprint(_requestContext, field, node);
        _blueprint->fetchPostings(strictSearch);
        _estimatedHits = _blueprint->getState().estimate().estHits;
        _blueprint->freeze();
    }
    _match_datas.push_back(layout.createMatchData());
    return _blueprint->createSearch(*_match_datas.back(), strictSearch);
}

} // namespace proton::matching
} // namespace proton
