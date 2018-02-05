<!-- Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root. -->
# List of possible future enhancements and features

This lists some possible improvements to Vespa which have been considered or requested, can be developed relatively independently of other work and are not yet under development.

## Linguistics for more languages

**Effort:** Small
**Skills:** Java

Vespa comes bundles with linguistics (converting raw text to tokens) for english but not for other languages. This means that it raw text input to Vespa in non-english languages will have sub-optimal recall and that CJK languages must be configured to use n-gram indexing (producing tokens by chopping the text without considering semantics). Linguistics implementations for other languages can be plugged in by configuring a component which implements com.yahoo.language.Linguistics. See com.yahoo.language.simple.SimpleLinguistics for a simple example of implementing Linguistics.

Estimated effort is for integration of existing linguistics libraries.

## Global writes

**Effort:** Large
**Skills:** C++, Java, distributed systems, performance, multithreading, network, distributed consistency

Vespa instances distribute data automatically within clusters, but these clusters are meant to consist of co-located machines - the distribution algorithm is not suitable for global distribution across datacenters because it cannot seamlessly tolerate datacenter-wide outages and does not attempt to minimize bandwith usage between datacenters.
Application usually achieve global precense instead by setting up multiple independent instances in different datacenters and write to all in parallel. This is robust and works well on average, but puts additional burden on applications to achieve cross-datacenter data consistency on datacenter failures, and does not enable automatic data recovery across datacenters, such that data redundancy is effectively required within each datacenter. This is fine in most cases, but not in the case where storage space drives cost and intermittent loss of data coverage (completeness as seen from queries) is tolerable.

A solution should sustain current write rates (tens of thousands of writes per ndoe per second), sustain write and read rates on loss of connectivity to one (any) data center, re-establish global data consistency when a lost datacenter is recovered and support some degree of tradeoff between consistency and operation latency (although the exact modes to be supported is part of the design and analysis needed).

## Indexed search in maps

**Effort:** Medium
**Skills:** C++, Java, multithreading, performance, indexing, data structures

Vespa supports maps and even supports queries in maps in the query language (due to supporting map queries in streaming search). However, maps cannot be indexed. Supporting map indexing would also be a solution to the occasional need for *dynamic fields*, where there is a need to manage a large number of searchable fields which are not suitable to being defined at config time. A full solution includes support for general structs as map values.

## Dispatch in Java

**Effort:** Small
**Skills:** Java, networking, multithreading

Containers executing queries need to scatter the query to all content nodes in a group and collect the partial results. Currently this part is handled by a separate C++ process called a *dispatcher* running on each container node. To ease implementation of new features and reduce unnecessary process communication the scatter-gather functionality should be implemented in Java and moved into the container. This has already been implemented for summary fetch requests (which do not require scatter-gather) in the class com.yahoo.search.dispatch.Dispatcher. Full dispatcher functionality should build on this. The summary requests uses RPC, which the content nodes currently does not support for searching. However, dispatching from Java can keep using the current "fnet" protocol used for search requests today from com.yahoo.prelude.fastsearch.FastSearcher.

## Change search protocol from fnet to RPC

**Effort:** Small
**Skills:** Java, C++, networking

Currently, search requests happens over a very old custom protocol called "fnet". While this is efficient, it is hard to extend. We want to replace it by RPC calls. This should happen after dispatch is implemented in Java. An RPC alternative is already implemented for summary fetch requests, but not for search requests.

## Support query profiles for document processors

**Effort:** Small
**Skills:** Java

Query profiles make it simple to support multiple buckets, behavior profiles for different use cases etc by providing bundles of parameters accessible to Searchers processing queries. Writes go through a similar chain of processors - Document Processors, but have no equivalent support for parametrization. This is to allow configuration of document processor profiles by reusing the query profile support also for document processors.

## Background reindexing

**Effort:** Medium
**Skills:** Java

Some times there is a need to reindex existing data to refresh the set of tokens produced from the raw text: Some search definition changes impacts the tokens produced, and changing versions of linguistics libraries also cause token changes. As content clusters store the raw data of documents it should be possible to reindex locally inside clusters in the background. However, today this is not supported and content need to be rewritten from the outside to refresh tokens, which is inconvenient and suboptimal. This is to support (scheduled or triggered) backgroun reindexing from local data. This can be achieved by configuring a message bus route which feeds content from a cluster back to itself through the indexing container cluster and triggering a visiting job using this route.

## TensorFlow integration

**Effort:** Low
**Skills:** Java

Vespa supports ranking models consisting of mathematical expressions over tensors. This is very powerful and allows execution of many kinds of machine learned models such as e.g deep neural nets. However, the user is responsible for converting the model created from the learning system to the mathematical model and tensor format required by Vespa. To make this process simpler Vespa could proviode ready-made converters from well-known frameworks. Either as standalone tools or by integration into the config model such that application packages can be configured with third-party models directly. The most requested such integration at the moment is to TensorFlow models.

## Global dynamic tensors

**Effort:** High
**Skills:** Java, C++, distributed systems, performance, networking, distributed consistency

Tensors in ranking models may either be passed with the query, be part of the document or be configured as part of the application package (global tensors). This is fine for many kinds of models but does not support the case of really large tensors (which barely fit in memory) and/or dynamically changing tensors (online learning of global models). These use cases require support for global tensors (tensors available locally on all content nodes during execution but not sent with the query or residing in documents) which are not configured as part of the application package but which are written independently and dynamically updateable at a high write rate. To support this at large scale, with a high write rate, we need a small cluster of nodes storing the source of truth of the global tensor and which have perfect consistency. This in turn must push updates to all content nodes in a best effort fashion given a fixed bandwith budget, such that query execution and document write traffic is prioritized over ensuring perfect consistency of global model updates.

## Java implementation of the content layer for testing

**Effort:** Medium
**Skills:** Java

There is currently support for creating Application instances programmatically in Java to unit test application package functionality (see com.yahoo.application.Application). However, only Java component functionality can be tested in this way as the content layer is not available, being implemented in C++. A Java implementation, of some or all of the functionality would enable developers to do more testing locally within their IDE. This is medium effort because performance is not a concern and some components, such as ranking expressions and features are already available as libraries (see the searchlib module).

## Update where

**Effort:** Medium
**Skills:** Java, C++, distributed systems

Support "update where" operations which changes/removes all documents matching some document selection expression. This entails adding a new document API operation and probably supporting continuations similar to visiting.

## Query tracing including content nodes

**Effort:** Low
**Skills:** Java, C++, multithreading

Currently, trace information can be requested for a given query by adding travelevel=N to the query. This is useful for debugging as well as understanding performance bottlenecks. However, the trace information only includes execution in the container, not in the content nodes. This is to implement similar tracing capabilities in the search core and integrating trace information from each content node into the container level trace. This would make it easier to understand the execution and performance consequences of various query expressions.
