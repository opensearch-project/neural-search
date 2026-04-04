## Version 3.6.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.6.0

### Features
* Add support for embedding model ID configuration in agentic query translator processor ([#1800](https://github.com/opensearch-project/neural-search/pull/1800))
* Add gRPC integration test for hybrid query end-to-end execution ([#1734](https://github.com/opensearch-project/neural-search/pull/1734))

### Bug Fixes
* Block hybrid query when nested inside compound and score queries such as function_score, script_score, and constant_score ([#1791](https://github.com/opensearch-project/neural-search/pull/1791))
* Fix hybrid search with collapse returning incorrect relevancy scores and missing results due to comparator and priority queue issues ([#1763](https://github.com/opensearch-project/neural-search/pull/1763))
* Fix hybrid search with collapse producing incorrect top document ordering and unnecessary duplicate strategy computation ([#1753](https://github.com/opensearch-project/neural-search/pull/1753))
* Fix HybridQueryDocIdStream compilation error by adding required intoArray override from upstream Lucene changes ([#1780](https://github.com/opensearch-project/neural-search/pull/1780))
* Fix remote symmetric models not being supported due to invalid validation ([#1767](https://github.com/opensearch-project/neural-search/pull/1767))
* Fix profiler support for hybrid query by unwrapping ProfileScorer to prevent NullPointerException ([#1754](https://github.com/opensearch-project/neural-search/pull/1754))
* Fix rerank processor to correctly extract text from nested and dot-notation fields ([#1805](https://github.com/opensearch-project/neural-search/pull/1805))
* Fix hybrid search with collapse producing different scores and total hits compared to non-collapse results ([#1787](https://github.com/opensearch-project/neural-search/pull/1787))
* Fix empty profiler data for hybrid query when used with sort and/or collapse ([#1794](https://github.com/opensearch-project/neural-search/pull/1794))

### Infrastructure
* Fix gRPC integration test port discovery for reliable execution in local and CI environments ([#1814](https://github.com/opensearch-project/neural-search/pull/1814))
* Fix integration tests to read number of cluster nodes dynamically instead of hardcoding ([#1776](https://github.com/opensearch-project/neural-search/pull/1776))

### Maintenance
* Optimize HybridQueryDocIdStream.intoArray to avoid unnecessary scorer state mutations and improve test coverage ([#1786](https://github.com/opensearch-project/neural-search/pull/1786))
