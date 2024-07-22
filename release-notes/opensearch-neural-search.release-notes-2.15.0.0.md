## Version 2.15.0.0 Release Notes

Compatible with OpenSearch 2.15.0

### Features
* Speed up NeuralSparseQuery by two-phase using a custom search pipeline.([#646](https://github.com/opensearch-project/neural-search/issues/646))
* Support batchExecute in TextEmbeddingProcessor and SparseEncodingProcessor ([#743](https://github.com/opensearch-project/neural-search/issues/743))
### Enhancements
* Pass empty doc collector instead of top docs collector to improve hybrid query latencies by 20% ([#731](https://github.com/opensearch-project/neural-search/pull/731))
* Optimize parameter parsing in text chunking processor ([#733](https://github.com/opensearch-project/neural-search/pull/733))
* Use lazy initialization for priority queue of hits and scores to improve latencies by 20% ([#746](https://github.com/opensearch-project/neural-search/pull/746))
* Optimize max score calculation in the Query Phase of the Hybrid Search ([765](https://github.com/opensearch-project/neural-search/pull/765))
* Implement parallel execution of sub-queries for hybrid search ([#749](https://github.com/opensearch-project/neural-search/pull/749))
### Bug Fixes
* Total hit count fix in Hybrid Query ([756](https://github.com/opensearch-project/neural-search/pull/756))
* Fix map type validation issue in multiple pipeline processors ([#661](https://github.com/opensearch-project/neural-search/pull/661))
### Infrastructure
* Disable memory circuit breaker for integ tests ([#770](https://github.com/opensearch-project/neural-search/pull/770))
