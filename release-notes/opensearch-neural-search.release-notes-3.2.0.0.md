## Version 3.2.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.2.0

### Features
* [Hybrid Query] Add upper bound parameter for min-max normalization technique ([#1431](https://github.com/opensearch-project/neural-search/pull/1431))
* [Experimental] Adds agentic search query clause and agentic query translator search request processor for agentic search ([#1484](https://github.com/opensearch-project/neural-search/pull/1484))

### Enhancements
* [Semantic Field] Support configuring the auto-generated knn_vector field through the semantic field. ([#1420](https://github.com/opensearch-project/neural-search/pull/1420))
* [Semantic Field] Support configuring the ingest batch size for the semantic field. ([#1438](https://github.com/opensearch-project/neural-search/pull/1438))
* [Semantic Field] Allow configuring prune strategies for sparse encoding in semantic fields. ([#1434](https://github.com/opensearch-project/neural-search/pull/1434))
* Enable inner hits within collapse parameter for hybrid query ([#1447](https://github.com/opensearch-project/neural-search/pull/1447))
* [Semantic Field] Support configuring the chunking strategies through the semantic field. ([#1446](https://github.com/opensearch-project/neural-search/pull/1446))
* [Semantic Field] Support configuring reusing existing embedding for the semantic field. ([#1480](https://github.com/opensearch-project/neural-search/pull/1480/files))
* Add setting for number of documents stored by HybridCollapsingTopDocsCollector ([#1471](https://github.com/opensearch-project/neural-search/pull/1471))

### Bug Fixes
* Fix for collapse bug with knn query not deduplicating results ([#1413](https://github.com/opensearch-project/neural-search/pull/1413))
* Fix the HybridQueryDocIdStream to properly handle upTo value ([#1414](https://github.com/opensearch-project/neural-search/pull/1414))
* Handle remote dense model properly during mapping transform for the semantic field ([#1427](https://github.com/opensearch-project/neural-search/pull/1427))
* Handle a hybrid query extended with DLS rules by the security plugin ([#1432](https://github.com/opensearch-project/neural-search/pull/1432))
* Fix the minimal supported version for neural sparse query analyzer field ([#1475](https://github.com/opensearch-project/neural-search/pull/1475))

### Infrastructure
* Support multi node integration testing ([#1320](https://github.com/opensearch-project/neural-search/pull/1320))