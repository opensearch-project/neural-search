## Version 3.5.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.5.0

### Features

* [GRPC] Implement Hybrid Queries ([#1665](https://github.com/opensearch-project/neural-search/pull/1665))
* [Agentic Search] Select explicit index for Agentic Query if returned from ListIndexTool ([#1713](https://github.com/opensearch-project/neural-search/pull/1713))

### Enhancements

* Enhance: Sparse ANN boost multi threads query efficiency ([#1712](https://github.com/opensearch-project/neural-search/pull/1712))
* [Feature Enhancement] Enable explain function within Sparse ANN query ([#1694](https://github.com/opensearch-project/neural-search/pull/1694))
* Apply min_score to final results in hybrid query ([#1726](https://github.com/opensearch-project/neural-search/pull/1726))
* Add sparse_vector ingest metrics ([#1715](https://github.com/opensearch-project/neural-search/pull/1715))

### Bug Fixes

* Fix postion overflow in hybrid bulk scorer ([#1706](https://github.com/opensearch-project/neural-search/pull/1706))
* Fix: Correct BWC tests between 3.5.0 and 2.19.0 ([#1737](https://github.com/opensearch-project/neural-search/pull/1737))
* Fix: memory usage track ([#1701](https://github.com/opensearch-project/neural-search/pull/1701))
* Fixed runtime error when number of shards greater than default batch reduce size ([#1738](https://github.com/opensearch-project/neural-search/pull/1738))
* Fix null instance handling for empty and skipped shards ([#1745](https://github.com/opensearch-project/neural-search/pull/1745))
* [Fix] Enable BWC tests ([#1729](https://github.com/opensearch-project/neural-search/pull/1729))
* Fix bugs in Hybrid Query with collapse ([#1702](https://github.com/opensearch-project/neural-search/pull/1702))
* Fix CollapseTopFieldDocs logic when a segment has no collapsed fieldDocs but has totalHits > 0 ([#1740](https://github.com/opensearch-project/neural-search/pull/1740))
* Fix array_index_out_of_bound_exception in case of docsPerGroupPerSubQuery greater or lesser than numHits ([#1742](https://github.com/opensearch-project/neural-search/pull/1742))
* Add subquery global rank for RRF score calculation & fix position overflow in HybridBulkScorer ([#1718](https://github.com/opensearch-project/neural-search/pull/1718))

### Infrastructure

* Include AdditionalCodecs argument to allow additional Codec registration ([#1741](https://github.com/opensearch-project/neural-search/pull/1741))
* [Seismic] BWC tests for nested field ([#1725](https://github.com/opensearch-project/neural-search/pull/1725))