## Version 2.19.0.0 Release Notes

Compatible with OpenSearch 2.19.0

### Features
* Pagination in Hybrid query ([#1048](https://github.com/opensearch-project/neural-search/pull/1048))
* Implement Reciprocal Rank Fusion score normalization/combination technique in hybrid query ([#874](https://github.com/opensearch-project/neural-search/pull/874))
### Bug Fixes
* Address inconsistent scoring in hybrid query results ([#998](https://github.com/opensearch-project/neural-search/pull/998))
* Fix bug where ingested document has list of nested objects ([#1040](https://github.com/opensearch-project/neural-search/pull/1040))
* Fixed document source and score field mismatch in sorted hybrid queries ([#1043](https://github.com/opensearch-project/neural-search/pull/1043))
* Update NeuralQueryBuilder doEquals() and doHashCode() to cater the missing parameters information ([#1045](https://github.com/opensearch-project/neural-search/pull/1045)).
* Fix bug where embedding is missing when ingested document has "." in field name, and mismatches fieldMap config ([#1062](https://github.com/opensearch-project/neural-search/pull/1062))
### Enhancements
* Explainability in hybrid query ([#970](https://github.com/opensearch-project/neural-search/pull/970))
* Support new knn query parameter expand_nested ([#1013](https://github.com/opensearch-project/neural-search/pull/1013))
* Implement pruning for neural sparse ingestion pipeline and two phase search processor ([#988](https://github.com/opensearch-project/neural-search/pull/988))
* Support empty string for fields in text embedding processor ([#1041](https://github.com/opensearch-project/neural-search/pull/1041))
* Optimize ML inference connection retry logic ([#1054](https://github.com/opensearch-project/neural-search/pull/1054))
* Support for builder constructor in Neural Query Builder ([#1047](https://github.com/opensearch-project/neural-search/pull/1047))
* Validate Disjunction query to avoid having nested hybrid query ([#1127](https://github.com/opensearch-project/neural-search/pull/1127))
### Maintenance
* Add reindex integration tests for ingest processors ([#1075](https://github.com/opensearch-project/neural-search/pull/1075))
* Fix github CI by adding eclipse dependency in formatting.gradle ([#1079](https://github.com/opensearch-project/neural-search/pull/1079))