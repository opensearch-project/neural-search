# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0](https://github.com/opensearch-project/neural-search/compare/2.x...HEAD)
### Features
### Enhancements
### Bug Fixes
- Fix for nested field missing sub embedding field in text embedding processor ([#913](https://github.com/opensearch-project/neural-search/pull/913))
### Infrastructure
### Documentation
### Maintenance
### Refactoring

## [Unreleased 2.x](https://github.com/opensearch-project/neural-search/compare/2.18...2.x)
### Features
- Pagination in Hybrid query ([#1048](https://github.com/opensearch-project/neural-search/pull/1048))
- Implement Reciprocal Rank Fusion score normalization/combination technique in hybrid query ([#874](https://github.com/opensearch-project/neural-search/pull/874))
### Enhancements
- Explainability in hybrid query ([#970](https://github.com/opensearch-project/neural-search/pull/970))
- Implement pruning for neural sparse ingestion pipeline and two phase search processor ([#988](https://github.com/opensearch-project/neural-search/pull/988))
- Support new knn query parameter expand_nested ([#1013](https://github.com/opensearch-project/neural-search/pull/1013))
- Support empty string for fields in text embedding processor ([#1041](https://github.com/opensearch-project/neural-search/pull/1041))
- Optimize ML inference connection retry logic ([#1054](https://github.com/opensearch-project/neural-search/pull/1054))
- Support for builder constructor in Neural Query Builder ([#1047](https://github.com/opensearch-project/neural-search/pull/1047))
### Bug Fixes
- Address inconsistent scoring in hybrid query results ([#998](https://github.com/opensearch-project/neural-search/pull/998))
- Fix bug where ingested document has list of nested objects ([#1040](https://github.com/opensearch-project/neural-search/pull/1040))
- Fixed document source and score field mismatch in sorted hybrid queries ([#1043](https://github.com/opensearch-project/neural-search/pull/1043))
- Update NeuralQueryBuilder doEquals() and doHashCode() to cater the missing parameters information ([#1045](https://github.com/opensearch-project/neural-search/pull/1045)).
- Fix bug where embedding is missing when ingested document has "." in field name, and mismatches fieldMap config ([#1062](https://github.com/opensearch-project/neural-search/pull/1062))
### Infrastructure
- Update batch related tests to use batch_size in processor & refactor BWC version check ([#852](https://github.com/opensearch-project/neural-search/pull/852))
- Fix CI for JDK upgrade towards 21 ([#835](https://github.com/opensearch-project/neural-search/pull/835))
### Documentation
### Maintenance
- Add reindex integration tests for ingest processors ([#1075](https://github.com/opensearch-project/neural-search/pull/1075))
- Fix github CI by adding eclipse dependency in formatting.gradle ([#1079](https://github.com/opensearch-project/neural-search/pull/1079))
### Refactoring
