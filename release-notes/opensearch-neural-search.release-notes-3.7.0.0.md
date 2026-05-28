## Version 3.7.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.7.0

### Enhancements

* Fix batch semantic highlighting on inner_hits fields by improving the batch processor and adding a request-level opt-in ([#1858](https://github.com/opensearch-project/neural-search/pull/1858))
* Propagate setMinCompetitiveScore to sub-query scorers in HybridBulkScorer to reduce collected documents and improve response time ([#1831](https://github.com/opensearch-project/neural-search/pull/1831))
  
### Bug Fixes

* Fix flaky integration test failure caused by ML memory circuit breaker during model deployment by adding deploy retry logic ([#1824](https://github.com/opensearch-project/neural-search/pull/1824))


### Infrastructure

* Add Maven cache mirror before mavenCentral to reduce HTTP 429 Too Many Requests throttling in CI builds ([#1856](https://github.com/opensearch-project/neural-search/pull/1856))
* Add gRPC integration tests for hybrid query with normalization pipeline, sort, and collapse ([#1827](https://github.com/opensearch-project/neural-search/pull/1827))
* Pin actions/github-script to exact commit SHA for improved supply chain security ([#1860](https://github.com/opensearch-project/neural-search/pull/1860))

### Maintenance

* Upgrade Gradle wrapper to 9.4.1 ([#1849](https://github.com/opensearch-project/neural-search/pull/1849))
* * Unwrap LeafReader to fix compatibility issue with core PR 21318 that wrapped SparsePostingsEnum into ExitablePostingsEnum ([#1855](https://github.com/opensearch-project/neural-search/pull/1855))

### Refactoring

* Unify highlight tag application logic into shared HighlightTagApplier utility with strict validation for invalid model output ([#1862](https://github.com/opensearch-project/neural-search/pull/1862))
