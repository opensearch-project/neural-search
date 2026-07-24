## Version 3.8.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.8.0

### Enhancements

* Allow custom field name for storing previous rerank score to avoid overwriting existing document fields ([#1880](https://github.com/opensearch-project/neural-search/pull/1880))
* Improve hybrid query filter validation error message to provide guidance on combining multiple filters ([#1870](https://github.com/opensearch-project/neural-search/pull/1870))
* Skip two-phase rescore optimization for queries containing sort fields ([#1898](https://github.com/opensearch-project/neural-search/pull/1898))

### Bug Fixes

* Block hybrid query execution with `dfs_query_then_fetch` search type to prevent incorrect results ([#1873](https://github.com/opensearch-project/neural-search/pull/1873))
* Fix hybrid query explanation producing a single normalization block instead of per-sub-query blocks for indices with nested fields ([#1876](https://github.com/opensearch-project/neural-search/pull/1876))

### Infrastructure

* Fix Check Workflow Events CI job by adding `pr_review.yml` to the allowlist and removing stray YAML fragment ([#1901](https://github.com/opensearch-project/neural-search/pull/1901))
* Fix flaky `HybridQueryExplainIT` explanation test by using per-document `_id` assertions ([#1900](https://github.com/opensearch-project/neural-search/pull/1900))
* Onboard new backport-pr reusable GitHub workflow ([#1881](https://github.com/opensearch-project/neural-search/pull/1881))
