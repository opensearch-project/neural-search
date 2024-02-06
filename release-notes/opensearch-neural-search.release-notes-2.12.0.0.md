## Version 2.12.0.0 Release Notes

Compatible with OpenSearch 2.12.0

### Features
- Add rerank processor interface and ml-commons reranker ([#494](https://github.com/opensearch-project/neural-search/pull/494))
### Bug Fixes
- Fixed exception for case when Hybrid query being wrapped into bool query ([#490](https://github.com/opensearch-project/neural-search/pull/490))
- Hybrid query and nested type fields ([#498](https://github.com/opensearch-project/neural-search/pull/498))
- Fixing multiple issues reported in #497 ([#524](https://github.com/opensearch-project/neural-search/pull/524))
- Fix Flaky test reported in #433 ([#533](https://github.com/opensearch-project/neural-search/pull/533))
- Enable support for default model id on HybridQueryBuilder ([#541](https://github.com/opensearch-project/neural-search/pull/541))
- Fix Flaky test reported in #384 ([#559](https://github.com/opensearch-project/neural-search/pull/559))
- Add validations for reranker requests per #555 ([#562](https://github.com/opensearch-project/neural-search/pull/562))
### Infrastructure
- BWC tests for Neural Search ([#515](https://github.com/opensearch-project/neural-search/pull/515))
- Github action to run integ tests in secure opensearch cluster ([#535](https://github.com/opensearch-project/neural-search/pull/535))
- BWC tests for Multimodal search, Hybrid Search and Neural Sparse Search ([#533](https://github.com/opensearch-project/neural-search/pull/533))
- Distribution bundle bwc tests ([#579])(https://github.com/opensearch-project/neural-search/pull/579)
### Maintenance
- Added support for jdk-21 ([#500](https://github.com/opensearch-project/neural-search/pull/500)))
- Update spotless and eclipse dependencies ([#589](https://github.com/opensearch-project/neural-search/pull/589))
### Refactoring
- Deprecate the `max_token_score` field in `neural_sparse` query clause ([#478](https://github.com/opensearch-project/neural-search/pull/478))
- Added spotless check in the build ([#515](https://github.com/opensearch-project/neural-search/pull/515))