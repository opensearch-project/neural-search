# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0](https://github.com/opensearch-project/neural-search/compare/2.x...HEAD)
### Features
### Enhancements
### Bug Fixes
- Fix async actions are left in neural_sparse query ([#438](https://github.com/opensearch-project/neural-search/pull/438))
- Fixed exception for case when Hybrid query being wrapped into bool query ([#490](https://github.com/opensearch-project/neural-search/pull/490))
- Hybrid query and nested type fields ([#498](https://github.com/opensearch-project/neural-search/pull/498))
- Fix typo for sparse encoding processor factory([#578](https://github.com/opensearch-project/neural-search/pull/578))
### Infrastructure
### Documentation
### Maintenance
- Added support for jdk-21 ([#500](https://github.com/opensearch-project/neural-search/pull/500)))
### Refactoring

## [Unreleased 2.x](https://github.com/opensearch-project/neural-search/compare/2.11...2.x)
### Features
### Enhancements
### Bug Fixes
- Fixing multiple issues reported in #497 ([#524](https://github.com/opensearch-project/neural-search/pull/524))
- Fix Flaky test reported in #433 ([#533](https://github.com/opensearch-project/neural-search/pull/533))
- Enable support for default model id on HybridQueryBuilder ([#541](https://github.com/opensearch-project/neural-search/pull/541))
- Fix Flaky test reported in #384 ([#559](https://github.com/opensearch-project/neural-search/pull/559))
### Infrastructure
- BWC tests for Neural Search ([#515](https://github.com/opensearch-project/neural-search/pull/515))
- Github action to run integ tests in secure opensearch cluster ([#535](https://github.com/opensearch-project/neural-search/pull/535))
- BWC tests for Multimodal search, Hybrid Search and Neural Sparse Search ([#533](https://github.com/opensearch-project/neural-search/pull/533))
### Documentation
### Maintenance
### Refactoring
- Added spotless check in the build ([#515](https://github.com/opensearch-project/neural-search/pull/515))
