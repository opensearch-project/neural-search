# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0](https://github.com/opensearch-project/neural-search/compare/2.x...HEAD)
### Features
- Lower bound for min-max normalization technique in hybrid query ([#1195](https://github.com/opensearch-project/neural-search/pull/1195))
### Enhancements
- Set neural-search plugin 3.0.0 baseline JDK version to JDK-21 ([#838](https://github.com/opensearch-project/neural-search/pull/838))
- Support different embedding types in model's response ([#1007](https://github.com/opensearch-project/neural-search/pull/1007))
### Bug Fixes
- Fix a bug to unflatten the doc with list of map with multiple entries correctly ([#1204](https://github.com/opensearch-project/neural-search/pull/1204)).
### Infrastructure
- [3.0] Update neural-search for OpenSearch 3.0 compatibility ([#1141](https://github.com/opensearch-project/neural-search/pull/1141))
### Documentation
### Maintenance
### Refactoring
- Encapsulate KNNQueryBuilder creation within NeuralKNNQueryBuilder ([#1183](https://github.com/opensearch-project/neural-search/pull/1183))

## [Unreleased 2.x](https://github.com/opensearch-project/neural-search/compare/2.19...2.x)
### Features
### Enhancements
### Bug Fixes
### Infrastructure
### Documentation
### Maintenance
### Refactoring
