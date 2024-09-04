# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0](https://github.com/opensearch-project/neural-search/compare/2.x...HEAD)
### Features
### Enhancements
- Set neural-search plugin 3.0.0 baseline JDK version to JDK-2 ([#838](https://github.com/opensearch-project/neural-search/pull/838))
### Bug Fixes
### Infrastructure
### Documentation
### Maintenance
### Refactoring

## [Unreleased 2.x](https://github.com/opensearch-project/neural-search/compare/2.16...2.x)
### Features
- Implement Reciprocal Rank Fusion score normalization/combination technique in hybrid query ([#874](https://github.com/opensearch-project/neural-search/pull/874))
### Enhancements
### Bug Fixes
- Fixed merge logic in hybrid query for multiple shards case ([#877](https://github.com/opensearch-project/neural-search/pull/877))
### Infrastructure
- Update batch related tests to use batch_size in processor & refactor BWC version check ([#852](https://github.com/opensearch-project/neural-search/pull/852))
### Documentation
### Maintenance
### Refactoring
