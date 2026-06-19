# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features

### Enhancements
* Improve hybrid query filter validation error message ([#1870](https://github.com/opensearch-project/neural-search/pull/1870))
### Bug Fixes
* [Hybrid Query] Block hybrid query execution with `search_type=dfs_query_then_fetch` ([#1873](https://github.com/opensearch-project/neural-search/pull/1873))
* [Hybrid Query] Fix `hybrid_score_explanation` returning a single normalization block for hybrid queries on indices that contain a nested field ([#1876](https://github.com/opensearch-project/neural-search/pull/1876))

### Infrastructure

### Documentation

### Maintenance

### Refactoring
