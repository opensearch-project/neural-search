# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0](https://github.com/opensearch-project/neural-search/compare/2.x...HEAD)
### Features
### Enhancements
### Bug Fixes
### Infrastructure
- Adding integration tests for scenario of hybrid query with aggregations ([#632](https://github.com/opensearch-project/neural-search/pull/632))
### Documentation
### Maintenance
### Refactoring

## [Unreleased 2.x](https://github.com/opensearch-project/neural-search/compare/2.13...2.x)
### Features
### Enhancements
- BWC tests for text chunking processor ([#661](https://github.com/opensearch-project/neural-search/pull/661))
- Allowing execution of hybrid query on index alias with filters ([#670](https://github.com/opensearch-project/neural-search/pull/670))
- Allowing query by raw tokens in neural_sparse query ([#693](https://github.com/opensearch-project/neural-search/pull/693))
- Removed stream.findFirst implementation to use more native iteration implement to improve hybrid query latencies by 35% ([#706](https://github.com/opensearch-project/neural-search/pull/706))
### Bug Fixes
- Add support for request_cache flag in hybrid query ([#663](https://github.com/opensearch-project/neural-search/pull/663))
### Infrastructure
### Documentation
### Maintenance
- Update bwc tests for neural_query_enricher neural_sparse search ([#652](https://github.com/opensearch-project/neural-search/pull/652))
### Refactoring
