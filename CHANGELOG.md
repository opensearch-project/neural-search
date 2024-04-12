# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0](https://github.com/opensearch-project/neural-search/compare/2.x...HEAD)
### Features
### Enhancements
### Bug Fixes
### Infrastructure
### Documentation
### Maintenance
### Refactoring

## [Unreleased 2.x](https://github.com/opensearch-project/neural-search/compare/2.14...2.x)
### Features
- Support batchExecute in TextEmbeddingProcessor and SparseEncodingProcessor ([#743](https://github.com/opensearch-project/neural-search/issues/743))
### Enhancements
- Pass empty doc collector instead of top docs collector to improve hybrid query latencies by 20% ([#731](https://github.com/opensearch-project/neural-search/pull/731))
- Optimize parameter parsing in text chunking processor ([#733](https://github.com/opensearch-project/neural-search/pull/733))
- Use lazy initialization for priority queue of hits and scores to improve latencies by 20% ([#746](https://github.com/opensearch-project/neural-search/pull/746))
### Bug Fixes
- Total hit count fix in Hybrid Query ([756](https://github.com/opensearch-project/neural-search/pull/756))
- Fix map type validation issue in multiple pipeline processors ([#661](https://github.com/opensearch-project/neural-search/pull/661))
### Infrastructure
### Documentation
### Maintenance
### Refactoring
