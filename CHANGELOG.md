# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features

### Enhancements
- [Agentic Search] Add conversation search support with agentic search ([#1626](https://github.com/opensearch-project/neural-search/pull/1626))
- [Agentic Search] Extract JSON from Agent Response ([#1631](https://github.com/opensearch-project/neural-search/pull/1631))
- [Agentic Search] Extract agent summary based on models ([#1633](https://github.com/opensearch-project/neural-search/pull/1633))

### Bug Fixes
- [SEISMIC]: Resolve a security risk of Sparse ANN - Move 'index.sparse' validation from REST to transport layer. ([#1630](https://github.com/opensearch-project/neural-search/pull/1630))
- [SEISMIC IT]: Fix some failed IT cases ([#1649](https://github.com/opensearch-project/neural-search/pull/1649), [#1653](https://github.com/opensearch-project/neural-search/pull/1653))
- [Hybrid Search]: Fix for hybrid search collapse bug when there are no documents in a shard ([#1647](https://github.com/opensearch-project/neural-search/pull/1647))

### Infrastructure
- Onboard to s3 snapshots ([#1618](https://github.com/opensearch-project/neural-search/pull/1618))
- Add BWC tests for Sparse ANN Seismic feature ([#1657](https://github.com/opensearch-project/neural-search/pull/1657))

### Documentation

### Maintenance

### Refactoring
