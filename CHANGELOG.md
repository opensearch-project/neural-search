# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0](https://github.com/opensearch-project/neural-search/compare/2.x...HEAD)
### Features
- Enabled support for applying default modelId in neural search query ([#337](https://github.com/opensearch-project/neural-search/pull/337)
### Enhancements
### Bug Fixes
### Infrastructure
### Documentation
### Maintenance
### Refactoring

## [Unreleased 2.x](https://github.com/opensearch-project/neural-search/compare/2.10...2.x)
### Features
Support sparse semantic retrieval by introducing `sparse_encoding` ingest processor and query builder ([#333](https://github.com/opensearch-project/neural-search/pull/333))
Added Multimodal semantic search feature ([#359](https://github.com/opensearch-project/neural-search/pull/359))
### Enhancements
Add `max_token_score` parameter to improve the execution efficiency for `neural_sparse` query clause ([#348](https://github.com/opensearch-project/neural-search/pull/348))
### Bug Fixes
### Infrastructure
### Documentation
### Maintenance
Consumed latest changes from core, use QueryPhaseSearcherWrapper as parent class for Hybrid QPS ([#356](https://github.com/opensearch-project/neural-search/pull/356))
### Refactoring
