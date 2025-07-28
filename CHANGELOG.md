# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features
- [Hybrid Query] Add upper bound parameter for min-max normalization technique ([#1431](https://github.com/opensearch-project/neural-search/pull/1431))
- [Experimental] Add agentic search query clause for agentic search

### Enhancements
- [Semantic Field] Support configuring the auto-generated knn_vector field through the semantic field. ([#1420](https://github.com/opensearch-project/neural-search/pull/1420))
- [Semantic Field] Support configuring the ingest batch size for the semantic field. ([#1438](https://github.com/opensearch-project/neural-search/pull/1438))
- [Semantic Field] Allow configuring prune strategies for sparse encoding in semantic fields. ([#1434](https://github.com/opensearch-project/neural-search/pull/1434))
- Enable inner hits within collapse parameter for hybrid query ([#1447](https://github.com/opensearch-project/neural-search/pull/1447))
- [Semantic Field] Support configuring the chunking strategies through the semantic field. ([#1446](https://github.com/opensearch-project/neural-search/pull/1446))

### Bug Fixes
- Fix for collapse bug with knn query not deduplicating results ([#1413](https://github.com/opensearch-project/neural-search/pull/1413))
- Fix the HybridQueryDocIdStream to properly handle upTo value ([#1414](https://github.com/opensearch-project/neural-search/pull/1414))
- Handle remote dense model properly during mapping transform for the semantic field ([#1427](https://github.com/opensearch-project/neural-search/pull/1427))
- Handle a hybrid query extended with DLS rules by the security plugin ([#1432](https://github.com/opensearch-project/neural-search/pull/1432))
- Fix the minimal supported version for neural sparse query analyzer field ([#1475](https://github.com/opensearch-project/neural-search/pull/1475))

### Infrastructure

### Documentation

### Maintenance

### Refactoring
