# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features
- [Hybrid] Support Sub Query Scores for Hybrid Search ([#1369](https://github.com/opensearch-project/neural-search/pull/1369))

### Enhancements
- [Semantic Field] Support configuring the auto-generated knn_vector field through the semantic field. ([#1420](https://github.com/opensearch-project/neural-search/pull/1420))

### Bug Fixes
- Fix for collapse bug with knn query not deduplicating results ([#1413](https://github.com/opensearch-project/neural-search/pull/1413))
- Fix the HybridQueryDocIdStream to properly handle upTo value ([#1414](https://github.com/opensearch-project/neural-search/pull/1414))
- Handle remote dense model properly during mapping transform for the semantic field ([#1427](https://github.com/opensearch-project/neural-search/pull/1427))

### Infrastructure

### Documentation

### Maintenance

### Refactoring
