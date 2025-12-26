# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features
- Add support for asymmetric embedding models([#1605](https://github.com/opensearch-project/neural-search/pull/1605))

### Enhancements
- [Agentic Search] Select explicit index for Agentic Query if returned from ListIndexTool

### Bug Fixes
- [SEISMIC]: Fix the memory usage track upon cache entry creation ([#1701](https://github.com/opensearch-project/neural-search/pull/1701))
- [HYBRID]: Fix for Hybrid Query with Collapse bugs([#1702](https://github.com/opensearch-project/neural-search/pull/1702))
- [HYBRID]: Fix position overflow of docIds in HybridBulkScorer to increase search relevance ([#1706](https://github.com/opensearch-project/neural-search/pull/1706))

### Infrastructure

### Documentation

### Maintenance

### Refactoring
