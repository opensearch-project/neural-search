# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features

### Enhancements

### Bug Fixes
- [HYBRID]: Fix relevancy bugs in hybrid query collapse ([#1753](https://github.com/opensearch-project/neural-search/pull/1753))
- [Neural] Fix issue where remote symmetric models are not supported ([#1767](https://github.com/opensearch-project/neural-search/pull/1767))
- [HYBRID]: Fix profiler support for hybrid query by unwrapping ProfileScorer to access HybridSubQueryScorer ([#1754](https://github.com/opensearch-project/neural-search/pull/1754))
- [HYBRID]: Fix missing results and ranking issue in hybrid query collapse([#1763](https://github.com/opensearch-project/neural-search/pull/1763))
- [HYBRID]: Fix HybridQueryDocIdStream by adding intoArray overridden method from upstream ([#1780](https://github.com/opensearch-project/neural-search/pull/1780))
- [HYBRID]: Fix response of hybrid search with collapse same as without collapse ([#1787](https://github.com/opensearch-project/neural-search/pull/1787))


### Infrastructure
- Fix integration test health check failures in remote clusters by dynamically discovering node count and using >= syntax for wait_for_nodes ([#1776](https://github.com/opensearch-project/neural-search/pull/1776))

### Documentation

### Maintenance

### Refactoring
