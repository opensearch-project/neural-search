# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features

### Enhancements
- [SemanticHighlighter] Support lists and scalars for semantic highlighting with per-element fragments, matching built-in highlighters ([#1813](https://github.com/opensearch-project/neural-search/issues/1813))
* [Hybrid Query] Add opt-in index setting `index.neural_search.hybrid_collapse_distinct_groups_enabled` to make collapse return top-`size` distinct groups instead of deduplicated top-`size` documents ([#1947](https://github.com/opensearch-project/neural-search/issues/1947))

### Bug Fixes
* [Hybrid Query] Fix hybrid `collapse` sorted by ascending `_score` returning wrong documents and an undercounted `hits.total` on shards with more than 4,096 matching documents ([#1795](https://github.com/opensearch-project/neural-search/issues/1795))

### Infrastructure

### Documentation

### Maintenance

### Refactoring
