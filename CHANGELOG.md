# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features

- [SEISMIC] Support SEISMIC, a new sparse ANN algorithm [#1578](https://github.com/opensearch-project/neural-search/pull/1578), [#1577](https://github.com/opensearch-project/neural-search/pull/1577), [#1566](https://github.com/opensearch-project/neural-search/pull/1566), [#1565](https://github.com/opensearch-project/neural-search/pull/1565), [#1564](https://github.com/opensearch-project/neural-search/pull/1564), [#1563](https://github.com/opensearch-project/neural-search/pull/1563), [#1562](https://github.com/opensearch-project/neural-search/pull/1562), [#1559](https://github.com/opensearch-project/neural-search/pull/1559), [#1557](https://github.com/opensearch-project/neural-search/pull/1557), [#1555](https://github.com/opensearch-project/neural-search/pull/1555), [#1554](https://github.com/opensearch-project/neural-search/pull/1554), [#1553](https://github.com/opensearch-project/neural-search/pull/1553), [#1539](https://github.com/opensearch-project/neural-search/pull/1539), [#1538](https://github.com/opensearch-project/neural-search/pull/1538), [#1537](https://github.com/opensearch-project/neural-search/pull/1537), [#1536](https://github.com/opensearch-project/neural-search/pull/1536), [#1524](https://github.com/opensearch-project/neural-search/pull/1524), [#1514](https://github.com/opensearch-project/neural-search/pull/1514), [#1502](https://github.com/opensearch-project/neural-search/pull/1502)

### Enhancements

- [Semantic Field] Support the sparse two phase processor for the semantic field.
- [Stats] Add stats for agentic query and agentic query translator processor.
- [Agentic Search] Adds validations and logging for agentic query
- [Performance Improvement] Introduce QueryCollectContextSpec in Hybrid Query to improve search performance.
- [Agentic Search] Add support for conversational agent

### Bug Fixes
- Fix reversed order of values in nested list with embedding processor [#1570](https://github.com/opensearch-project/neural-search/pull/1570)
- [Semantic Field] Fix not able to index the multiFields for the rawFieldType [#1572](https://github.com/opensearch-project/neural-search/pull/1572).

### Infrastructure

- [Unit Test] Enable mocking of final classes and static functions ([#1528](https://github.com/opensearch-project/neural-search/pull/1528)).

### Documentation

### Maintenance

- Remove commons-lang:commons-lang dependency and use gradle version catalog for commons-lang3 ([#1551](https://github.com/opensearch-project/neural-search/pull/1551))
- Update Lucene101 codec path to backward path & force errorprone version to 2.21.1 to resolve conflict ([#1574](https://github.com/opensearch-project/neural-search/pull/1574))

### Refactoring
