# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features
- Add support for asymmetric embedding models([#1605](https://github.com/opensearch-project/neural-search/pull/1605))
- Implement GRPC Hybrid Query ([#1665](https://github.com/opensearch-project/neural-search/pull/1665))
- Add support for min_score param in hybrid search([#1726](https://github.com/opensearch-project/neural-search/pull/1726))

### Enhancements
- [SEISMIC Query Explain]: Enable explain function within Sparse ANN query ([#1694](https://github.com/opensearch-project/neural-search/pull/1694))
- [SEISMIC]: Boost multi threads query efficiency ([#1712](https://github.com/opensearch-project/neural-search/pull/1712))
- Add ingest through sparse_vector field metrics([#1715](https://github.com/opensearch-project/neural-search/pull/1715))
- [Agentic Search] Select explicit index for Agentic Query if returned from ListIndexTool
- Include AdditionalCodecs argument to allow additional Codec registration ([#1741](https://github.com/opensearch-project/neural-search/pull/1741))

### Bug Fixes
- [SEISMIC]: Fix the memory usage track upon cache entry creation ([#1701](https://github.com/opensearch-project/neural-search/pull/1701))
- [HYBRID]: Fix for Hybrid Query with Collapse bugs([#1702](https://github.com/opensearch-project/neural-search/pull/1702))
- [HYBRID]: Fix position overflow of docIds in HybridBulkScorer to increase search relevance ([#1706](https://github.com/opensearch-project/neural-search/pull/1706))
- [HYBRID]: Fix logic of RRF score calculation as per document global rank in the subquery ([#1718](https://github.com/opensearch-project/neural-search/pull/1718))
- [HYBRID]: Fix runtime error when number of shards greater than default batch reduce size ([#1738](https://github.com/opensearch-project/neural-search/pull/1738))
- [HYBRID]: Fix CollapseTopFieldDocs logic when a segment has no collapsed fieldDocs but has totalHits > 0 ([#1740](https://github.com/opensearch-project/neural-search/pull/1740))
- [HYBRID]: Fix array_index_out_of_bound_exception in case of docsPerGroupPerSubQuery greater or lesser than numHits ([#1742](https://github.com/opensearch-project/neural-search/pull/1742))
- [HYBRID]: Fix null instance handling for empty and skipped shards ([#1745](https://github.com/opensearch-project/neural-search/pull/1745))

### Infrastructure
- [BWC]: Enable BWC tests after upgrading to Grade 9 ([#1729](https://github.com/opensearch-project/neural-search/pull/1729))
- [BWC]: Correct BWC tests between 3.5 and 2.19 ([#1737](https://github.com/opensearch-project/neural-search/pull/1737))
- [BWC]: Introduce BWC tests for nested field support with for Sparse ANN ([#1725](https://github.com/opensearch-project/neural-search/pull/1725))

### Documentation

### Maintenance

### Refactoring
