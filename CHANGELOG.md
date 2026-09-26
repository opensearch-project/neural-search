# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features

### Enhancements
- [SemanticHighlighter] Support lists and scalars for semantic highlighting with per-element fragments, matching built-in highlighters ([#1813](https://github.com/opensearch-project/neural-search/issues/1813))
* [Hybrid Query] Add opt-in index setting `index.neural_search.hybrid_collapse_distinct_groups_enabled` to make collapse return top-`size` distinct groups instead of deduplicated top-`size` documents ([#1947](https://github.com/opensearch-project/neural-search/issues/1947))
* [Hybrid Query] Allow `sort: [_score, <field>]` with `collapse`, using the field to break exact `_score` ties so the collapsed group head is deterministic ([#1984](https://github.com/opensearch-project/neural-search/issues/1984))

### Bug Fixes
* [Hybrid Query] Fix NoSuchElementException in hybrid query with sort/search_after when a shard returns no results ([#1939](https://github.com/opensearch-project/neural-search/pull/1939))
* [SemanticHighlighter] Fix SemanticHighlighterExtBuilder.toXContent ([#1906](https://github.com/opensearch-project/neural-search/issues/1906)) (query-insights [#651](https://github.com/opensearch-project/query-insights/issues/651))
* [Sparse ANN] Fold sparse vector tokens into the signed-short range (modulus 32768) so folded tokens are never sign-extended to a negative value when stored in short[] ([#1926](https://github.com/opensearch-project/neural-search/pull/1926))
* [Hybrid Query] Read the current document and its sub-query matches from the positioned disjunction iterator, fixing an ArrayIndexOutOfBoundsException and silently misattributed scores when a sub-query has a two-phase iterator ([#1946](https://github.com/opensearch-project/neural-search/issues/1946))
* [RRF] Reject a combination technique other than rrf when creating a score-ranker-processor, instead of accepting the pipeline and throwing NullPointerException on every query ([#1949](https://github.com/opensearch-project/neural-search/pull/1949))
* [Sparse ANN] Skip cache cleanup for a closed index's shards, which have no MapperService, so reopening a sparse index no longer leaks the shard lock and leaves the shard unassigned ([#1982](https://github.com/opensearch-project/neural-search/issues/1982))
* [Hybrid Query] Fix inaccurate hits.total.value on hybrid queries with a small size, caused by top-k heap eviction feeding min-competitive-score pruning before track_total_hits' threshold was reached ([opensearch-project/OpenSearch#22823](https://github.com/opensearch-project/OpenSearch/issues/22823))
* [Hybrid Query] Fix hybrid `collapse` sorted by ascending `_score` returning wrong documents and an undercounted `hits.total` on shards with more than 4,096 matching documents ([#1795](https://github.com/opensearch-project/neural-search/issues/1795))
* [Sparse ANN] Widen the CSR indptr offset to 64-bit across the JNI/Java boundary (matching neural-sparse-cpp `offset_t=int64`) so a single segment can hold more than 2.15B cumulative non-zeros without int32 overflow ([#2001](https://github.com/opensearch-project/neural-search/pull/2001))
* [Sparse ANN] Bump neural-sparse-cpp for the DiskSeismic mmap madvise fix (MADV_RANDOM for per_block), fixing a large memory-constrained latency regression ([#2005](https://github.com/opensearch-project/neural-search/issues/2005))

### Infrastructure

### Documentation

### Maintenance

### Refactoring
