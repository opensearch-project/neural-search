# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features
* [Hybrid Query] Add fusion mode to the hybrid query ([#2036](https://github.com/opensearch-project/neural-search/pull/2036))
* [Hybrid Query] Turn fusion mode on by default; `plugins.neural_search.hybrid.fusion.enabled` becomes the switch that turns it off ([#2037](https://github.com/opensearch-project/neural-search/pull/2037))

### Enhancements
- [SemanticHighlighter] Support lists and scalars for semantic highlighting with per-element fragments, matching built-in highlighters ([#1813](https://github.com/opensearch-project/neural-search/issues/1813))
* [Hybrid Query] Add opt-in index setting `index.neural_search.hybrid_collapse_distinct_groups_enabled` to make collapse return top-`size` distinct groups instead of deduplicated top-`size` documents ([#1947](https://github.com/opensearch-project/neural-search/issues/1947))
* [Hybrid Query] Allow `sort: [_score, <field>]` with `collapse`, using the field to break exact `_score` ties so the collapsed group head is deterministic ([#1984](https://github.com/opensearch-project/neural-search/issues/1984))
* [Hybrid Query] Accept `pagination_depth` in fused hybrid query ([#2058](https://github.com/opensearch-project/neural-search/pull/2058))
* [Hybrid Query] Accept top-level `weights` in classic and fused hybrid query modes ([#2065](https://github.com/opensearch-project/neural-search/pull/2065))

### Bug Fixes
* [Neural Sparse Query] Expose embedded filters to query builder and Lucene query visitor traversal, and reject unsupported filters on `rank_features` fields instead of silently ignoring them ([#2044](https://github.com/opensearch-project/neural-search/pull/2044))
* [Hybrid Query] Fix hybrid `collapse` sorted by ascending `_score` returning wrong documents and an undercounted `hits.total` on shards with more than 4,096 matching documents ([#1795](https://github.com/opensearch-project/neural-search/issues/1795))
* [Ingest Processors] Pass `_routing` to the `skip_existing` lookup so writes to indices with required or custom routing no longer fail or silently re-embed ([#2024](https://github.com/opensearch-project/neural-search/issues/2024))
* [Semantic Field] Fix the semantic field processor invoking the ingest handler twice on embedding-generation failure, which indexed the document without its embedding instead of failing the write ([#2033](https://github.com/opensearch-project/neural-search/pull/2033))
* [Semantic Field] Pass `_routing` to the `skip_existing_embedding` lookup so unchanged documents in routed indices reuse their embeddings instead of being silently re-embedded ([#2040](https://github.com/opensearch-project/neural-search/pull/2040))
* [Hybrid Query] Reject sorting a hybrid query by `_score` more than once, or by ascending `_score` without `collapse`, which returned wrong documents and scores ([#2046](https://github.com/opensearch-project/neural-search/pull/2046))
* [Hybrid Query] Fix hybrid query failing with a `ClassCastException` when a point in time `slice` or an alias filter excludes a shard ([#2045](https://github.com/opensearch-project/neural-search/pull/2045))
* [Ingest Processors] Fix `field_map` entries sharing the same first dotted path segment (e.g. `author.name` and `author.bio`) overwriting each other, which silently dropped embeddings for all but the last field ([#2052](https://github.com/opensearch-project/neural-search/pull/2052))
* [Hybrid Query] Reject a hybrid query without `fusion` nested in a hybrid query with `fusion`, and the other way around ([#2051](https://github.com/opensearch-project/neural-search/pull/2051))
* [Ingest Processors] Deep-merge `field_map` target maps so sibling keys that diverge below the first path segment (e.g. `author.name.first` and `author.name.last`) are no longer dropped ([#2054](https://github.com/opensearch-project/neural-search/pull/2054))
* [Semantic Field] Stop logging the full ingest document on chunking or model-info failure; log only the document `_id` and `_index` so customer content is no longer written to service logs ([#2057](https://github.com/opensearch-project/neural-search/pull/2057))

### Infrastructure

### Documentation

### Maintenance

### Refactoring
* [RRF] Extract the rank arithmetic into a shared RRFScoreNormalizer and hoist the workflow duplicated between NormalizationProcessor and RRFProcessor into AbstractScoreHybridizationProcessor ([#1944](https://github.com/opensearch-project/neural-search/pull/1944))
* [z_score] Compute the per-subquery mean, standard deviation, max and min in a single DescriptiveStatistics pass instead of four, reducing normalization allocation by 4x ([#1960](https://github.com/opensearch-project/neural-search/pull/1960))
* Extract the z_score and l2 score-normalization arithmetic into shared ZScoreNormalizer and L2ScoreNormalizer cores, so the classic shard-side path and coordinator-side fused mode use one implementation of each formula ([#1961](https://github.com/opensearch-project/neural-search/pull/1961))
