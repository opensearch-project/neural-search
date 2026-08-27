# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features

### Enhancements
* In-query fusion in hybrid search. Implement base classes and enable fusion (min_max and arithmetic mean) ([#1933](https://github.com/opensearch-project/neural-search/pull/1933))
* In-query fusion in hybrid search. Support nested hybrid queries, search across multiple indices, aggregations, collapse with group expansion, and point in time; refuse fused mode while any node in the cluster is below 3.8.0; refuse `scroll` in fused mode with a validation error (use `point_in_time` instead); cap the leg sub-searches one request may fan out with the `plugins.neural_search.hybrid.fusion.max_leg_searches` cluster setting ([#1943](https://github.com/opensearch-project/neural-search/pull/1943))
* In-query fusion in hybrid search. Support `z_score` and `l2` normalization in fused mode, so the whole score-normalization family is available with `arithmetic_mean`; both run the same shared normalization cores as the classic shard-side path ([#1962](https://github.com/opensearch-project/neural-search/pull/1962))
* In-query fusion in hybrid search. Refuse, with a validation error, a fused hybrid query that the request's own `query` does not expose, so that `plugins.neural_search.hybrid.fusion.max_leg_searches` counts every leg a request fans out: `post_filter`, aggregation filters, sorts, `rescore` and highlight queries are not counted — use a `bool` query with the same clauses as `should` there instead; `wrapper` and `template` carry a query without exposing it — write the hybrid directly instead ([#1969](https://github.com/opensearch-project/neural-search/pull/1969))
* In-query fusion in hybrid search. Report `matched_queries` for a sub-query carrying `_name`: named legs are registered on the shard even when the non-scoring Tail is not built, and a materialized kNN/neural leg keeps its `_name` — reported over the documents that leg returned, and carrying the substitute's score rather than the ANN similarity under `include_named_queries_score` ([#1967](https://github.com/opensearch-project/neural-search/pull/1967))
* In-query fusion in hybrid search. Define `rescore` semantics in fused mode: a `rescore` may rescale and reorder the hybrid's hits but no longer promote a document fusion did not rank, since every `rescore` query in the chain is confined to the fused window whatever its `window_size`; fused scores are floored to a small positive value so a ranked document outranks the non-scoring tail; a rescorer type other than `query` is rejected with a validation error ([#1973](https://github.com/opensearch-project/neural-search/pull/1973))
* In-query fusion in hybrid search. Support `profile` in fused mode: every leg reports its own profiler tree, under a shard entry labelled `[fused:hybrid_N.leg_M]` ([#1977](https://github.com/opensearch-project/neural-search/pull/1977))
* In-query fusion in hybrid search. Support `rrf` in fused mode as a normalizer over ranks, computing rank scores through the same code as the score-ranker-processor, and read `rank_constant` from the place each config shape's own classic factory reads it — the `combination` clause for `score-ranker-processor`, `normalization.parameters` for `normalization-processor` ([#1948](https://github.com/opensearch-project/neural-search/pull/1948))

### Bug Fixes
* [SemanticHighlighter] Fix SemanticHighlighterExtBuilder.toXContent ([#1906](https://github.com/opensearch-project/neural-search/issues/1906)) (query-insights [#651](https://github.com/opensearch-project/query-insights/issues/651))
* [Sparse ANN] Fold sparse vector tokens into the signed-short range (modulus 32768) so folded tokens are never sign-extended to a negative value when stored in short[] ([#1926](https://github.com/opensearch-project/neural-search/pull/1926))

### Infrastructure


### Documentation

### Maintenance

### Refactoring
* [SemanticHighlighter] Traverse the query tree with a QueryBuilderVisitor instead of a "manual" walk ([#1915](https://github.com/opensearch-project/neural-search/pull/1915))
* [RRF] Extract the rank arithmetic into a shared RRFScoreNormalizer and hoist the workflow duplicated between NormalizationProcessor and RRFProcessor into AbstractScoreHybridizationProcessor ([#1944](https://github.com/opensearch-project/neural-search/pull/1944))
* [z_score] Compute the per-subquery mean, standard deviation, max and min in a single DescriptiveStatistics pass instead of four, reducing normalization allocation by 4x ([#1960](https://github.com/opensearch-project/neural-search/pull/1960))
* Extract the z_score and l2 score-normalization arithmetic into shared ZScoreNormalizer and L2ScoreNormalizer cores, so the classic shard-side path and coordinator-side fused mode use one implementation of each formula ([#1961](https://github.com/opensearch-project/neural-search/pull/1961))
