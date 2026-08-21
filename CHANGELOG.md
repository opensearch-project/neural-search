# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features

### Enhancements
* In-query fusion in hybrid search. Implement base classes and enable fusion (min_max and arithmetic mean) ([#1933](https://github.com/opensearch-project/neural-search/pull/1933))
* In-query fusion in hybrid search. Support nested hybrid queries, search across multiple indices, aggregations, collapse with group expansion, and point in time; refuse fused mode while any node in the cluster is below 3.8.0; refuse `scroll` in fused mode with a validation error (use `point_in_time` instead); cap the leg sub-searches one request may fan out with the `plugins.neural_search.hybrid.fusion.max_leg_searches` cluster setting ([#1943](https://github.com/opensearch-project/neural-search/pull/1943))

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
