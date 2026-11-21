# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features

### Enhancements
* [Hybrid Query] Allow `sort: [_score, <field>]` without `collapse` and page it with `search_after`, also fixes per-sub-query windows for field sorts, #2042 ([#2082](https://github.com/opensearch-project/neural-search/pull/2082))

### Bug Fixes
* [Hybrid Query] Fix `explain: true` changing the `_score` values and, for `z_score`, the hit order of a classic hybrid query: scores are now normalized once, and the explanation's top node matches its children ([#2104](https://github.com/opensearch-project/neural-search/pull/2104))
* [Hybrid Query] Point the `sort: [_score, <field>]` without `collapse` version gate at the release that ships it, so a rolling upgrade no longer accepts the shape while a node that cannot order such a page is still in the cluster ([#2103](https://github.com/opensearch-project/neural-search/pull/2103))
* [Hybrid Query] Fix concurrent segment search changing hybrid query results: each shard now keeps at most `pagination_depth` (or `size`) hits per sub-query. Score-ordered, field-sorted and default-mode `collapse` results no longer depend on the segment layout, `rescore` runs once per shard, single-shard requests fetch only the kept hits, and pages beyond `pagination_depth` return "Reached end of result"; distinct-groups collapse not affected ([#2093](https://github.com/opensearch-project/neural-search/pull/2093))
* [Hybrid Query] Fix concurrent sorted hybrid queries returning wrong documents or failing with runtime exception ([#2089](https://github.com/opensearch-project/neural-search/pull/2089))
* [Hybrid Query] Resolve the index default search pipeline as core does when a targeted index sets it to `_none`, for `fusion: "pipeline"` and top-level `weights` ([#2084](https://github.com/opensearch-project/neural-search/pull/2084))
* [Hybrid Query] Fixed hybrid query without search pipeline, it runs run fusion mode with default techniques ([#2084](https://github.com/opensearch-project/neural-search/pull/2084))
* [Hybrid Query] Fix inaccurate hits.total.value on hybrid queries with a small size, caused by top-k heap eviction feeding min-competitive-score pruning before track_total_hits' threshold was reached ([opensearch-project/OpenSearch#22823](https://github.com/opensearch-project/OpenSearch/issues/22823))

### Infrastructure
* Fix flaky hybrid collapse collector tests by keeping insertion order when the test index is force-merged ([#2075](https://github.com/opensearch-project/neural-search/issues/2075))
* Add a multi-segment index writer helper to the hybrid collapse collector tests ([#2095](https://github.com/opensearch-project/neural-search/pull/2095))
* Hold the hybrid query's fused-mode version gate at the release that shipped it, instead of at the version under development, so the guard test stops failing once the branch version moves ahead ([#2101](https://github.com/opensearch-project/neural-search/pull/2101))

### Documentation

### Maintenance

### Refactoring

