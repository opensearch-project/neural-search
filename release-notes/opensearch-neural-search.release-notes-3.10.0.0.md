## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Features

* In-query fusion for the hybrid query, enabling coordinator-side fusion with normalization and combination techniques directly in the query ([#2036](https://github.com/opensearch-project/neural-search/pull/2036))
* Enable fusion mode by default for hybrid queries via `plugins.neural_search.hybrid.fusion.enabled` setting ([#2037](https://github.com/opensearch-project/neural-search/pull/2037))
* Accept `fusion.weights` for request-level combination weights in fused hybrid queries ([#2065](https://github.com/opensearch-project/neural-search/pull/2065))
* Accept `pagination_depth` in fused hybrid queries as the fusion window size ([#2058](https://github.com/opensearch-project/neural-search/pull/2058))

### Enhancements

* Allow `sort: [_score, <field>]` for hybrid queries with collapse for deterministic group-head selection ([#2010](https://github.com/opensearch-project/neural-search/pull/2010))
* Gate collapse distinct-groups collection behind an opt-in index setting `index.neural_search.hybrid_collapse_distinct_groups_enabled` ([#1956](https://github.com/opensearch-project/neural-search/pull/1956))
* Support lists and scalars with per-element fragments in semantic highlighting ([#2000](https://github.com/opensearch-project/neural-search/pull/2000))
* Expose neural sparse filters to query builder and Lucene visitors, and reject unsupported `method_parameters.filter` on `rank_features` fields explicitly ([#2044](https://github.com/opensearch-project/neural-search/pull/2044))
* Add routing support to the `skip_existing` lookup for inference processors (`text_embedding`, `sparse_encoding`, `text_image_embedding`) ([#2028](https://github.com/opensearch-project/neural-search/pull/2028))
* Reject non-numeric score-combination weights with HTTP 400 instead of HTTP 500 ([#2067](https://github.com/opensearch-project/neural-search/pull/2067))

### Bug Fixes

* Fix hybrid `collapse` sorted by ascending `_score` returning wrong documents and undercounted `hits.total` on shards with more than 4,096 matching documents ([#2026](https://github.com/opensearch-project/neural-search/pull/2026))
* Fix semantic field processor indexing documents without embeddings on inference failure by ensuring the error handler returns immediately ([#2033](https://github.com/opensearch-project/neural-search/pull/2033))
* Fix `ClassCastException` when a point-in-time slice or alias filter excludes a shard from a hybrid query ([#2045](https://github.com/opensearch-project/neural-search/pull/2045))
* Reject sorting a hybrid query by `_score` ascending or by `_score` more than once, which previously returned wrong results silently ([#2046](https://github.com/opensearch-project/neural-search/pull/2046))
* Reject unknown `combination.parameters` keys and unknown keys on inline `fusion` clauses instead of silently ignoring them ([#2069](https://github.com/opensearch-project/neural-search/pull/2069))
* Reject a hybrid query without fusion nested in a hybrid query with fusion, or vice versa, with a clear error ([#2051](https://github.com/opensearch-project/neural-search/pull/2051))
* Resolve fused hybrid legs before fan-out so a sub-query's inference runs once per request instead of twice ([#2074](https://github.com/opensearch-project/neural-search/pull/2074))
* Keep `agent_steps_summary` from the conversational agent's final output for OpenAI native function calling in agentic search ([#2071](https://github.com/opensearch-project/neural-search/pull/2071))
* Pass `_routing` to the semantic field `skip_existing_embedding` lookup so documents with custom routing are found correctly ([#2040](https://github.com/opensearch-project/neural-search/pull/2040))
* Fix `field_map` sibling key overwrite when multiple fields share a dotted prefix, which silently dropped embeddings ([#2052](https://github.com/opensearch-project/neural-search/pull/2052))
* Fix deep-merge of `field_map` target maps so nested siblings at any depth are preserved ([#2054](https://github.com/opensearch-project/neural-search/pull/2054))
* Stop logging customer document content on semantic field processor ingest failure; log only document identifiers ([#2057](https://github.com/opensearch-project/neural-search/pull/2057))
* Fold sparse token IDs at or above 32768 on the native engine so they match the query-side folding ([#2048](https://github.com/opensearch-project/neural-search/pull/2048))

### Infrastructure

* Register BWC test models from `raw.githubusercontent.com` directly, fixing restart-upgrade tests for OpenSearch 2.9–2.11 ([#2070](https://github.com/opensearch-project/neural-search/pull/2070))
* Match k-NN explanation by shape in explain integration tests to accommodate Lucene 10.6 wording changes ([#2079](https://github.com/opensearch-project/neural-search/pull/2079))

### Documentation

* Add changelog entry for field_map sibling key overwrite fix ([#2053](https://github.com/opensearch-project/neural-search/pull/2053))
