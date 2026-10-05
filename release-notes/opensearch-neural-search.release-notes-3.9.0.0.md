## Version 3.9.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.9.0

### Features
* Add `model_selection` parameter to semantic field to resolve model ID from operator-configured cluster settings instead of requiring an explicit `model_id` ([#1919](https://github.com/opensearch-project/neural-search/pull/1919))
* Add JNI bridge layer for the native sparse ANN engine (neural-sparse-cpp) with CMake/Gradle build wiring ([#1972](https://github.com/opensearch-project/neural-search/pull/1972))
* Wire the native sparse engine into the plugin with codec, query, mapper, and feature-flag support ([#1974](https://github.com/opensearch-project/neural-search/pull/1974))
* Ship native sparse engine SIMD variants and OpenMP runtime in the distribution build with CPU feature detection ([#1978](https://github.com/opensearch-project/neural-search/pull/1978))
* Report sparse vector field adoption counts (indices, fields, native vs. lucene engine) as neural info stats ([#1996](https://github.com/opensearch-project/neural-search/pull/1996))

### Enhancements
* Add conversion from `heap_factor` query parameter to native engine `k_prime` block budget for disk-based sparse indexes ([#1997](https://github.com/opensearch-project/neural-search/pull/1997))
* Add support for 64-bit CSR forward-index offsets in the native sparse engine, allowing single segments with more than 2.1 billion non-zeros ([#2001](https://github.com/opensearch-project/neural-search/pull/2001))
* Expose neural query embedded filters to `QueryBuilderVisitor` traversal for cross-plugin compatibility ([#1992](https://github.com/opensearch-project/neural-search/pull/1992))
* Reject unsupported combination techniques (e.g., `arithmetic_mean`) in the score-ranker-processor at pipeline creation time instead of failing with a `NullPointerException` at search time ([#1949](https://github.com/opensearch-project/neural-search/pull/1949))

### Bug Fixes
* Fix `SemanticHighlighterExtBuilder.toXContent` to produce the enclosing field name expected by `SearchExtBuilder` ([#1907](https://github.com/opensearch-project/neural-search/pull/1907))
* Fix `NoSuchElementException` in hybrid query when using `search_after` with `sort` and a shard returns no results ([#1939](https://github.com/opensearch-project/neural-search/pull/1939))
* Fix sparse vector token IDs folding into the negative signed-short range, which caused `NegativeArraySizeException` and incorrect scoring ([#1926](https://github.com/opensearch-project/neural-search/pull/1926))
* Skip sparse cache cleanup for closed index shards to prevent reopened indexes from becoming permanently unassigned ([#1983](https://github.com/opensearch-project/neural-search/pull/1983))
* Fix `HybridQueryScorer` reading stale document and sub-match state from a frozen priority queue, which caused wrong per-sub-query scores under profiling, aggregation filters, nested queries, and document-level security ([#1950](https://github.com/opensearch-project/neural-search/pull/1950))

### Infrastructure
* Add end-to-end remote dense model integration test for semantic field mapping transformer using TorchServe Docker mock ([#1966](https://github.com/opensearch-project/neural-search/pull/1966))
* Add cross-plugin integration tests for hybrid queries with document-level security ([#1957](https://github.com/opensearch-project/neural-search/pull/1957))
* Pin two-phase processor integration test index to a single shard to fix flaky assertion under non-default shard counts ([#1959](https://github.com/opensearch-project/neural-search/pull/1959))

### Refactoring
* Compute RRF rank scores with exact integer arithmetic, eliminating per-document `BigDecimal` allocations with bit-identical results ([#1942](https://github.com/opensearch-project/neural-search/pull/1942))
* Use `QueryBuilderVisitor` in `HighlightConfigResolver` instead of manually walking the query tree ([#1915](https://github.com/opensearch-project/neural-search/pull/1915))
* Bump neural-sparse-cpp submodule for the DiskSeismic mmap madvise fix ([#2005](https://github.com/opensearch-project/neural-search/pull/2005))
