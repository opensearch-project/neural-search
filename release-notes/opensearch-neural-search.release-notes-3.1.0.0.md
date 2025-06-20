## Version 3.1.0.0 Release Notes

Compatible with OpenSearch 3.1.0

### Features
- Implement analyzer based neural sparse query ([#1088](https://github.com/opensearch-project/neural-search/pull/1088) [#1279](https://github.com/opensearch-project/neural-search/pull/1279))
- [Semantic Field] Add semantic mapping transformer. ([#1276](https://github.com/opensearch-project/neural-search/pull/1276))
- [Semantic Field] Add semantic ingest processor. ([#1309](https://github.com/opensearch-project/neural-search/pull/1309))
- [Semantic Field] Implement the query logic for the semantic field. ([#1315](https://github.com/opensearch-project/neural-search/pull/1315))
- [Semantic Field] Enhance semantic field to allow to enable/disable chunking. ([#1337](https://github.com/opensearch-project/neural-search/pull/1337))
- [Semantic Field] Implement the search analyzer support for semantic field at query time. ([#1341](https://github.com/opensearch-project/neural-search/pull/1341))
- Add `FixedCharLengthChunker` for character length-based chunking ([#1342](https://github.com/opensearch-project/neural-search/pull/1342))
- [Semantic Field] Implement the search analyzer support for semantic field at semantic field index creation time. ([#1367](https://github.com/opensearch-project/neural-search/pull/1367))
- [Hybrid] Add collapse functionality to hybrid query ([#1345](https://github.com/opensearch-project/neural-search/pull/1345))

### Enhancements
- [Performance Improvement] Add custom bulk scorer for hybrid query (2-3x faster) ([#1289](https://github.com/opensearch-project/neural-search/pull/1289))
- [Stats] Add stats for text chunking processor algorithms ([#1308](https://github.com/opensearch-project/neural-search/pull/1308))
- Support custom weights in RRF normalization processor ([#1322](https://github.com/opensearch-project/neural-search/pull/1322))
- [Stats] Add stats tracking for semantic highlighting ([#1327](https://github.com/opensearch-project/neural-search/pull/1327))
- [Stats] Add stats for text embedding processor with different settings ([#1332](https://github.com/opensearch-project/neural-search/pull/1332))
- Validate model id and analyzer should not be provided at the same time for the neural sparse query ([#1359](https://github.com/opensearch-project/neural-search/pull/1359))
- [Stats] Add stats for score based and rank based normalization processors ([#1326](https://github.com/opensearch-project/neural-search/pull/1326))
- [Stats] Add stats tracking for semantic field ([#1362](https://github.com/opensearch-project/neural-search/pull/1362))
- [Stats] Add stats for neural query enricher, neural sparse encoding, two phase, and reranker processors ([#1343](https://github.com/opensearch-project/neural-search/pull/1343))
- [Stats] Add `include_individual_nodes`, `include_all_nodes`, `include_info` parameters to stats API ([#1360](https://github.com/opensearch-project/neural-search/pull/1360))
- [Stats] Add stats for custom flags set in ingest processors ([#1378](https://github.com/opensearch-project/neural-search/pull/1378))

### Bug Fixes
- Fix score value as null for single shard when sorting is not done on score field ([#1277](https://github.com/opensearch-project/neural-search/pull/1277))
- Return bad request for stats API calls with invalid stat names instead of ignoring them ([#1291](https://github.com/opensearch-project/neural-search/pull/1291))
- Add validation for invalid nested hybrid query ([#1305](https://github.com/opensearch-project/neural-search/pull/1305))
- Use stack to collect semantic fields to avoid stack overflow ([#1357](https://github.com/opensearch-project/neural-search/pull/1357))
- Filter requested stats based on minimum cluster version to fix BWC tests for stats API ([#1373](https://github.com/opensearch-project/neural-search/pull/1373))
- Fix neural radial search serialization in multi-node clusters ([#1393](https://github.com/opensearch-project/neural-search/pull/1393)))

### Infrastructure
- [3.0] Update neural-search for OpenSearch 3.0 beta compatibility ([#1245](https://github.com/opensearch-project/neural-search/pull/1245))

### Maintenance
- Update Lucene dependencies ([#1336](https://github.com/opensearch-project/neural-search/pull/1336))