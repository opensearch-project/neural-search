## Version 2.10.0.0 Release Notes

Compatible with OpenSearch 2.10.0

### Features
* Improved Hybrid Search relevancy by Score Normalization and Combination ([#241](https://github.com/opensearch-project/neural-search/pull/241/))
* Support sparse semantic retrieval by introducing `sparse_encoding` ingest processor and query builder ([#333](https://github.com/opensearch-project/neural-search/pull/333))

### Enhancements
* Changed format for hybrid query results to a single list of scores with delimiter ([#259](https://github.com/opensearch-project/neural-search/pull/259))
* Added validations for score combination weights in Hybrid Search ([#265](https://github.com/opensearch-project/neural-search/pull/265))
* Made hybrid search active by default ([#274](https://github.com/opensearch-project/neural-search/pull/274))
