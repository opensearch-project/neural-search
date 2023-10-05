## Version 2.11.0.0 Release Notes

Compatible with OpenSearch 2.11.0

### Features
* Support sparse semantic retrieval by introducing `sparse_encoding` ingest processor and query builder ([#333](https://github.com/opensearch-project/neural-search/pull/333))
* Enabled support for applying default modelId in neural search query ([#337](https://github.com/opensearch-project/neural-search/pull/337)
### Bug Fixes
* Fixed exception in Hybrid Query for one shard and multiple node ([#396](https://github.com/opensearch-project/neural-search/pull/396))
### Maintenance
* Consumed latest changes from core, use QueryPhaseSearcherWrapper as parent class for Hybrid QPS ([#356](https://github.com/opensearch-project/neural-search/pull/356))
