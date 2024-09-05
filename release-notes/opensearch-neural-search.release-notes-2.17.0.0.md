## Version 2.17.0.0 Release Notes

Compatible with OpenSearch 2.17.0

### Enhancements
- Adds rescore parameter support ([#885](https://github.com/opensearch-project/neural-search/pull/885))
### Bug Fixes
- Removing code to cut search results of hybrid search in the priority queue ([#867](https://github.com/opensearch-project/neural-search/pull/867))
- Fixed merge logic in hybrid query for multiple shards case ([#877](https://github.com/opensearch-project/neural-search/pull/877))
### Infrastructure
- Update batch related tests to use batch_size in processor & refactor BWC version check ([#852](https://github.com/opensearch-project/neural-search/pull/852))