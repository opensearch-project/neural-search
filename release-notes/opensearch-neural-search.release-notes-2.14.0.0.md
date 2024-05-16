## Version 2.14.0.0 Release Notes

Compatible with OpenSearch 2.14.0

### Features
* Support k-NN radial search parameters in neural search([#697](https://github.com/opensearch-project/neural-search/pull/697))
### Enhancements
* BWC tests for text chunking processor ([#661](https://github.com/opensearch-project/neural-search/pull/661))
* Add support for request_cache flag in hybrid query ([#663](https://github.com/opensearch-project/neural-search/pull/663))
* Allowing execution of hybrid query on index alias with filters ([#670](https://github.com/opensearch-project/neural-search/pull/670))
* Allowing query by raw tokens in neural_sparse query ([#693](https://github.com/opensearch-project/neural-search/pull/693))
* Removed stream.findFirst implementation to use more native iteration implement to improve hybrid query latencies by 35% ([#706](https://github.com/opensearch-project/neural-search/pull/706))
* Removed map of subquery to subquery index in favor of storing index as part of disi wrapper to improve hybrid query latencies by 20% ([#711](https://github.com/opensearch-project/neural-search/pull/711))
* Avoid change max_chunk_limit exceed exception in text chunking processor ([#717](https://github.com/opensearch-project/neural-search/pull/717))
### Bug Fixes
* Fix async actions are left in neural_sparse query ([#438](https://github.com/opensearch-project/neural-search/pull/438))
* Fix typo for sparse encoding processor factory([#578](https://github.com/opensearch-project/neural-search/pull/578))
* Add non-null check for queryBuilder in NeuralQueryEnricherProcessor ([#615](https://github.com/opensearch-project/neural-search/pull/615))
* Add max_token_score field placeholder in NeuralSparseQueryBuilder to fix the rolling-upgrade from 2.x nodes bwc tests. ([#696](https://github.com/opensearch-project/neural-search/pull/696))
* Fix multi node "no such index" error in text chunking processor. ([#713](https://github.com/opensearch-project/neural-search/pull/713))
### Infrastructure
* Adding integration tests for scenario of hybrid query with aggregations ([#632](https://github.com/opensearch-project/neural-search/pull/632))
### Maintenance
* Update bwc tests for neural_query_enricher neural_sparse search ([#652](https://github.com/opensearch-project/neural-search/pull/652))
