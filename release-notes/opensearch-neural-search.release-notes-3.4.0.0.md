## Version 3.4.0 Release Notes

Compatible with OpenSearch 3.4.0 and OpenSearch Dashboards 3.4.0

### Enhancements
- [Agentic Search] Preserve source parameter for the query ([#1669](https://github.com/opensearch-project/neural-search/pull/1669))
- [SEISMIC Nested Field]: Sparse ANN ingestion and query handle nested fields ([#1678](https://github.com/opensearch-project/neural-search/pull/1678))

### Bug Fixes
- [SEISMIC IT]: Fix some failed IT cases ([#1655](https://github.com/opensearch-project/neural-search/pull/1655))
- [SEISMIC Query]: Sparse ANN query handle non-specified method_parameters ([#1674](https://github.com/opensearch-project/neural-search/pull/1674))
- Revert change in ([#1086](https://github.com/opensearch-project/neural-search/pull/1086)) to add support for empty string ([#1668](https://github.com/opensearch-project/neural-search/pull/1668))

### Infrastructure
- Onboard to s3 snapshots ([#1618](https://github.com/opensearch-project/neural-search/pull/1618))
- Add BWC tests for Sparse ANN Seismic feature ([#1657](https://github.com/opensearch-project/neural-search/pull/1657))
- Add role assignment multi-node integ testing in CI ([#1663](https://github.com/opensearch-project/neural-search/pull/1663))
- Upgrade codecov-action version to v5 and fix codecov rate limit issue ([#1676](https://github.com/opensearch-project/neural-search/pull/1676))

### Maintenance
- Update to Gradle 9.2 and run CI checks with JDK 25 ([#1667](https://github.com/opensearch-project/neural-search/pull/1667))
