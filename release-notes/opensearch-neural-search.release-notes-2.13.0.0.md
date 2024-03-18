## Version 2.13.0.0 Release Notes

Compatible with OpenSearch 2.13.0

### Features
- Implement document chunking processor with fixed token length and delimiter algorithm ([#607](https://github.com/opensearch-project/neural-search/pull/607/))
- Enabled support for applying default modelId in neural sparse query ([#614](https://github.com/opensearch-project/neural-search/pull/614)
### Enhancements
- Adding aggregations in hybrid query ([#630](https://github.com/opensearch-project/neural-search/pull/630))
- Support for post filter in hybrid query ([#633](https://github.com/opensearch-project/neural-search/pull/633))
### Bug Fixes
- Fix runtime exceptions in hybrid query for case when sub-query scorer return TwoPhase iterator that is incompatible with DISI iterator ([#624](https://github.com/opensearch-project/neural-search/pull/624))