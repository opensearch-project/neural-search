## Version 3.0.0.0-alpha1 Release Notes

Compatible with OpenSearch 3.0.0-alpha1

### Enhancements
- Set neural-search plugin 3.0.0 baseline JDK version to JDK-21 ([#838](https://github.com/opensearch-project/neural-search/pull/838))
- Support different embedding types in model's response ([#1007](https://github.com/opensearch-project/neural-search/pull/1007))
### Bug Fixes
- Fix a bug to unflatten the doc with list of map with multiple entries correctly ([#1204](https://github.com/opensearch-project/neural-search/pull/1204)).
### Infrastructure
- [3.0] Update neural-search for OpenSearch 3.0 compatibility ([#1141](https://github.com/opensearch-project/neural-search/pull/1141))
### Refactoring
- Encapsulate KNNQueryBuilder creation within NeuralKNNQueryBuilder ([#1183](https://github.com/opensearch-project/neural-search/pull/1183))
### Documentation
- Adding code guidelines ([#502](https://github.com/opensearch-project/neural-search/pull/502))