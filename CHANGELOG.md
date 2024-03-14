# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0](https://github.com/opensearch-project/neural-search/compare/2.x...HEAD)
### Features
### Enhancements
### Bug Fixes
- Fix async actions are left in neural_sparse query ([#438](https://github.com/opensearch-project/neural-search/pull/438))
- Fix typo for sparse encoding processor factory([#578](https://github.com/opensearch-project/neural-search/pull/578))
- Add non-null check for queryBuilder in NeuralQueryEnricherProcessor ([#615](https://github.com/opensearch-project/neural-search/pull/615))
### Infrastructure
### Documentation
### Maintenance
### Refactoring

## [Unreleased 2.x](https://github.com/opensearch-project/neural-search/compare/2.12...2.x)
### Features
- Enabled support for applying default modelId in neural sparse query ([#614](https://github.com/opensearch-project/neural-search/pull/614)
### Enhancements
- Adding aggregations in hybrid query ([#630](https://github.com/opensearch-project/neural-search/pull/630))
### Bug Fixes
- Fix runtime exceptions in hybrid query for case when sub-query scorer return TwoPhase iterator that is incompatible with DISI iterator ([#624](https://github.com/opensearch-project/neural-search/pull/624))
### Infrastructure
### Documentation
### Maintenance
### Refactoring
