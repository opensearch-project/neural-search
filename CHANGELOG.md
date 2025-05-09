# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features
- Implement analyzer based neural sparse query ([#1088](https://github.com/opensearch-project/neural-search/pull/1088) [#1279](https://github.com/opensearch-project/neural-search/pull/1279))
- [Semantic Field] Add semantic mapping transformer. ([#1276](https://github.com/opensearch-project/neural-search/pull/1276))

### Enhancements
- Add stats for text chunking processor algorithms ([#1308](https://github.com/opensearch-project/neural-search/pull/1308))

### Bug Fixes
- Fix score value as null for single shard when sorting is not done on score field ([#1277](https://github.com/opensearch-project/neural-search/pull/1277))
- Return bad request for stats API calls with invalid stat names instead of ignoring them ([#1291](https://github.com/opensearch-project/neural-search/pull/1291))


### Infrastructure

### Documentation

### Maintenance

### Refactoring
