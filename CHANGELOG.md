# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features
- Implement analyzer based neural sparse query ([#1088](https://github.com/opensearch-project/neural-search/pull/1088))
- [Semantic Field] Add semantic field mapper. ([#1225](https://github.com/opensearch-project/neural-search/pull/1225)).

### Enhancements
- [Performance Improvement] Add custom bulk scorer for hybrid query (2-3x faster) ([#1289](https://github.com/opensearch-project/neural-search/pull/1289))
- [Stats] Add stats for text chunking processor algorithms ([#1308](https://github.com/opensearch-project/neural-search/pull/1308))

### Bug Fixes
- Add validations to prevent empty input_text_field and input_image_field in TextImageEmbeddingProcessor ([#1257](https://github.com/opensearch-project/neural-search/pull/1257))
- Fix score value as null for single shard when sorting is not done on score field ([#1277](https://github.com/opensearch-project/neural-search/pull/1277))

### Infrastructure

### Documentation

### Maintenance
 - Update Lucene dependencies ([#1336](https://github.com/opensearch-project/neural-search/pull/1336))

### Refactoring
