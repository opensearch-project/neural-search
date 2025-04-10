# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features
- Lower bound for min-max normalization technique in hybrid query ([#1195](https://github.com/opensearch-project/neural-search/pull/1195))
- Support filter function for HybridQueryBuilder and NeuralQueryBuilder ([#1206](https://github.com/opensearch-project/neural-search/pull/1206))
- Add Z Score normalization technique ([#1224](https://github.com/opensearch-project/neural-search/pull/1224))
- Support semantic sentence highlighter ([#1193](https://github.com/opensearch-project/neural-search/pull/1193))

### Enhancements

### Bug Fixes
- Remove validations for unmapped fields (text and image) in TextImageEmbeddingProcessor ([#1230](https://github.com/opensearch-project/neural-search/pull/1230))

### Infrastructure

### Documentation

### Maintenance

### Refactoring
