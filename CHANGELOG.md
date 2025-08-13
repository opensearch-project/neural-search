# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features

### Enhancements

- [Semantic Field] Support the sparse two phase processor for the semantic field.
- [Stats] Add stats for agentic query and agentic query translator processor.
- [Agentic Search] Adds validations and logging for agentic query
- [Semantic Highlighting] Add semantic highlighting response processor with batch inference support ([#1520](https://github.com/opensearch-project/neural-search/pull/1520))

### Bug Fixes

### Infrastructure

- [Unit Test] Enable mocking of final classes and static functions ([#1528](https://github.com/opensearch-project/neural-search/pull/1528)).

### Documentation

### Maintenance

- Remove commons-lang:commons-lang dependency and use gradle version catalog for commons-lang3 ([#1551](https://github.com/opensearch-project/neural-search/pull/1551))

### Refactoring
