# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features

### Enhancements
* Improve error messages for misconfigured remote model connectors to provide actionable guidance on post_process_function configuration ([#1825](https://github.com/opensearch-project/neural-search/pull/1825))

### Bug Fixes
* Fix semantic highlighter crash on documents with missing highlighted fields ([#1810](https://github.com/opensearch-project/neural-search/pull/1810))
* [Text Chunking] Fix text chunking processor ignoring index max_token_count setting when ingesting via alias ([#1803](https://github.com/opensearch-project/neural-search/pull/1803))

### Infrastructure
* [GRPC] Add gRPC integration tests for hybrid query with normalization pipeline, sort, and collapse ([#1827](https://github.com/opensearch-project/neural-search/pull/1827))
* Improve CI performance with concurrency groups and parallel builds ([#1828](https://github.com/opensearch-project/neural-search/pull/1828))

### Documentation

### Maintenance

### Refactoring
