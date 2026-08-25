# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x](https://github.com/opensearch-project/neural-search/compare/main...HEAD)

### Features

### Enhancements

### Bug Fixes
* [Hybrid Query] Fix NoSuchElementException in hybrid query with sort/search_after when a shard returns no results ([#1939](https://github.com/opensearch-project/neural-search/pull/1939))
* [SemanticHighlighter] Fix SemanticHighlighterExtBuilder.toXContent ([#1906](https://github.com/opensearch-project/neural-search/issues/1906)) (query-insights [#651](https://github.com/opensearch-project/query-insights/issues/651))
* [Sparse ANN] Fold sparse vector tokens into the signed-short range (modulus 32768) so folded tokens are never sign-extended to a negative value when stored in short[] ([#1926](https://github.com/opensearch-project/neural-search/pull/1926))
* [RRF] Reject a combination technique other than rrf when creating a score-ranker-processor, instead of accepting the pipeline and throwing NullPointerException on every query ([#1949](https://github.com/opensearch-project/neural-search/pull/1949))

### Infrastructure
* Resolve the Eclipse JDT formatter directly from download.eclipse.org instead of the ci.opensearch.org P2 mirror, which was timing out at configuration time and failing CI ([#20826](https://github.com/opensearch-project/OpenSearch/issues/20826))


### Documentation

### Maintenance

### Refactoring
* [SemanticHighlighter] Traverse the query tree with a QueryBuilderVisitor instead of a "manual" walk ([#1915](https://github.com/opensearch-project/neural-search/pull/1915))
* [RRF] Compute rank scores with exact integer arithmetic instead of allocating a BigDecimal per document ([#1942](https://github.com/opensearch-project/neural-search/pull/1942))
