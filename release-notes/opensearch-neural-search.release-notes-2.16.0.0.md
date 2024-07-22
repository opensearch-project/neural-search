## Version 2.16.0.0 Release Notes

Compatible with OpenSearch 2.16.0

### Features
- Enable sorting and search_after features in Hybrid Search [#827](https://github.com/opensearch-project/neural-search/pull/827)
### Enhancements
- InferenceProcessor inherits from AbstractBatchingProcessor to support sub batching in processor [#820](https://github.com/opensearch-project/neural-search/pull/820)
- Adds dynamic knn query parameters efsearch and nprobes [#814](https://github.com/opensearch-project/neural-search/pull/814/)
- Enable '.' for nested field in text embedding processor ([#811](https://github.com/opensearch-project/neural-search/pull/811))
- Enhance syntax for nested mapping in destination fields([#841](https://github.com/opensearch-project/neural-search/pull/841))
### Bug Fixes
- Fix function names and comments in the gradle file for BWC tests ([#795](https://github.com/opensearch-project/neural-search/pull/795/files))
- Fix for missing HybridQuery results when concurrent segment search is enabled ([#800](https://github.com/opensearch-project/neural-search/pull/800))
### Infrastructure
- Add BWC for batch ingestion ([#769](https://github.com/opensearch-project/neural-search/pull/769))
- Add backward test cases for neural sparse two phase processor ([#777](https://github.com/opensearch-project/neural-search/pull/777))
- Fix CI for JDK upgrade towards 21 ([#835](https://github.com/opensearch-project/neural-search/pull/835))
- Maven publishing workflow by upgrade jdk to 21 ([#837](https://github.com/opensearch-project/neural-search/pull/837))