## Version 3.0.0.0-beta1 Release Notes

Compatible with OpenSearch 3.0.0-beta1

### Features
- Lower bound for min-max normalization technique in hybrid query ([#1195](https://github.com/opensearch-project/neural-search/pull/1195))
- Support filter function for HybridQueryBuilder and NeuralQueryBuilder ([#1206](https://github.com/opensearch-project/neural-search/pull/1206))
- Add Z Score normalization technique ([#1224](https://github.com/opensearch-project/neural-search/pull/1224))
- Support semantic sentence highlighter ([#1193](https://github.com/opensearch-project/neural-search/pull/1193))
- Optimize embedding generation in Text Embedding Processor ([#1191](https://github.com/opensearch-project/neural-search/pull/1191))
- Optimize embedding generation in Sparse Encoding Processor ([#1246](https://github.com/opensearch-project/neural-search/pull/1246))
- Optimize embedding generation in Text/Image Embedding Processor ([#1249](https://github.com/opensearch-project/neural-search/pull/1249))
- Inner hits support with hybrid query ([#1253](https://github.com/opensearch-project/neural-search/pull/1253))
- Support custom tags in semantic highlighter ([#1254](https://github.com/opensearch-project/neural-search/pull/1254))
- Add stats API ([#1256](https://github.com/opensearch-project/neural-search/pull/1256))

### Bug Fixes
- Remove validations for unmapped fields (text and image) in TextImageEmbeddingProcessor ([#1230](https://github.com/opensearch-project/neural-search/pull/1230))

### Infrastructure
- [3.0] Update neural-search for OpenSearch 3.0 beta compatibility ([#1245](https://github.com/opensearch-project/neural-search/pull/1245))
