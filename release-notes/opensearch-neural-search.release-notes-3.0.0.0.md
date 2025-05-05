## Version 3.0.0.0 Release Notes

Compatible with OpenSearch 3.0.0
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
- Implement analyzer based neural sparse query ([#1088](https://github.com/opensearch-project/neural-search/pull/1088))
- [Semantic Field] Add semantic field mapper. ([#1225](https://github.com/opensearch-project/neural-search/pull/1225))
### Enhancements
- Set neural-search plugin 3.0.0 baseline JDK version to JDK-21 ([#838](https://github.com/opensearch-project/neural-search/pull/838))
- Support different embedding types in model's response ([#1007](https://github.com/opensearch-project/neural-search/pull/1007))
### Bug Fixes
- Fix a bug to unflatten the doc with list of map with multiple entries correctly ([#1204](https://github.com/opensearch-project/neural-search/pull/1204)).
- Remove validations for unmapped fields (text and image) in TextImageEmbeddingProcessor ([#1230](https://github.com/opensearch-project/neural-search/pull/1230))
- Add validations to prevent empty input_text_field and input_image_field in TextImageEmbeddingProcessor ([#1257](https://github.com/opensearch-project/neural-search/pull/1257))
- Fix score value as null for single shard when sorting is not done on score field ([#1277](https://github.com/opensearch-project/neural-search/pull/1277))
### Infrastructure
- [3.0] Update neural-search for OpenSearch 3.0 compatibility ([#1141](https://github.com/opensearch-project/neural-search/pull/1141))
- [3.0] Update neural-search for OpenSearch 3.0 beta compatibility ([#1245](https://github.com/opensearch-project/neural-search/pull/1245))
### Refactoring
- Encapsulate KNNQueryBuilder creation within NeuralKNNQueryBuilder ([#1183](https://github.com/opensearch-project/neural-search/pull/1183))
### Documentation
- Adding code guidelines ([#502](https://github.com/opensearch-project/neural-search/pull/502))