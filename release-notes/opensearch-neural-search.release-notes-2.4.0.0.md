## Version 2.4.0.0 Release Notes

Compatible with OpenSearch 2.4.0

### Features

* Add MLCommonsClientAccessor and MLPredict TransportAction for accessing the MLClient's predict API ([#16](https://github.com/opensearch-project/neural-search/pull/16))
* Add parsing logic for neural query ([#15](https://github.com/opensearch-project/neural-search/pull/15))
* Integrate model inference to build neural query ([#20](https://github.com/opensearch-project/neural-search/pull/20))
* Add text embedding processor to neural search ([#18](https://github.com/opensearch-project/neural-search/pull/18))

### Enhancements

* Change text embedding processor to async mode for better isolation ([#27](https://github.com/opensearch-project/neural-search/pull/27))

### Bug Fixes

* Update the model function name from CUSTOM to TEXT_EMBEDDING as per the latest changes in MLCommons ([#17](https://github.com/opensearch-project/neural-search/pull/17))
* Fix the locale changes from locale.default to locale.ROOT to fix the tests failing on Windows ([#43](https://github.com/opensearch-project/neural-search/pull/43))

### Infrastructure

* Initial commit for setting up the neural search plugin ([#2](https://github.com/opensearch-project/neural-search/pull/2))
* Fix CI and Link Checker GitHub workflows ([#3](https://github.com/opensearch-project/neural-search/pull/3))
* Enable the K-NN plugin and ML plugin for integ test cluster ([#6](https://github.com/opensearch-project/neural-search/pull/6))
* Add dependency on k-NN plugin ([#10](https://github.com/opensearch-project/neural-search/pull/10))
* Switch pull_request_target to pull_request in CI ([#26](https://github.com/opensearch-project/neural-search/pull/26))
* Fix minor build.gradle issues ([#28](https://github.com/opensearch-project/neural-search/pull/28))
* Fix group id for ml-commons dependency ([#32](https://github.com/opensearch-project/neural-search/pull/32))
* Add Windows support for CI ([#40](https://github.com/opensearch-project/neural-search/pull/40))
* Add opensearch prefix to plugin name ([#38](https://github.com/opensearch-project/neural-search/pull/38))
* Add integration tests for neural query ([#36](https://github.com/opensearch-project/neural-search/pull/36))
* Switch processor IT to use Lucene ([#48](https://github.com/opensearch-project/neural-search/pull/48))
* Add release note draft automation ([#52](https://github.com/opensearch-project/neural-search/pull/52))

### Documentation

* Add additional maintainers to repo ([#8](https://github.com/opensearch-project/neural-search/pull/8))
* Fix headers in README ([#13](https://github.com/opensearch-project/neural-search/pull/13))

### Maintenance

* Upgrade plugin to 2.4 and refactor zip dependencies ([#25](https://github.com/opensearch-project/neural-search/pull/25))

### Refactoring

* Refactor project package structure ([#55](https://github.com/opensearch-project/neural-search/pull/55))
