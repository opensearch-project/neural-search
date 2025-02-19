- [Developer Guide](#developer-guide)
  - [Getting Started](#getting-started)
    - [Fork OpenSearch neural-search Repo](#fork-opensearch-neural-search-repo)
    - [Install Prerequisites](#install-prerequisites)
      - [JDK 21](#jdk-21)
      - [Environment](#Environment)
  - [Use an Editor](#use-an-editor)
    - [IntelliJ IDEA](#intellij-idea)
  - [Build](#build)
  - [Run OpenSearch neural-search](#run-opensearch-neural-search)
    - [Run Single-node Cluster Locally](#run-single-node-cluster-locally)
    - [Run Multi-node Cluster Locally](#run-multi-node-cluster-locally)
  - [Debugging](#debugging)
    - [Major Dependencies](#major-dependencies)
  - [Backwards Compatibility Testing](#backwards-compatibility-testing)
    - [Adding new tests](#adding-new-tests)
  - [Supported configurations](#supported-configurations)
  - [Submitting Changes](#submitting-changes)
  - [Building On Lucene Version Updates](#building-on-lucene-version-updates)
  - [Code Guidelines](#code-guidelines)
    - [Class and package names](#class-and-package-names)
    - [Modular code](#modular-code)
    - [Documentation](#documentation)
    - [Code style](#code-style)
    - [Style and Formatting Check](#style-and-formatting-check)
    - [Tests](#tests)
    - [Outdated or irrelevant code](#outdated-or-irrelevant-code)

# Developer Guide

So you want to contribute code to OpenSearch neural-search? Excellent! We're glad you're here. Here's what you need to do.

## Getting Started

### Fork OpenSearch neural-search Repo

Fork [opensearch-project/OpenSearch neural-search](https://github.com/opensearch-project/neural-search) and clone locally.

Example:
```
git clone https://github.com/[your username]/neural-search.git
```

### Install Prerequisites

#### JDK 21

OpenSearch builds using Java 21 at a minimum. This means you must have a JDK 21 installed with the environment variable
`JAVA_HOME` referencing the path to Java home for your JDK 21 installation, e.g. `JAVA_HOME=/usr/lib/jvm/jdk-21`.

One easy way to get Java 21 on *nix is to use [sdkman](https://sdkman.io/).

```bash
curl -s "https://get.sdkman.io" | bash
source ~/.sdkman/bin/sdkman-init.sh
sdk install java 21.0.2-open
sdk use java 21.0.2-open
```

JDK versions 14 and 17 were tested and are fully supported for local development.

## Use an Editor

### IntelliJ IDEA

When importing into IntelliJ you will need to define an appropriate JDK. The convention is that **this SDK should be named "21"**, and the project import will detect it automatically. For more details on defining an SDK in IntelliJ please refer to [this documentation](https://www.jetbrains.com/help/idea/sdk.html#define-sdk). Note that SDK definitions are global, so you can add the JDK from any project, or after project import. Importing with a missing JDK will still work, IntelliJ will report a problem and will refuse to build until resolved.

You can import the OpenSearch project into IntelliJ IDEA as follows.

1. Select **File > Open**
2. In the subsequent dialog navigate to the root `build.gradle` file
3. In the subsequent dialog select **Open as Project**

## Java Language Formatting Guidelines

Taken from [OpenSearch's guidelines](https://github.com/opensearch-project/OpenSearch/blob/main/DEVELOPER_GUIDE.md):

Java files in the OpenSearch codebase are formatted with the Eclipse JDT formatter, using the [Spotless Gradle](https://github.com/diffplug/spotless/tree/master/plugin-gradle) plugin. The formatting check can be run explicitly with:

    ./gradlew spotlessJavaCheck

The code can be formatted with:

    ./gradlew spotlessApply

Please follow these formatting guidelines:

* Java indent is 4 spaces
* Line width is 140 characters
* Lines of code surrounded by `// tag::NAME` and `// end::NAME` comments are included in the documentation and should only be 76 characters wide not counting leading indentation. Such regions of code are not formatted automatically as it is not possible to change the line length rule of the formatter for part of a file. Please format such sections sympathetically with the rest of the code, while keeping lines to maximum length of 76 characters.
* Wildcard imports (`import foo.bar.baz.*`) are forbidden and will cause the build to fail.
* If *absolutely* necessary, you can disable formatting for regions of code with the `// tag::NAME` and `// end::NAME` directives, but note that these are intended for use in documentation, so please make it clear what you have done, and only do this where the benefit clearly outweighs the decrease in consistency.
* Note that JavaDoc and block comments i.e. `/* ... */` are not formatted, but line comments i.e `// ...` are.
* There is an implicit rule that negative boolean expressions should use the form `foo == false` instead of `!foo` for better readability of the code. While this isn't strictly enforced, it might get called out in PR reviews as something to change.

## Build

OpenSearch neural-search uses a [Gradle](https://docs.gradle.org/6.6.1/userguide/userguide.html) wrapper for its build.
Run `gradlew` on Unix systems.

Build OpenSearch neural-search using `gradlew build`. This command will
also run Integration Tests and Unit Tests.

```
./gradlew build
```

## Run Unit Tests
If you want to strictly test that your unit tests are passing
you can run the following.

```
./gradlew test
```


## Run OpenSearch neural-search

### Run Single-node Cluster Locally
Run OpenSearch neural-search using `gradlew run`.

```shell script
./gradlew run
```
That will build OpenSearch and start it, writing its log above Gradle's status message. We log a lot of stuff on startup, specifically these lines tell you that plugin is ready.
```
[2023-10-24T16:26:24,789][INFO ][o.o.h.AbstractHttpServerTransport] [integTest-0] publish_address {127.0.0.1:9200}, bound_addresses {[::1]:9200}, {127.0.0.1:9200}
[2023-10-24T16:26:24,793][INFO ][o.o.n.Node               ] [integTest-0] started
```

It's typically easier to wait until the console stops scrolling, and then run `curl` in another window to check if OpenSearch instance is running.

```bash
curl localhost:9200

{
  "name" : "integTest-0",
  "cluster_name" : "integTest",
  "cluster_uuid" : "XSMfCO3FR8CBzRnhG1AC7w",
  "version" : {
    "distribution" : "opensearch",
    "number" : "3.0.0-SNAPSHOT",
    "build_type" : "tar",
    "build_hash" : "5bd413c588f48589c6fd6c4de4e87550271aecf8",
    "build_date" : "2023-10-24T18:06:58.612820Z",
    "build_snapshot" : true,
    "lucene_version" : "9.8.0",
    "minimum_wire_compatibility_version" : "2.12.0",
    "minimum_index_compatibility_version" : "2.0.0"
  },
  "tagline" : "The OpenSearch Project: https://opensearch.org/"
}
```

Additionally, it is also possible to run a cluster with security enabled:
```shell script
./gradlew run -Dsecurity.enabled=true
```

By default, if `-Dsecurity.enabled=true` is passed the following defaults will be used: `https=true`, `user=admin`. There is no default password and it is to be set as `password=<admin-password>`.

Then, to connect to the cluster, use the following command. Remember to replace `<admin-password>` with the password you chose when setting up the admin user.
```bash
curl https://localhost:9200 --insecure -u admin:<admin-password>

{
  "name" : "integTest-0",
  "cluster_name" : "integTest",
  "cluster_uuid" : "kLsNk4JDTMyp1yQRqog-3g",
  "version" : {
    "distribution" : "opensearch",
    "number" : "3.0.0-SNAPSHOT",
    "build_type" : "tar",
    "build_hash" : "9d85e566894ef53e5f2093618b3d455e4d0a04ce",
    "build_date" : "2023-10-30T18:34:06.996519Z",
    "build_snapshot" : true,
    "lucene_version" : "9.8.0",
    "minimum_wire_compatibility_version" : "2.12.0",
    "minimum_index_compatibility_version" : "2.0.0"
  },
  "tagline" : "The OpenSearch Project: https://opensearch.org/"
}
```

### Run Multi-node Cluster Locally

It can be useful to test and debug on a multi-node cluster. In order to launch a 3 node cluster with the neural-search plugin installed, run the following command:

```
./gradlew run -PnumNodes=3
```

In order to run the integration tests with a 3 node cluster, run this command:

```
./gradlew :integTest -PnumNodes=3
```

Additionally, to run integration tests on multi nodes with security enabled, run
```
./gradlew :integTest -Dsecurity.enabled=true -PnumNodes=3
```

Some integration tests are skipped by default, mainly to save time and resources. A special parameter is required to include those tests in the executed test suite. For example, the following command enables additional tests for aggregations when they are bundled with hybrid queries
```
./gradlew :integTest -PnumNodes=3 -Dtest_aggs=true
```

Integration tests can be run with remote cluster. For that run the following command and replace host/port/cluster name values with ones for the target cluster:

```
./gradlew :integTestRemote -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername="integTest-0" -Dhttps=false -PnumNodes=1
```

In case remote cluster is secured it's possible to pass username and password with the following command:

```
./gradlew :integTestRemote -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername="integTest-0" -Dhttps=true -Duser=admin -Dpassword=<admin-password>
```

### Debugging

Sometimes it is useful to attach a debugger to either the OpenSearch cluster or the integration test runner to see what's going on. For running unit tests, hit **Debug** from the IDE's gutter to debug the tests. For the OpenSearch cluster, first, make sure that the debugger is listening on port `5005`. Then, to debug the cluster code, run:

```
./gradlew :integTest -Dcluster.debug=1 # to start a cluster with debugger and run integ tests
```

OR

```
./gradlew run --debug-jvm # to just start a cluster that can be debugged
```

The OpenSearch server JVM will connect to a debugger attached to `localhost:5005` before starting. If there are multiple nodes, the servers will connect to debuggers listening on ports `5005, 5006, ...`

To debug code running in an integration test (which exercises the server from a separate JVM), first, setup a remote debugger listening on port `8000`, and then run:

```
./gradlew :integTest -Dtest.debug=1
```

The test runner JVM will connect to a debugger attached to `localhost:8000` before running the tests.

Additionally, it is possible to attach one debugger to the cluster JVM and another debugger to the test runner. First, make sure one debugger is listening on port `5005` and the other is listening on port `8000`. Then, run:
```
./gradlew :integTest -Dtest.debug=1 -Dcluster.debug=1
```

#### Major Dependencies
Currently, the major dependencies that Neural Search depends on are [ML-Commons](https://github.com/opensearch-project/ml-commons) and [K-NN](https://github.com/opensearch-project/k-NN).
Make sure to check on them when you observe a failure that affects Neural Search.
See [Building on Lucene Version updates](#building-on-lucene-version-updates) as an example where K-NN caused a build failure.
Also, please note that it may take time for developers to create a fix for your current dependency issue.

## Backwards Compatibility Testing

The purpose of Backwards Compatibility Testing and different types of BWC tests are explained [here](https://github.com/opensearch-project/opensearch-plugins/blob/main/TESTING.md#backwards-compatibility-testing). The BWC tests (i.e. Restart-Upgrade, Mixed-Cluster and Rolling-Upgrade scenarios) should be added with any new feature being added to Neural Search.
The current design has mixed-cluster tests combined with rolling-upgrade tests in the same test class for [example](https://github.com/opensearch-project/neural-search/blob/main/qa/rolling-upgrade/src/test/java/org/opensearch/neuralsearch/bwc/SemanticSearchIT.java).

Use these commands to run BWC tests for neural search:
1. Rolling upgrade tests: `./gradlew :qa:rolling-upgrade:testRollingUpgrade`
2. Full restart upgrade tests: `./gradlew :qa:restart-upgrade:testAgainstNewCluster`
3. `./gradlew :qa:bwcTestSuite` is used to run all the above bwc tests together.

bwc.version stands for the older version of OpenSearch against which one needs to check the compatibility with the current version.
The details regarding all bwc versions of OpenSearch can be found [here](https://github.com/opensearch-project/OpenSearch/blob/main/libs/core/src/main/java/org/opensearch/Version.java).
Use this command to run BWC tests for a given Backwards Compatibility Version:
```
./gradlew :qa:bwcTestSuite -Dbwc.version=2.9.0
```
Here, we are testing BWC Tests with BWC version of plugin as 2.9.0.
The tests will not run on MAC OS due to issues coming from the OS.

### Adding new tests

Before adding any new tests to Backward Compatibility Tests, we should be aware that the tests in BWC are not independent. While creating an index, a test cannot use the same index name if it is already used in other tests.

### Supported configurations

By default, neural-search plugin supports `lucene` k-NN engine for local runs. Below is the sample request for creating of new index using this engine:

```
{
    "settings": {
        "index.knn": true,
        "default_pipeline": "nlp-pipeline",
        "number_of_shards": 2
    },
    "mappings": {
        "properties": {
            "passage_embedding": {
                "type": "knn_vector",
                "dimension": 768,
                "method": {
                    "name": "hnsw",
                    "engine": "lucene"
                }
            },
            "passage_text": {
                "type": "text"
            }
        }
    }
}
```

The reason for this is that neural-search uses k-NN zip artifact from maven, and that artifact doesn't have native libraries. It's possible to install k-NN locally and build those native libraries (like Nmslib). For instructions on how to do this please check dev guide in [k-NN plugin repository.](https://github.com/opensearch-project/k-NN)

## Submitting Changes

See [CONTRIBUTING](CONTRIBUTING.md).

## Backports

The Github workflow in [`backport.yml`](.github/workflows/backport.yml) creates backport PRs automatically when the
original PR with an appropriate label `backport <backport-branch-name>` is merged to main with the backport workflow
run successfully on the PR. For example, if a PR on main needs to be backported to `2.x` branch, add a label
`backport 2.x` to the PR and make sure the backport workflow runs on the PR along with other checks. Once this PR is
merged to main, the workflow will create a backport PR to the `2.x` branch.

## Building On Lucene Version Updates
There may be a Lucene version update that can affect your workflow causing errors like
`java.lang.NoClassDefFoundError: org/apache/lucene/codecs/lucene99/Lucene99Codec` or
`Provider org.opensearch.knn.index.codec.KNN910Codec.KNN910Codec could not be instantiated`. In this case
we can observe there may be an issue with a dependency with [K-NN](https://github.com/opensearch-project/k-NN).
This results in having issues with not being able to do `./gradlew run` or `./gradlew build`.

You can check this [K-NN PR](https://github.com/opensearch-project/k-NN/pull/2195) as an example of this event happening or this [Neural Search PR](https://github.com/opensearch-project/neural-search/pull/913#issuecomment-2400189329) that shows a developer going
through the same build issue.

**Follow the steps to remedy the gradle run issue.**
1. From your cloned neural search repo root directory `rm -rf build .gradle`
2. Clear the following directories from your gradle folder located in your root directory
   1. `cd ~/.gradle`
   2. `rm -rf caches workers wrapper daemon`
   3. `cd -` switch back the previous directory (i.e. the neural search repo root directory)
3. Finally run `./gradlew run`

**Follow the steps to remedy the gradle build issue**

**PREREQ:** Make sure you have OpenSearch repo cloned locally

1. From your cloned neural search repo root directory `rm -rf build .gradle`
2. Delete the .gradle folder and .m2 folder. `rm -rf ~/.gradle ~/.m2`
3. Head over to your OpenSearch cloned repo root directory
   1. `./gradlew publisToMavenLocal`
4. Finally run `./gradlew build` from the neural search repo

## Code Guidelines

### Class and package names

Class names should use `CamelCase`.

Try to put new classes into existing packages if package name abstracts the purpose of the class.

Example of good class file name and package utilization:

`src/main/java/org/opensearch/neuralsearch/processor/factory/RerankProcessorFactory.java`

following naming needs improvement, it creates unnecessary package and uses underscores case for file name

`src/main/java/org/opensearch/neuralsearch/rerank_factory/rerank_processor_factory.java`

### Modular code

Try to organize code into small classes and methods with a single concise purpose. It's preferable to have multiple small
methods rather than a long single one and does everything.

### Documentation

Document you code. That includes purpose of new classes, every public method and code sections that have critical or non-trivial
logic (check this example https://github.com/opensearch-project/neural-search/blob/main/src/main/java/org/opensearch/neuralsearch/query/NeuralQueryBuilder.java#L238).

When you submit a feature PR, please submit a new
[documentation issue](https://github.com/opensearch-project/documentation-website/issues/new/choose). This is a path for the documentation to be published as part of https://opensearch.org/docs/latest/ documentation site.

Please be prepared to provide any additional guidance (like example of query request/response, details of API parameters etc.) for the team doing the documentation.

### Code style

For the most part, we're using common conventions for Java projects. Here are a few things to keep in mind.

1. Use descriptive names for classes, methods, fields, and variables.
2. Avoid abbreviations unless they are widely accepted
3. Use `final` on all method arguments unless it's absolutely necessary
4. Wildcard imports are not allowed.
5. Static imports are preferred over qualified imports when using static methods
6. Prefer creating non-static public methods whenever possible. Avoid static methods in general, as they can often serve as shortcuts.
Static methods are acceptable if they are private and do not access class state.
7. Use functional programming style inside methods unless it's a performance critical section.
8. For parameters of lambda expression please use meaningful names instead of shorten cryptic ones.
9. Use Optional for return values if the value may not be present. This should be preferred to returning null.
10. Do not create checked exceptions, and do not throw checked exceptions from public methods whenever possible. In general, if you call a method with a checked exception, you should wrap that exception into an unchecked exception.
11. Throwing checked exceptions from private methods is acceptable.
12. Use String.format when a string includes parameters, and prefer this over direct string concatenation. Always specify a Locale with String.format;
as a rule of thumb, use Locale.ROOT.
13. Prefer Lombok annotations to the manually written boilerplate code
14. When throwing an exception, avoid including user-provided content in the exception message. For secure coding practices,
limit exception messages to the bare minimum and log additional details to the application logs, as user input could be maliciously crafted.

Please check [Java Language Formatting Guidelines](##Java Language Formatting Guidelines) section for more details.

### Style and Formatting Check
As part of our continuous integration (CI) pipeline, we check code formatting and style. Failed checks may block your PR from being merged. To avoid this, run the following command locally before submitting your PR:

```bash
./gradlew spotlessApply
```

To verify that all required public Java documentation is present, run:

```bash
./gradlew javadoc
```

This command will identify missing or incomplete documentation. Common warnings include:

- missing documentation for public methods
- missing parameter descriptions
- missing documentation for public fields

Example warning:

```
/neural-search/build/generated/sources/delombok/java/main/org/opensearch/neuralsearch/processor/combination/ArithmeticMeanScoreCombinationTechnique.java:18: warning: no comment
public static final String TECHNIQUE_NAME = "arithmetic_mean";
```
Documentation requirements:

- all public methods must have descriptions that explain:
    - main responsibility of the method
    - any side effects (like mutating input objects)
    - any exceptional conditions or error cases
- all parameters in public methods must have descriptions
- public fields should be documented with their purpose and usage

Fix any new warnings before submitting your PR to ensure proper code documentation.

### Tests

Write unit and integration tests for your new functionality.

Unit tests are preferred as they are cheap and fast, try to use them to cover all possible
combinations of parameters. Utilize mocks to mimic dependencies.

Integration tests should be used sparingly, focusing primarily on the main (happy path) scenario or cases where extensive
mocking is impractical. Include one or two unhappy paths to confirm that correct response codes are returned to the user.
Whenever possible, favor scenarios that do not require model deployment. If model deployment is necessary, use an existing
model, as tests involving new model deployments are the most resource-intensive.

If your changes could affect backward compatibility, please include relevant backward compatibility tests along with your
PR. For guidance on adding these tests, refer to the [Backwards Compatibility Testing](#backwards-compatibility-testing) section in this guide.

### Outdated or irrelevant code

Do not submit code that is not used or needed, even if it's commented. We rely on github as version control system, code
can be restored if needed.
