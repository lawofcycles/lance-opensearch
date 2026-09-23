- [Developer Guide](#developer-guide)
  - [Getting Started](#getting-started)
    - [Fork the repository](#fork-the-repository)
    - [Install prerequisites](#install-prerequisites)
      - [JDK 21](#jdk-21)
      - [No native toolchain](#no-native-toolchain)
  - [Use an editor](#use-an-editor)
  - [Java formatting](#java-formatting)
  - [Build](#build)
    - [Building against another OpenSearch version](#building-against-another-opensearch-version)
  - [Run the plugin in a local cluster](#run-the-plugin-in-a-local-cluster)
  - [Run the tests](#run-the-tests)
    - [Unit tests](#unit-tests)
    - [Integration tests](#integration-tests)
    - [Running one test class](#running-one-test-class)
    - [Precommit checks](#precommit-checks)
    - [Dependencies, audit lists and the security policy](#dependencies-audit-lists-and-the-security-policy)
  - [Debugging](#debugging)
  - [Repository layout](#repository-layout)
  - [Submitting changes](#submitting-changes)

# Developer Guide

So you want to contribute code to OpenSearch Lance? Excellent! We're glad you're here. Here's what you need to do.

## Getting Started

### Fork the repository

Fork [lawofcycles/lance-opensearch](https://github.com/lawofcycles/lance-opensearch) and clone locally.

Example:
```
git clone https://github.com/[your username]/lance-opensearch.git
```

### Install prerequisites

#### JDK 21

The plugin builds with Java 21 (`java_release_version=21` in `gradle.properties`; OpenSearch 3.x itself requires 21). Install a JDK 21 such as [Eclipse Temurin](https://adoptium.net/) or [Amazon Corretto](https://aws.amazon.com/corretto/) and point the environment variable `JAVA_HOME` at it, e.g. `JAVA_HOME=/usr/lib/jvm/jdk-21`.

One easy way to get Java 21 on *nix is to use [sdkman](https://sdkman.io/).

```bash
curl -s "https://get.sdkman.io" | bash
source ~/.sdkman/bin/sdkman-init.sh
sdk install java 21.0.6-tem
sdk use java 21.0.6-tem
```

#### No native toolchain

Lance is written in Rust, and the plugin talks to it through the JNI bindings in `org.lance:lance-core`. That artifact ships the compiled native library for macOS/aarch64, linux/x86_64 and linux/aarch64, and the build bundles it into the plugin zip. You do not need Rust, CMake or any C compiler to build, test or run the plugin. On other platforms the native library is missing and the plugin fails to load.

## Use an editor

The build is a standard Gradle project. In IntelliJ IDEA, choose **File > Open**, select the root `build.gradle`, and open it as a project with a JDK 21 SDK. The `idea` and `eclipse` Gradle plugins are applied, so `./gradlew idea` or `./gradlew eclipse` also generate project files.

## Java formatting

Java sources are formatted with the Eclipse JDT formatter through the [Spotless Gradle](https://github.com/diffplug/spotless/tree/main/plugin-gradle) plugin, using the configuration in `buildSrc/formatterConfig.xml`. The formatting check is part of `./gradlew check`; run it alone with:

```
./gradlew spotlessJavaCheck
```

Apply the formatting with:

```
./gradlew spotlessApply
```

Java indent is 4 spaces, line width is 140 characters, wildcard imports are forbidden, and unused imports are removed by the formatter. Use imports rather than fully qualified class names.

## Build

The repository uses the Gradle wrapper; run `./gradlew` on Unix systems. The first build downloads the OpenSearch build tools and dependencies and takes several minutes.

Build the plugin zip without running the tests:

```
./gradlew assemble
```

The artifact is `build/distributions/opensearch-lance-<version>.zip` (`version` in `gradle.properties`, `0.1.0` today). It is about 280 MB; the bulk is the Lance native library inside `lance-core`.

Build everything, including the precommit checks and all three test suites, exactly as the CI does:

```
./gradlew build
```

### Building against another OpenSearch version

`build.gradle` reads the OpenSearch version from the `opensearch.version` system property and defaults to `3.8.0`. To compile and test against a different version (releases from Maven Central, snapshots from the OpenSearch snapshot repository, both of which `build.gradle` lists):

```
./gradlew build -Dopensearch.version=<version>
```

The plugin declares that version in its `plugin-descriptor.properties`, so the zip only installs into an OpenSearch distribution of the same version.

## Run the plugin in a local cluster

The OpenSearch plugin build tools register a `./gradlew run` task, but `build.gradle` does not configure that cluster with the two JVM options the plugin requires, so use it only after adding them to a `testClusters.run` block. The supported way to try the plugin is to install the zip into an OpenSearch 3.8.0 distribution. [`docs/getting-started.md`](docs/getting-started.md) walks through it for Docker and for a native distribution, and continues with preparing a Lance table and running every query shape. The two options every node needs are:

```
--add-opens=java.base/java.nio=ALL-UNNAMED
-Darrow.allocation.manager.type=Unsafe
```

The first is required by Arrow's C data interface; the second picks the Arrow allocator that works with OpenSearch's Netty configuration.

## Run the tests

### Unit tests

```
./gradlew test
```

Unit tests are the `*Tests` classes under `src/test/java`, extending `OpenSearchTestCase`. They run in the Gradle test JVM and take well under a minute after the first build.

### Integration tests

```
./gradlew integTest
```

`integTest` starts a single node OpenSearch test cluster with the plugin installed and runs the `*IT` classes under `src/test/java` against it over REST. The classes extend `LanceRestTestCase`, which writes Lance tables into `build/shared-tables` and registers that directory as a namespace, so the tests need no external table. `LanceMultiNodeIT` is excluded here and runs on its own:

```
./gradlew multiNodeIntegTest
```

`multiNodeIntegTest` starts a three node cluster and runs the coordinator fan-out and namespace propagation checks that need more than one node. Expect a few minutes for each integration task.

Gradle only runs test classes whose names end in `Tests` or `IT`. The `testingConventions` precommit check reports a class that has test methods but another suffix, so it cannot be skipped silently. Shared base classes end in `TestCase` (`LanceRestTestCase`).

### Running one test class

Pass `--tests` with the fully qualified class name (method names can follow after a dot):

```
./gradlew test --tests 'org.opensearch.lance.LanceOverridesTests'
./gradlew integTest --tests 'org.opensearch.lance.LanceAttachIT'
./gradlew integTest --tests 'org.opensearch.lance.LanceAttachIT.testAttachRejectsMissingTable'
```

Test results are written as JUnit XML under `build/test-results/<task>/TEST-*.xml`, and the cluster logs of an integration run under `build/testclusters/<task>-<n>/logs/`.

### Precommit checks

`./gradlew build` runs `check`, which includes the OpenSearch `precommit` task group and the Spotless formatting check before the test suites. The individual tasks are:

* `forbiddenApis` (`forbiddenApisMain`, `forbiddenApisTest`, `forbiddenApisTestFixtures`): rejects JDK and Lucene APIs OpenSearch forbids in plugins.
* `licenseHeaders`: every Java source must start with the `Copyright OpenSearch Contributors` / `SPDX-License-Identifier: Apache-2.0` header.
* `jarHell`: no class appears twice on the runtime classpath.
* `thirdPartyAudit`: bundled third party jars reference no missing classes and no `sun.misc.Unsafe` user is unaccounted for. The ignore lists in `build.gradle` are grouped by the jar that holds the reference, one comment per group.
* `dependencyLicenses`: every bundled jar has a matching `licenses/<jar>.sha1`, `LICENSE` and `NOTICE` file. After changing a dependency version run `./gradlew updateShas` and commit the new `.sha1` files.
* `testingConventions`, `filepermissions`, `validateNebulaPom`.
* `spotlessJavaCheck`: the formatting described above.

Run them without the tests:

```
./gradlew precommit spotlessJavaCheck
```

`loggerUsageCheck` is disabled in `build.gradle` because the module it needs is not published outside the opensearch-project organization.

### Dependencies, audit lists and the security policy

[docs/dependencies.md](docs/dependencies.md) lists every bundled jar with the code that needs it, the artifacts that were tried and left out, how the `thirdPartyAudit` ignore lists were verified and how to re-verify them after a version change (empty the lists, run `./gradlew thirdPartyAudit`, compare the report with `build.gradle`), the license file mappings, and the reason behind each grant in `src/main/plugin-metadata/plugin-security.policy`. Read it before adding or upgrading a dependency; the "Checking a dependency change" section at its end is the checklist.

## Debugging

To debug a unit test, run it from the IDE with the debugger, or attach a debugger to the Gradle test JVM:

```
./gradlew test --tests 'org.opensearch.lance.LanceOverridesTests' --debug-jvm
```

`--debug-jvm` makes the test JVM wait for a debugger on `localhost:5005` before running. The same option applies to the integration test runner:

```
./gradlew integTest --tests 'org.opensearch.lance.LanceAttachIT' --debug-jvm
```

This attaches to the JVM that sends the REST requests, not to the OpenSearch node. To step through plugin code running inside the node, add a JDWP agent to the cluster JVM in a scratch edit of the `testClusters.integTest` block in `build.gradle`, for example `jvmArgs('-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=127.0.0.1:5006')`, and attach the debugger to port 5006. Do not commit that edit.

Whichever JVM you attach to, the node logs under `build/testclusters/integTest-0/logs/` are usually the fastest way to see why a request failed.

## Repository layout

All production code lives under `src/main/java/org/opensearch/lance/`, one subpackage per concern (attach, dispatch, engine, namespace, the Calcite planner under `plan/`, the `lance_*` queries, REST handlers, and so on). The subpackage tree with the role of each package is in the [Directory layout](docs/architecture.md#directory-layout) section of `docs/architecture.md`, which also explains the request paths and the design behind them. Tests mirror the main tree under `src/test/java/`. The user facing reference is `docs/features.md` and the known gaps are in `docs/limitations.md`.

## Submitting changes

See [CONTRIBUTING](CONTRIBUTING.md): open an issue first, sign every commit with `git commit -s`, add a changelog fragment under `changelog/unreleased/`, and fill every section of the pull request template.
