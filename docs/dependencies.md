# Dependencies

What the plugin zip bundles, why each jar is there, and how the build's
three gates (`thirdPartyAudit`, `jarHell`, `dependencyLicenses`) and the
security policy relate to it. Versions live in `gradle.properties`; the
declarations, grouped the same way as below, are in `build.gradle`.

- [How the closure is built](#how-the-closure-is-built)
- [Bundled jars](#bundled-jars)
- [Jars tried and not bundled](#jars-tried-and-not-bundled)
- [Third party audit](#third-party-audit)
- [Licenses and notices](#licenses-and-notices)
- [Security policy](#security-policy)
- [Checking a dependency change](#checking-a-dependency-change)

## How the closure is built

The `opensearch.opensearchplugin` Gradle plugin resolves no transitive
dependencies for third party groups, so `build.gradle` declares every jar
that ends up in the zip and "excluding" an artifact means not declaring it.
The upside is that the zip contains exactly the list below and nothing a
POM change upstream can add silently; the downside is that a new
dependency's whole compile tree has to be written out.

Classes that OpenSearch core already ships (`jackson-core`, `protobuf-java`,
`jackson-dataformat-yaml`, `reactive-streams`, `jts-core` 1.15, Lucene,
log4j) must not be bundled a second time: `jarHell` fails the build when a
class appears twice on the runtime classpath. `jackson-core` is excluded
from every declaration that pulls it in transitively for that reason.

The zip is 315.9 MB. 279 MB of it is native code: `lance-core` carries
`liblance_jni` for linux/x86_64, linux/aarch64 and darwin/aarch64 (201 MB
compressed) and `arrow-dataset` carries `libarrow_dataset_jni` for five
platforms (78 MB), a library the plugin never loads (see the table). The
other 58 third party jars total 41 MB, and the plugin's own jar 1 MB.

## Bundled jars

Sizes are the compressed jar sizes inside the zip. "Needed by" names the
bundled code that references the jar, from a `jdeps -verbose:class` scan of
the whole closure, or the plugin class that uses it.

### Lance and Apache Arrow

| jar | size | needed by |
|---|---|---|
| lance-core 12.0.0 | 201.4 MB | The plugin: `Dataset`, `LanceScanner`, the index builders, `DirectoryNamespace` and `RestNamespace`. Contains the JNI library for three platforms and a shaded Guava. |
| jar-jni 1.1.1 | 26 KB | lance-core's `JniLoader` delegates to `io.questdb.jar.jni.JarJniLoader` to extract and load `liblance_jni`. lance-core's POM declares it with unresolved `${arrow.version}` and `${opentelemetry.version}` properties, which makes Gradle drop the whole dependency block, so it is declared explicitly. |
| arrow-vector 18.3.0 | 2.2 MB | The plugin (`org.apache.arrow.vector`, 104 imports) and lance-core: every column the plugin reads is an Arrow vector. |
| arrow-memory-core 18.3.0 | 117 KB | `BufferAllocator`, `RootAllocator`, `ArrowBuf`; referenced by arrow-vector, lance-core and every namespace client. |
| arrow-memory-unsafe 18.3.0 | 10 KB | The allocation manager the node selects with `-Darrow.allocation.manager.type=Unsafe`, loaded by reflection. arrow-memory-netty cannot run under OpenSearch's `-Dio.netty.noUnsafe=true`. |
| arrow-dataset 18.3.0 | 77.8 MB | Two interfaces only: `org.lance.ipc.LanceScanner` implements `org.apache.arrow.dataset.scanner.Scanner` and returns `ScanTask`. Dropping the jar makes `LanceScanner` fail to link. The jar's `libarrow_dataset_jni` (five platforms) is never loaded; shrinking the zip by 78 MB needs Lance to stop implementing the Arrow Dataset interface or Arrow to publish the API without the natives. |
| arrow-format 18.3.0 | 122 KB | The FlatBuffers schema classes (`org.apache.arrow.flatbuf`) arrow-vector's IPC reader and `Schema` serialisation use. |
| arrow-c-data 18.3.0 | 126 KB | The C Data Interface (`org.apache.arrow.c`) through which lance-core hands Arrow batches from Rust to the JVM; the plugin imports it in two places. Extracts and loads `libarrow_cdata_jni`. |
| flatbuffers-java 2.0.0 | 67 KB | arrow-format (179 classes) and arrow-vector's IPC message classes. |
| commons-codec 1.18.0 | 373 KB | arrow-vector's `JsonFileReader` / `JsonFileWriter` (`Hex`) and Calcite: `BuiltInMethod`'s static initializer reflects over `SqlFunctions`, whose signatures name `DecoderException`, so the class must resolve even though no SQL function runs. |
| slf4j-api 2.0.17 | 70 KB | Arrow, Calcite, Avatica, HttpClient 5, the AWS SDK and the catalog clients log through SLF4J. OpenSearch core carries the log4j binding. |

### Lance Namespace and catalog clients

| jar | size | needed by |
|---|---|---|
| lance-namespace-core 0.11.1 | 31 KB | The `LanceNamespace` interface the plugin's `LanceNamespaceService` drives, and the base class of every catalog client. |
| lance-namespace-apache-client 0.11.1 | 690 KB | The request and response models (`org.lance.namespace.model`) and error classes that `DirectoryNamespace`, `RestNamespace` and the catalog clients exchange; also the generated `ApiClient`, which nothing calls. |
| lance-namespace-impls-core 0.4.1 | 18 KB | `RestClient` (the HTTP layer of the Iceberg, Polaris and Unity clients) and `LanceTableUtil`. |
| lance-namespace-glue 0.4.1 | 18 KB | `GlueNamespace`, the `glue` namespace type. |
| lance-namespace-iceberg 0.4.1 | 25 KB | `IcebergNamespace`, the `iceberg` namespace type. |
| lance-namespace-polaris 0.4.1 | 22 KB | `PolarisNamespace`, the `polaris` namespace type. |
| lance-namespace-unity 0.4.1 | 23 KB | `UnityNamespace`, the `unity` namespace type. |

The four 0.4.1 clients build against lance-namespace 0.7.7; no released
version targets 0.11.x. The surface the plugin calls (`initialize`,
`listNamespaces`, `listTables`, `describeTable`, their models and the
errors package) was verified binary compatible against the bundled 0.11.1
by comparing `javap` method descriptors, and each client is loaded and
initialised offline by a unit test. `docs/limitations.md` records why the
Hive metastore clients are not bundled.

### Apache HttpClient 5

| jar | size | needed by |
|---|---|---|
| httpclient5 5.6.1 | 1.1 MB | impls-core's `RestClient` (Iceberg, Polaris, Unity). Also referenced by `ApiClient` and by Avatica's remote client, neither of which runs. |
| httpcore5 5.4 | 953 KB | httpclient5, httpcore5-h2, `RestClient`. |
| httpcore5-h2 5.4 | 263 KB | httpclient5's HTTP/2 classes reference it; the audit needs it present. |

impls-core compiles against httpclient5 5.2.1. The bundled versions are
the ones OpenSearch's test framework puts on the test classpath through
`opensearch-rest-client`; the plugin has to match them or the
`opensearchplugin` version conflict check fails. All 24 HttpClient methods
impls-core and the three clients call resolve against 5.6.1 / 5.4.

### AWS SDK for Java 2 (Glue)

The Glue client's compile tree at 2.32.2, the version
`lance-namespace-glue` 0.4.1 was built against. Each module is referenced
by the modules listed; `glue` itself by `GlueNamespace`.

| jar | size | referenced by |
|---|---|---|
| glue 2.32.2 | 8.9 MB | lance-namespace-glue (`GlueNamespace`, `GlueNamespaceConfig`, `GlueToLanceErrorConverter`) |
| aws-json-protocol 2.32.2 | 142 KB | glue |
| protocol-core 2.32.2 | 44 KB | glue, aws-json-protocol |
| aws-core 2.32.2 | 191 KB | glue, aws-json-protocol, lance-namespace-glue |
| sdk-core 2.32.2 | 1.0 MB | glue, aws-core, aws-json-protocol, auth, regions, protocol-core, lance-namespace-glue |
| auth 2.32.2 | 235 KB | aws-core, lance-namespace-glue (credential providers) |
| regions 2.32.2 | 951 KB | glue, aws-core, auth, lance-namespace-glue |
| profiles 2.32.2 | 51 KB | auth, aws-core, regions, sdk-core |
| endpoints-spi 2.32.2 | 13 KB | glue, aws-core, sdk-core |
| identity-spi 2.32.2 | 31 KB | glue, auth, aws-core, sdk-core, the http-auth modules |
| http-client-spi 2.32.2 | 92 KB | every module above and url-connection-client |
| http-auth 2.32.2 | 17 KB | glue, auth, aws-core |
| http-auth-aws 2.32.2 | 159 KB | glue, auth, sdk-core (the SigV4 signer) |
| http-auth-spi 2.32.2 | 46 KB | glue, auth, aws-core, sdk-core, http-auth, http-auth-aws |
| checksums 2.32.2 | 70 KB | sdk-core, http-auth-aws |
| checksums-spi 2.32.2 | 8 KB | sdk-core, checksums, http-auth-aws |
| retries 2.32.2 | 66 KB | aws-core, sdk-core |
| retries-spi 2.32.2 | 31 KB | glue, aws-core, sdk-core, retries |
| metrics-spi 2.32.2 | 27 KB | glue, aws-core, aws-json-protocol, sdk-core, http-client-spi |
| json-utils 2.32.2 | 33 KB | glue, auth, aws-json-protocol, regions |
| third-party-jackson-core 2.32.2 | 535 KB | aws-json-protocol, json-utils (the SDK's shaded JSON parser) |
| utils 2.32.2 | 240 KB | every module |
| annotations 2.32.2 | 14 KB | The `SdkPublicApi`, `SdkInternalApi`, `ThreadSafe` and similar annotation types applied to every SDK class. The JVM tolerates a missing annotation type, so the jar is technically removable; it stays so that reflection over SDK classes sees the annotations, as in the organization's repository-s3. |
| url-connection-client 2.32.2 | 33 KB | The SDK's synchronous HTTP transport, loaded through the `SdkHttpService` service file rather than a class reference. `apache-client` (with `httpcore` 4 and `commons-logging`) and `netty-nio-client` are not bundled because this one needs nothing beyond the SPI. |
| eventstream 1.0.1 | 30 KB | `software.amazon.eventstream.Message` is referenced by aws-core's `EventStreamAsyncResponseTransformer`, auth's `BaseEventStreamAsyncAws4Signer` and http-auth-aws's `SigV4DataFramePublisher`. The Glue API has no event stream operation, so none of them runs, but the audit needs the class present. |

### Jackson

OpenSearch core ships `jackson-core` only.

| jar | size | needed by |
|---|---|---|
| jackson-databind 2.21.3 | 1.7 MB | arrow-vector's `Schema` JSON, the namespace models, lance-core's `JsonUtils`, Substrait's extension loader, Calcite, Avatica, json-path. |
| jackson-annotations 2.21 | 82 KB | The models of lance-namespace-apache-client and the catalog clients, Calcite, Avatica, Substrait. |
| jackson-datatype-jdk8 2.21.3 | 36 KB | Substrait's `SimpleExtension` registers the `Jdk8Module` when it deserialises the extension YAML (`Optional` fields). |
| jackson-datatype-jsr310 2.21.3 | 137 KB | `JavaTimeModule`, registered by lance-core's `DirectoryNamespace` and `RestNamespace` and by the namespace `ApiClient` for the timestamp fields of the 0.11 REST models. |

### Apache Calcite and Substrait

| jar | size | needed by |
|---|---|---|
| calcite-core 1.39.0 | 8.5 MB | The planner (`org.opensearch.lance.plan`): `RelOptCluster`, `RelBuilder`, the Volcano and Hep planners, metadata, the type system. 1.39.0 is the version isthmus 0.57.0 declares and was compiled against. |
| calcite-linq4j 1.39.0 | 519 KB | Creating any character type walks `SqlCollation`'s static initializer into `CalciteTrace`, which loads `org.apache.calcite.linq4j.function.Functions`. Dropping it fails the unit tests with `NoClassDefFoundError`. |
| avatica-core 1.26.0 | 1.3 MB | calcite-core's compile dependency; the type code reaches Avatica's utility classes (`ByteString`, `DateTimeUtils`). Version declared by calcite-core. |
| guava 33.3.0-jre | 3.1 MB | calcite-core (type interning through `LoadingCache`, immutable collections), calcite-linq4j, isthmus, lance-namespace-glue; the plugin imports `com.google.common.collect` in five places. Version declared by calcite-core. |
| failureaccess 1.0.2 | 5 KB | Guava's `AbstractFuture` extends `InternalFutureFailureAccess`. |
| core 0.57.0 (io.substrait) | 4.8 MB | The Substrait plan model and extension loader the plugin emits plans with. |
| isthmus 0.57.0 | 180 KB | `SubstraitRelVisitor`, the Calcite RelNode to Substrait plan bridge. |
| janino 3.1.11 | 957 KB | `JaninoRelMetadataProvider` compiles Calcite's metadata handlers with Janino on the first metadata call; without the jar `RelMetadataQuery.getRowCount` throws `IllegalStateException: Unable to instantiate java compiler`. Version declared by calcite-core. |
| commons-compiler 3.1.11 | 172 KB | Loading `JaninoRelMetadataProvider` (which every `RelOptCluster` constructor does) needs `org.codehaus.commons.compiler.CompileException`. |
| json-path 2.9.0 | 277 KB | Initialising `DefaultRelMetadataProvider` walks `BuiltInMethod`'s static initializer over `JsonFunctions`, whose method signatures name json-path types; without the jar `RelOptCluster.create` throws `NoClassDefFoundError`. Only the signatures are needed, no JSON function runs. |

## Jars tried and not bundled

Each of these was either excluded and shown unnecessary (the audit, the
unit tests and both integration clusters pass without it) or is kept out
because OpenSearch core already carries it.

| artifact | why it is not in the zip |
|---|---|
| http-auth-aws-eventstream 2.32.2 | A Maven carrier module: its only class is the empty `HttpAuthAwsEventStream`, whose POM says the module exists to bring in `eventstream` "but does not have any code that explicitly uses it". Nothing in the closure references it and `eventstream` is declared directly. |
| arrow-memory-netty | Cannot run under OpenSearch's `-Dio.netty.noUnsafe=true`; arrow-memory-unsafe is the allocator. |
| jackson-core, protobuf-java, jackson-dataformat-yaml, reactive-streams, jts-core | On the OpenSearch core classpath; bundling any of them is jarHell. |
| calcite-server | isthmus references its SQL DDL parser from its SQL entry points only; the plugin converts `RelNode` plans and never parses SQL. |
| avatica-metrics | Referenced by Avatica's remote JDBC server side only. |
| jts-core 1.19, jts-io-common, proj4j | Calcite's geo functions; unreachable, and core already ships JTS 1.15 (jarHell). |
| sketches-core, uzaygezen-core, aggdesigner-algorithm | Calcite's profiler and lattice tooling; unreachable from the planning API. |
| commons-dbcp2, commons-io, commons-lang, commons-lang3, commons-math3, commons-text | Calcite's JDBC adapter, visualizer, SQL function bodies and SQL validator; unreachable from the planning API. |
| json-smart | json-path's default provider; instantiated only when a json-path `Configuration` is built inside Calcite's JSON_* SQL function bodies. |
| protobuf-java-util, reflections, caffeine | Used by isthmus's tests only. |
| apache-client, httpcore 4, commons-logging, netty-nio-client | Alternative AWS SDK HTTP transports; url-connection-client is the one in use. |
| aws-crt | The CRT backed SigV4a signer; the default signer is pure Java. |
| lance-namespace-hive2, lance-namespace-hive3 | See "Hive Metastore catalogs" in `docs/limitations.md`. |
| apiguardian, checker-qual, error_prone_annotations, jspecify, jsr305, value-annotations | Class retention annotations; the audit does not flag them and the JVM tolerates their absence. |

## Third party audit

`thirdPartyAudit` scans the bytecode of every bundled jar against the
OpenSearch core classpath and fails on any class it cannot resolve and on
any use of `sun.misc.Unsafe`. The two lists in `build.gradle` are grouped
by the jar whose bytecode holds the reference; each group's comment names
the referencing classes and why that code path is not reached.

The `ignoreMissingClasses` list has 124 entries in 12 groups (lance-core:
OpenTelemetry, the shaded failureaccess; lance-namespace-apache-client:
`JsonNullable`; calcite-core: geo, statistics, Apache Commons; avatica-core:
metrics; isthmus: `SqlDdlParserImpl`; janino: Ant; json-path: the JSON
providers; httpclient5 and httpcore5-h2: Brotli, Commons Compress,
Conscrypt; http-auth-aws: aws-crt). The `ignoreViolations` list has 28
entries in 3 groups (Guava, Arrow's `MemoryUtil`, lance-core's shaded
Guava). Guava's `Striped64` and `AbstractFuture$UnsafeAtomicHelper` and
Arrow's `MemoryUtil` are reached on every request; the other Unsafe users
are in the jars but referenced by nothing outside their own library.

To re-verify the lists after a version change, empty both lists, run
`./gradlew thirdPartyAudit` and compare the task's "Missing classes" and
"Classes with violations" output with the entries in `build.gradle`; every
entry that is no longer reported can be removed, every new class needs a
group. `jdeps --multi-release 21 -verbose:class -cp <all bundled jars and
the core jars> <jar>` names the class that holds each reference (the
modular Arrow and Jackson jars need their `module-info.class` removed from
a copy first, or jdeps resolves them as modules).

## Licenses and notices

`dependencyLicenses` requires, for every bundled jar, a
`licenses/<jar>.sha1` (generated by `./gradlew updateSHAs`) and a
`<name>-LICENSE.txt` / `<name>-NOTICE.txt` pair, where `<name>` is the jar
name without its version unless a `mapping` in `build.gradle` collapses a
family of jars onto one pair (`arrow-*` onto `arrow`, the AWS SDK modules
onto `aws-sdk`, and so on). The 60 third party jars map onto 20 pairs. `NOTICE.txt` at
the repository root names the projects; the `generateNotice` task appends
every `licenses/*-NOTICE.txt` to it when it builds the zip.

Licenses other than Apache 2.0 in the closure: Janino and commons-compiler
(BSD 3-Clause), slf4j-api (MIT).

## Security policy

`src/main/plugin-metadata/plugin-security.policy` carries one comment per
grant naming the class that needs it. Three facts about OpenSearch 3.x
shape it.

- The Java agent that replaced the SecurityManager enforces
  `java.io.FilePermission` on `java.nio.file.Files`, `FileChannel` and
  `FileSystemProvider` operations, `java.net.SocketPermission` on socket
  connects, and the exit and halt interceptors. `RuntimePermission`,
  `ReflectPermission` and `PropertyPermission` are not checked. The
  reflection and class loader grants stay because `opensearch-plugin
  install` shows them to the operator and because the organization's
  plugins declare theirs.
- The core policy grants every codebase, plugin jars included, read, write
  and delete under `path.data`, `path.logs`, `path.repo` and
  `java.io.tmpdir`. The JNI extraction (both loaders write a temporary
  file under `java.io.tmpdir`), the `node_local` clone directories under
  `path.data` and the text analyzer backfill spool need no plugin grant.
- The Lance native library reads and writes table files and opens object
  store connections itself, outside the JVM. The agent never sees those
  operations, so no `FilePermission` in the plugin policy widens or
  narrows what a table URI may point at; `lance.allowed_table_roots` is
  the control. The plugin's own Java file access outside the core grants
  is the read of `/proc/meminfo` by `FtsAdmission`, which has an explicit
  grant.

The policy language expands system properties (`${java.io.tmpdir}`,
`${/}`) and the `${codebase.<jar>}` names of the plugin's own jars; it has
no `${path.data}` style expansion, which is why the core grants the data
path itself.

## Checking a dependency change

After changing a version or adding a jar:

1. `./gradlew updateSHAs` and commit the new `licenses/*.sha1`; add or map
   the `LICENSE` / `NOTICE` pair.
2. `./gradlew thirdPartyAudit jarHell dependencyLicenses`; regroup any new
   missing class under the jar that references it with a comment saying
   why the path is not reached, or bundle the jar it needs.
3. `./gradlew test integTest multiNodeIntegTest`; the two integration
   clusters run the plugin under the agent, so a grant the policy lacks
   surfaces there as a `SecurityException` in the node log.
4. Update the tables in this file.
