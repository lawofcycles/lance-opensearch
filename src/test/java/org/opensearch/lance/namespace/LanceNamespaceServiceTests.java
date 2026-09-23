/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.lance.namespace.model.DescribeTableResponse;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;

/**
 * Unit tests for {@link LanceNamespaceService}. Registration state
 * lives in cluster-state metadata routed through a
 * {@code TransportClusterManagerNodeAction}, so
 * {@link LanceNamespaceService#register} cannot be exercised without a
 * live transport stack; the tests here cover the surface that stands
 * on its own: cadence exposure, the metadata-backed
 * {@link LanceNamespaceService#namespaces} reader, and the listing
 * cycle's handling of tables whose index already exists.
 *
 * <p>End-to-end register / unregister behaviour is exercised in
 * {@code LanceNamespaceIT}.
 *
 * <p>Thread leak checking is disabled at the suite level because
 * {@code DirectoryNamespace.initialize} spins up Lance's native runtime,
 * whose worker threads outlive the JUnit test process and cannot be
 * shut down from Java. The Docker integration environment terminates
 * them at process exit.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceNamespaceServiceTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private NoOpClient client;
    private ClusterService clusterService;
    private LanceNamespaceService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        client = new NoOpClient(threadPool);
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        // A one-hour cadence keeps the scheduled poll from firing during a
        // unit test, so the tests only see the state we drive explicitly.
        service = new LanceNamespaceService(client, clusterService, threadPool, TimeValue.timeValueHours(1), 1_000_000L);
    }

    @Override
    public void tearDown() throws Exception {
        client.close();
        clusterService.close();
        ThreadPool.terminate(threadPool, 30L, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testCadenceReturnsConstructorValue() {
        assertEquals(TimeValue.timeValueHours(1), service.cadence());
    }

    public void testNamespacesStartsEmpty() {
        assertTrue("expected empty namespaces list, saw: " + service.namespaces(), service.namespaces().isEmpty());
    }

    public void testNamespacesReadsFromClusterState() {
        // Bypass the transport action layer and inject a cluster
        // state directly. The service reads Metadata.custom on every
        // namespaces() call, so this exercises the read path without
        // depending on a live transport.
        String uri = "/injected-" + randomAlphaOfLength(8);
        LanceNamespaceMetadata metadata = LanceNamespaceMetadata.EMPTY.withRegistered(
            new LanceNamespaceMetadata.Entry(uri, StorageOptions.empty())
        );
        ClusterState state = ClusterState.builder(clusterService.state())
            .metadata(Metadata.builder(clusterService.state().metadata()).putCustom(LanceNamespaceMetadata.TYPE, metadata))
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        assertEquals(List.of(uri), service.namespaces());
    }

    public void testNamespacesReturnsUnmodifiableSnapshot() {
        // The list returned by namespaces() must be a defensive copy;
        // attempting to mutate it should throw regardless of whether
        // the underlying state changes later.
        String uri = "/read-only-" + randomAlphaOfLength(8);
        LanceNamespaceMetadata metadata = LanceNamespaceMetadata.EMPTY.withRegistered(
            new LanceNamespaceMetadata.Entry(uri, StorageOptions.empty())
        );
        ClusterState state = ClusterState.builder(clusterService.state())
            .metadata(Metadata.builder(clusterService.state().metadata()).putCustom(LanceNamespaceMetadata.TYPE, metadata))
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        List<String> before = service.namespaces();
        expectThrows(UnsupportedOperationException.class, () -> before.add("/injected"));
        assertEquals(1, service.namespaces().size());
    }

    public void testPollLeavesAnExistingIndexOfTheTableAloneWithoutOpeningIt() throws Exception {
        // The manager's cycle only lists catalogs and creates indexes.
        // A table whose index exists (index.lance.table equals the table
        // path) is left to the node holding its shard: no dataset open,
        // no refresh, no mapping update, nothing through the client. The
        // table path here does not exist on disk, so an open would have
        // failed and shown up as a skipped table.
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.tables = Set.of("orders");
        LanceNamespaceFactory.setInstantiatorForTests(type -> recording);
        RecordingNoOpClient recordingClient = new RecordingNoOpClient(threadPool);
        LanceNamespaceService listing = new LanceNamespaceService(
            recordingClient,
            clusterService,
            threadPool,
            TimeValue.timeValueHours(1),
            1_000_000L
        );
        try {
            String root = "/no-such-root-" + randomAlphaOfLength(6);
            LanceNamespaceMetadata metadata = LanceNamespaceMetadata.EMPTY.withRegistered(
                new LanceNamespaceMetadata.Entry(root, StorageOptions.empty())
            );
            IndexMetadata existing = IndexMetadata.builder("orders")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_INDEX_UUID, "orders-uuid")
                        .put(LanceEngineFactory.TABLE_SETTING, root + "/orders.lance")
                )
                .build();
            ClusterState state = ClusterState.builder(clusterService.state())
                .metadata(
                    Metadata.builder(clusterService.state().metadata())
                        .putCustom(LanceNamespaceMetadata.TYPE, metadata)
                        .put(existing, false)
                )
                .build();
            ClusterServiceUtils.setState(clusterService, state);

            LanceNamespaceService.PollReport report = listing.pollNow(null);
            assertTrue("nothing to surface: " + report, report.surfaced().isEmpty());
            assertTrue("an existing index of the table is not a skip: " + report, report.skipped().isEmpty());
            assertTrue("no client action for an existing index: " + recordingClient.actionNames, recordingClient.actionNames.isEmpty());
        } finally {
            LanceNamespaceFactory.resetInstantiatorForTests();
        }
    }

    public void testPollReportsANameCollisionAndWarnsOnce() throws Exception {
        // An index under the table's name that is not backed by the table
        // (a plain index here) is a name collision: the cycle skips the
        // table with the reason and warns once, not on every cycle.
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.tables = Set.of("orders");
        LanceNamespaceFactory.setInstantiatorForTests(type -> recording);
        RecordingNoOpClient recordingClient = new RecordingNoOpClient(threadPool);
        LanceNamespaceService listing = new LanceNamespaceService(
            recordingClient,
            clusterService,
            threadPool,
            TimeValue.timeValueHours(1),
            1_000_000L
        );
        try {
            String root = "/no-such-root-" + randomAlphaOfLength(6);
            LanceNamespaceMetadata metadata = LanceNamespaceMetadata.EMPTY.withRegistered(
                new LanceNamespaceMetadata.Entry(root, StorageOptions.empty())
            );
            IndexMetadata plain = IndexMetadata.builder("orders")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_INDEX_UUID, "orders-uuid")
                )
                .build();
            ClusterState state = ClusterState.builder(clusterService.state())
                .metadata(
                    Metadata.builder(clusterService.state().metadata()).putCustom(LanceNamespaceMetadata.TYPE, metadata).put(plain, false)
                )
                .build();
            ClusterServiceUtils.setState(clusterService, state);

            LanceNamespaceService.PollReport first = listing.pollNow(null);
            assertTrue(first.surfaced().isEmpty());
            assertEquals(1, first.skipped().size());
            assertEquals("orders", first.skipped().get(0).index());
            assertTrue(first.skipped().get(0).reason(), first.skipped().get(0).reason().startsWith("name collision"));
            LanceNamespaceService.PollReport second = listing.pollNow(null);
            assertEquals("the collision is reported on every cycle", 1, second.skipped().size());
            assertTrue("no client action for a collision: " + recordingClient.actionNames, recordingClient.actionNames.isEmpty());
        } finally {
            LanceNamespaceFactory.resetInstantiatorForTests();
        }
    }

    public void testInitializeFailureSurfacesAsUnavailableAndRetries() throws Exception {
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.initializeFailure = new IllegalStateException("bad credentials");
        LanceNamespaceFactory.setInstantiatorForTests(type -> recording);
        try {
            LanceNamespaceMetadata metadata = LanceNamespaceMetadata.EMPTY.withRegistered(
                new LanceNamespaceMetadata.Entry(
                    "cat",
                    LanceNamespaceMetadata.Entry.TYPE_REST,
                    null,
                    StorageOptions.empty(),
                    Map.of("uri", "http://catalog.example:8080", "header.Authorization", "Bearer hunter2")
                )
            );
            ClusterState state = ClusterState.builder(clusterService.state())
                .metadata(Metadata.builder(clusterService.state().metadata()).putCustom(LanceNamespaceMetadata.TYPE, metadata))
                .build();
            // The applier only records the registration; the poll (on
            // the generic pool in production) drives ensureHandle and
            // records the failure rather than throwing.
            ClusterServiceUtils.setState(clusterService, state);
            service.poll();
            List<LanceNamespaceListResponse.NamespaceInfo> infos = service.namespaceInfos();
            assertEquals(1, infos.size());
            assertEquals("cat", infos.get(0).name());
            assertEquals("rest", infos.get(0).type());
            assertNull(infos.get(0).path());
            assertEquals("bad credentials", infos.get(0).error());
            // The listing redacts the credential-bearing config key.
            assertEquals("***", infos.get(0).config().get("header.Authorization"));
            assertEquals("http://catalog.example:8080", infos.get(0).config().get("uri"));
            // The failed initialise still received the raw secret.
            assertEquals("Bearer hunter2", recording.initializeCalls.get(0).get("header.Authorization"));

            // A later poll cycle retries; success clears the status.
            recording.initializeFailure = null;
            service.poll();
            assertNull(service.namespaceInfos().get(0).error());
            assertEquals(2, recording.initializeCalls.size());
        } finally {
            LanceNamespaceFactory.resetInstantiatorForTests();
        }
    }

    public void testListTablesFallsBackToChildNamespacesForParentScopedCatalogs() throws Exception {
        // A Glue-style catalog rejects a root table listing; the service
        // walks the first level of child namespaces instead.
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.childNamespaces = Set.of("salesdb");
        recording.tables = Set.of("orders", "customers");
        LanceNamespaceFactory.setInstantiatorForTests(type -> recording);
        try {
            LanceNamespaceMetadata metadata = LanceNamespaceMetadata.EMPTY.withRegistered(
                new LanceNamespaceMetadata.Entry(
                    "glue-tokyo",
                    LanceNamespaceMetadata.Entry.TYPE_GLUE,
                    null,
                    StorageOptions.empty(),
                    Map.of("region", "ap-northeast-1")
                )
            );
            ClusterState state = ClusterState.builder(clusterService.state())
                .metadata(Metadata.builder(clusterService.state().metadata()).putCustom(LanceNamespaceMetadata.TYPE, metadata))
                .build();
            ClusterServiceUtils.setState(clusterService, state);
            Optional<Set<String>> tables = service.listTables("glue-tokyo");
            assertTrue(tables.isPresent());
            assertEquals(Set.of("orders", "customers"), tables.get());
        } finally {
            LanceNamespaceFactory.resetInstantiatorForTests();
        }
    }

    /** Register a stub-backed entry and return the tables the preview lists. */
    private Set<String> listThroughRegisteredStub(RecordingLanceNamespace recording, String type, Map<String, String> config)
        throws Exception {
        LanceNamespaceFactory.setInstantiatorForTests(seen -> recording);
        LanceNamespaceMetadata metadata = LanceNamespaceMetadata.EMPTY.withRegistered(
            new LanceNamespaceMetadata.Entry("walk-cat", type, null, StorageOptions.empty(), config)
        );
        ClusterState state = ClusterState.builder(clusterService.state())
            .metadata(Metadata.builder(clusterService.state().metadata()).putCustom(LanceNamespaceMetadata.TYPE, metadata))
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        Optional<Set<String>> tables = service.listTables("walk-cat");
        assertTrue(tables.isPresent());
        return tables.get();
    }

    public void testWalkReachesTablesOfTwoLevelTreeWithoutWarehouse() throws Exception {
        // Unity's shape: the root listing is rejected, the first level
        // (the catalog) holds no tables and answers bare child names,
        // the second level (the schema) holds the tables. The default
        // max_namespace_depth of 2 reaches them.
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.namespaceTree = Map.of("", Set.of("main"), "main", Set.of("default"));
        recording.tableTree = Map.of("main.default", Set.of("events"));
        try {
            assertEquals(Set.of("events"), listThroughRegisteredStub(recording, LanceNamespaceMetadata.Entry.TYPE_UNITY, Map.of()));
            // The walk asked exactly the tree's levels, nothing deeper.
            assertTrue(recording.listNamespacesIds.contains(List.of()));
            assertTrue(recording.listNamespacesIds.contains(List.of("main")));
            assertFalse(recording.listNamespacesIds.contains(List.of("main", "default")));
        } finally {
            LanceNamespaceFactory.resetInstantiatorForTests();
        }
    }

    public void testWalkStartsFromTheWarehouseConfigAndFollowsDotJoinedChildren() throws Exception {
        // Iceberg / Polaris shape: every id is rooted at the warehouse
        // named in config, and listNamespaces answers dot-joined full
        // paths (wh.ns1) that the walk must split back into id levels.
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.namespaceTree = Map.of("wh", Set.of("wh.ns1"));
        recording.tableTree = Map.of("wh.ns1", Set.of("logs"));
        try {
            assertEquals(
                Set.of("logs"),
                listThroughRegisteredStub(
                    recording,
                    LanceNamespaceMetadata.Entry.TYPE_ICEBERG,
                    Map.of("endpoint", "http://catalog.example:8181", "warehouse", "wh")
                )
            );
            // The seed came from the warehouse config, not the root.
            assertEquals(List.of("wh"), recording.listNamespacesIds.get(0));
            assertTrue(recording.listTablesIds.contains(List.of("wh", "ns1")));
        } finally {
            LanceNamespaceFactory.resetInstantiatorForTests();
        }
    }

    public void testWalkDepthIsBoundedByMaxNamespaceDepthConfig() throws Exception {
        // A third level exists but the default depth of 2 must not
        // reach it; raising max_namespace_depth to 3 must.
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.namespaceTree = Map.of("", Set.of("a"), "a", Set.of("a.b"), "a.b", Set.of("a.b.c"));
        recording.tableTree = Map.of("a.b.c", Set.of("deep"));
        try {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> listThroughRegisteredStub(recording, LanceNamespaceMetadata.Entry.TYPE_REST, Map.of("uri", "http://c.example"))
            );
            // Every reached namespace refused its table listing and no
            // table surfaced, so the failure is reported instead of an
            // empty result that would mask a misconfiguration.
            assertTrue(e.getMessage(), e.getMessage().contains("no tables"));
        } finally {
            LanceNamespaceFactory.resetInstantiatorForTests();
        }
        RecordingLanceNamespace deeper = new RecordingLanceNamespace();
        deeper.namespaceTree = Map.of("", Set.of("a"), "a", Set.of("a.b"), "a.b", Set.of("a.b.c"));
        deeper.tableTree = Map.of("a.b.c", Set.of("deep"));
        try {
            assertEquals(
                Set.of("deep"),
                listThroughRegisteredStub(
                    deeper,
                    LanceNamespaceMetadata.Entry.TYPE_REST,
                    Map.of("uri", "http://c.example", "max_namespace_depth", "3")
                )
            );
        } finally {
            LanceNamespaceFactory.resetInstantiatorForTests();
        }
    }

    public void testWalkCollectsTablesFromEveryNamespaceThatAnswers() throws Exception {
        // Tables can live at more than one level (Iceberg allows both);
        // the walk collects them all instead of stopping at the first
        // namespace that answers.
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.namespaceTree = Map.of("wh", Set.of("wh.ns1", "wh.ns2"), "wh.ns1", Set.of("wh.ns1.sub"));
        recording.tableTree = Map.of("wh.ns1", Set.of("top"), "wh.ns1.sub", Set.of("nested"), "wh.ns2", Set.of("side"));
        try {
            assertEquals(
                Set.of("top", "nested", "side"),
                listThroughRegisteredStub(
                    recording,
                    LanceNamespaceMetadata.Entry.TYPE_ICEBERG,
                    Map.of("endpoint", "http://catalog.example:8181", "warehouse", "wh")
                )
            );
        } finally {
            LanceNamespaceFactory.resetInstantiatorForTests();
        }
    }

    public void testInvalidMaxNamespaceDepthSurfacesAsListingFailure() throws Exception {
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.namespaceTree = Map.of("", Set.of("main"));
        recording.tableTree = Map.of();
        try {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> listThroughRegisteredStub(recording, LanceNamespaceMetadata.Entry.TYPE_UNITY, Map.of("max_namespace_depth", "zero"))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("max_namespace_depth"));
        } finally {
            LanceNamespaceFactory.resetInstantiatorForTests();
        }
    }

    public void testCatalogTypesReportUnavailableOnInitializeFailure() throws Exception {
        // The unavailable status and the retry loop for each of the
        // three catalog types added after glue, and the redaction of
        // their credential keys in the listing.
        Map<String, Map<String, String>> configByType = Map.of(
            LanceNamespaceMetadata.Entry.TYPE_ICEBERG,
            Map.of("endpoint", "http://catalog.example:8181", "warehouse", "wh", "credential", "id:sekrit"),
            LanceNamespaceMetadata.Entry.TYPE_POLARIS,
            Map.of("endpoint", "http://polaris.example:8181", "warehouse", "cat", "auth_token", "sekrit"),
            LanceNamespaceMetadata.Entry.TYPE_UNITY,
            Map.of("endpoint", "http://unity.example:8080", "catalog", "main", "auth_token", "sekrit")
        );
        int slot = 0;
        for (Map.Entry<String, Map<String, String>> typeAndConfig : configByType.entrySet()) {
            String type = typeAndConfig.getKey();
            RecordingLanceNamespace recording = new RecordingLanceNamespace();
            recording.initializeFailure = new IllegalStateException("401 from " + type);
            LanceNamespaceFactory.setInstantiatorForTests(seen -> recording);
            try {
                LanceNamespaceMetadata metadata = LanceNamespaceMetadata.EMPTY.withRegistered(
                    new LanceNamespaceMetadata.Entry(type + "-cat", type, null, StorageOptions.empty(), typeAndConfig.getValue())
                );
                ClusterState state = ClusterState.builder(clusterService.state())
                    .metadata(Metadata.builder(clusterService.state().metadata()).putCustom(LanceNamespaceMetadata.TYPE, metadata))
                    .version(clusterService.state().version() + ++slot)
                    .build();
                ClusterServiceUtils.setState(clusterService, state);
                service.poll();
                List<LanceNamespaceListResponse.NamespaceInfo> infos = service.namespaceInfos();
                assertEquals(1, infos.size());
                assertEquals(type, infos.get(0).type());
                assertEquals("401 from " + type, infos.get(0).error());
                for (String value : infos.get(0).config().values()) {
                    assertFalse("credential must be redacted: " + infos.get(0).config(), value.contains("sekrit"));
                }
                // The implementation still received the raw value.
                assertTrue(
                    "initialize must see the raw credential: " + recording.initializeCalls.get(0),
                    recording.initializeCalls.get(0).values().stream().anyMatch(value -> value.contains("sekrit"))
                );
            } finally {
                LanceNamespaceFactory.resetInstantiatorForTests();
            }
        }
    }

    public void testGlueEntryDrivesInitializeWithSecretsAndReportsUnavailableOnFailure() throws Exception {
        // The request path down to initialize for the glue type: the
        // stub receives the config with the credential keys intact, a
        // thrown initialize surfaces as the unavailable status, and the
        // listing never shows the raw secret.
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.initializeFailure = new IllegalStateException("The security token included in the request is invalid");
        LanceNamespaceFactory.setInstantiatorForTests(type -> {
            assertEquals(LanceNamespaceMetadata.Entry.TYPE_GLUE, type);
            return recording;
        });
        try {
            LanceNamespaceMetadata metadata = LanceNamespaceMetadata.EMPTY.withRegistered(
                new LanceNamespaceMetadata.Entry(
                    "glue-tokyo",
                    LanceNamespaceMetadata.Entry.TYPE_GLUE,
                    null,
                    StorageOptions.empty(),
                    Map.of(
                        "region",
                        "ap-northeast-1",
                        "catalog_id",
                        "123456789012",
                        "root",
                        "s3://bucket/prefix",
                        "access_key_id",
                        "AKIA123",
                        "secret_access_key",
                        "sekrit"
                    )
                )
            );
            ClusterState state = ClusterState.builder(clusterService.state())
                .metadata(Metadata.builder(clusterService.state().metadata()).putCustom(LanceNamespaceMetadata.TYPE, metadata))
                .build();
            ClusterServiceUtils.setState(clusterService, state);
            service.poll();
            Map<String, String> received = recording.initializeCalls.get(0);
            assertEquals("sekrit", received.get("secret_access_key"));
            assertEquals("AKIA123", received.get("access_key_id"));
            assertEquals("ap-northeast-1", received.get("region"));
            List<LanceNamespaceListResponse.NamespaceInfo> infos = service.namespaceInfos();
            assertEquals("glue", infos.get(0).type());
            assertTrue(infos.get(0).error(), infos.get(0).error().contains("security token"));
            assertEquals("***", infos.get(0).config().get("secret_access_key"));
            assertEquals("***", infos.get(0).config().get("access_key_id"));
            assertEquals("123456789012", infos.get(0).config().get("catalog_id"));
        } finally {
            LanceNamespaceFactory.resetInstantiatorForTests();
        }
    }

    public void testPollSkipsCatalogTableOutsideAllowedRootsAndWarnsOnce() throws Exception {
        // The register call for a catalog type names no root, so the
        // allowlist applies to each table location the catalog returns.
        // A location outside the roots is skipped without touching the
        // client, and its warning fires once, not on every poll.
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.tables = Set.of("orders");
        recording.tableLocations = Map.of("orders", "/forbidden/orders.lance");
        LanceNamespaceFactory.setInstantiatorForTests(type -> recording);
        RecordingNoOpClient recordingClient = new RecordingNoOpClient(threadPool);
        LanceNamespaceService guarded = new LanceNamespaceService(
            recordingClient,
            clusterService,
            threadPool,
            TimeValue.timeValueHours(1),
            1_000_000L,
            TimeValue.timeValueHours(1),
            null,
            new AllowedTableRoots(List.of("/allowed"))
        );
        try {
            LanceNamespaceMetadata metadata = LanceNamespaceMetadata.EMPTY.withRegistered(
                new LanceNamespaceMetadata.Entry(
                    "glue-tokyo",
                    LanceNamespaceMetadata.Entry.TYPE_GLUE,
                    null,
                    StorageOptions.empty(),
                    Map.of("region", "ap-northeast-1")
                )
            );
            ClusterState state = ClusterState.builder(clusterService.state())
                .metadata(Metadata.builder(clusterService.state().metadata()).putCustom(LanceNamespaceMetadata.TYPE, metadata))
                .build();
            ClusterServiceUtils.setState(clusterService, state);

            guarded.poll();
            assertTrue(
                "expected the disallowed-location warning after the first poll",
                guarded.hasWarnedDisallowedLocation("glue-tokyo", "orders")
            );
            assertEquals(1, guarded.disallowedLocationWarningCount());
            assertTrue(
                "no client action may fire for a disallowed location: " + recordingClient.actionNames,
                recordingClient.actionNames.isEmpty()
            );

            guarded.poll();
            assertEquals("the warning fires once, not per poll", 1, guarded.disallowedLocationWarningCount());
            assertTrue(
                "still no client action after the second poll: " + recordingClient.actionNames,
                recordingClient.actionNames.isEmpty()
            );
            // The registration itself stays healthy; only the table is skipped.
            assertEquals(2, recording.describeTableCalls);
        } finally {
            LanceNamespaceFactory.resetInstantiatorForTests();
        }
    }

    public void testUnregisterWaitsForTheListingInFlightBeforeReleasingTheHandle() throws Exception {
        // The applier hands a removed registration's handle to the
        // generic pool for closing while the preview (or the poll) may
        // still be inside listTables on another thread. The release
        // must wait for that call to return; on a real
        // DirectoryNamespace the alternative is a SIGSEGV.
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.tables = Set.of("orders");
        recording.listTablesGate = new CountDownLatch(1);
        LanceNamespaceFactory.setInstantiatorForTests(type -> recording);
        AtomicReference<Optional<Set<String>>> listed = new AtomicReference<>();
        AtomicReference<Throwable> listingFailure = new AtomicReference<>();
        Thread lister = new Thread(() -> {
            try {
                listed.set(service.listTables("cat"));
            } catch (Throwable t) {
                listingFailure.set(t);
            }
        });
        try {
            LanceNamespaceMetadata metadata = LanceNamespaceMetadata.EMPTY.withRegistered(
                new LanceNamespaceMetadata.Entry(
                    "cat",
                    LanceNamespaceMetadata.Entry.TYPE_REST,
                    null,
                    StorageOptions.empty(),
                    Map.of("uri", "http://catalog.example:8080")
                )
            );
            ClusterState registered = ClusterState.builder(clusterService.state())
                .metadata(Metadata.builder(clusterService.state().metadata()).putCustom(LanceNamespaceMetadata.TYPE, metadata))
                .build();
            ClusterServiceUtils.setState(clusterService, registered);
            lister.start();
            assertTrue("listTables never reached the stub", recording.listTablesEntered.await(30, TimeUnit.SECONDS));

            // Unregister while the listing is held open.
            ClusterState unregistered = ClusterState.builder(registered)
                .metadata(Metadata.builder(registered.metadata()).removeCustom(LanceNamespaceMetadata.TYPE))
                .version(registered.version() + 1)
                .build();
            ClusterServiceUtils.setState(clusterService, unregistered);
            // The close task is on the generic pool, parked behind the
            // listing; the stub has not been released.
            assertBusy(() -> assertTrue("expected the close task on the generic pool", genericActiveThreads() >= 1));
            assertEquals(0, recording.closeCalls.get());

            recording.listTablesGate.countDown();
            lister.join(30_000);
            assertNull(listingFailure.get());
            assertEquals(Optional.of(Set.of("orders")), listed.get());
            assertBusy(() -> assertEquals(1, recording.closeCalls.get()));
            assertFalse("the handle was released while listTables was in flight", recording.closedWhileListing);
        } finally {
            recording.listTablesGate.countDown();
            lister.join(30_000);
            LanceNamespaceFactory.resetInstantiatorForTests();
        }
    }

    private long genericActiveThreads() {
        for (ThreadPoolStats.Stats stats : threadPool.stats()) {
            if (ThreadPool.Names.GENERIC.equals(stats.getName())) {
                return stats.getActive();
            }
        }
        return 0;
    }

    /** NoOpClient that records the action names driven through it. */
    private static final class RecordingNoOpClient extends NoOpClient {
        final List<String> actionNames = new CopyOnWriteArrayList<>();

        RecordingNoOpClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            actionNames.add(action.name());
            super.doExecute(action, request, listener);
        }
    }

    public void testTableLocationStripsTrailingSlashAndHandlesMissingLocation() {
        DescribeTableResponse response = new DescribeTableResponse();
        assertNull(LanceCatalogEnumerator.tableLocation(response));
        response.setLocation("s3://bucket/prefix/orders.lance/");
        assertEquals("s3://bucket/prefix/orders.lance", LanceCatalogEnumerator.tableLocation(response));
        response.setLocation("/data/orders.lance");
        assertEquals("/data/orders.lance", LanceCatalogEnumerator.tableLocation(response));
        assertNull(LanceCatalogEnumerator.tableLocation(null));
    }

    public void testMergeStorageOptionsOverlaysEntryValuesOnCatalogValues() {
        StorageOptions entryOptions = StorageOptions.of(Map.of("aws_region", "ap-northeast-1"));
        assertEquals(entryOptions, LanceCatalogEnumerator.mergeStorageOptions(null, entryOptions));
        assertEquals(entryOptions, LanceCatalogEnumerator.mergeStorageOptions(Map.of(), entryOptions));
        StorageOptions merged = LanceCatalogEnumerator.mergeStorageOptions(
            Map.of("aws_region", "us-east-1", "allow_http", "true"),
            entryOptions
        );
        assertEquals("ap-northeast-1", merged.asMap().get("aws_region"));
        assertEquals("true", merged.asMap().get("allow_http"));
    }
}
