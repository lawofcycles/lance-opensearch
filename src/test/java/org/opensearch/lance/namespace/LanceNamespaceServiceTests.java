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
import java.util.concurrent.TimeUnit;

import org.lance.namespace.model.DescribeTableResponse;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.cluster.ClusterState;
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

/**
 * Unit tests for {@link LanceNamespaceService}. Registration state
 * lives in cluster-state metadata routed through a
 * {@code TransportClusterManagerNodeAction}, so
 * {@link LanceNamespaceService#register} cannot be exercised without a
 * live transport stack; the tests here cover the surface that stands
 * on its own: cadence exposure, the metadata-backed
 * {@link LanceNamespaceService#namespaces} reader, and the
 * settings-only classification the poll uses to adopt indexes it does
 * not track yet.
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

    public void testClassifyForAdoptionSkipsIndexWithoutLanceTable() {
        Settings plain = Settings.builder().put("index.number_of_shards", 1).build();
        assertEquals(LanceNamespaceService.Adoption.NOT_LANCE, LanceNamespaceService.classifyForAdoption("plain", plain, Set.of("/ns")));
        Settings emptyTable = Settings.builder().put(LanceEngineFactory.TABLE_SETTING, "").build();
        assertEquals(
            LanceNamespaceService.Adoption.NOT_LANCE,
            LanceNamespaceService.classifyForAdoption("plain", emptyTable, Set.of("/ns"))
        );
    }

    public void testClassifyForAdoptionSkipsPinnedIndex() {
        // A pinned index is a readonly snapshot and stays outside the
        // poll even when its table sits under a registered namespace.
        Settings pinned = Settings.builder()
            .put(LanceEngineFactory.TABLE_SETTING, "/ns/demo.lance")
            .put(LanceEngineFactory.VERSION_SETTING, randomIntBetween(0, 100))
            .build();
        assertEquals(LanceNamespaceService.Adoption.PINNED, LanceNamespaceService.classifyForAdoption("demo", pinned, Set.of("/ns")));
    }

    public void testClassifyForAdoptionRecognisesNamespaceSurfacedIndex() {
        // The surface step builds the table path as root + "/" + name +
        // ".lance", so that exact shape under a registered root is a
        // namespace index. An explicit -1 version (the follow-latest
        // default) does not count as a pin.
        Settings surfaced = Settings.builder()
            .put(LanceEngineFactory.TABLE_SETTING, "/ns/demo.lance")
            .put(LanceEngineFactory.VERSION_SETTING, -1L)
            .build();
        assertEquals(
            LanceNamespaceService.Adoption.NAMESPACE,
            LanceNamespaceService.classifyForAdoption("demo", surfaced, Set.of("/other", "/ns"))
        );
    }

    public void testClassifyForAdoptionTreatsOtherLanceIndexesAsAttached() {
        Settings attached = Settings.builder().put(LanceEngineFactory.TABLE_SETTING, "/elsewhere/demo.lance").build();
        // Not under any registered root.
        assertEquals(LanceNamespaceService.Adoption.ATTACH, LanceNamespaceService.classifyForAdoption("demo", attached, Set.of("/ns")));
        // No namespace registered at all.
        assertEquals(LanceNamespaceService.Adoption.ATTACH, LanceNamespaceService.classifyForAdoption("demo", attached, Set.of()));
        // Under a registered root but attached under a different index
        // name, so the namespace loop would never sync it by that name.
        Settings renamed = Settings.builder().put(LanceEngineFactory.TABLE_SETTING, "/ns/demo.lance").build();
        assertEquals(LanceNamespaceService.Adoption.ATTACH, LanceNamespaceService.classifyForAdoption("alias", renamed, Set.of("/ns")));
        // A tag does not change the classification; it is carried into
        // the attach bookkeeping by the caller.
        Settings tagged = Settings.builder()
            .put(LanceEngineFactory.TABLE_SETTING, "/elsewhere/demo.lance")
            .put(LanceEngineFactory.TAG_SETTING, "release")
            .build();
        assertEquals(LanceNamespaceService.Adoption.ATTACH, LanceNamespaceService.classifyForAdoption("demo", tagged, Set.of("/ns")));
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
            // The applier callback drives ensureHandle; the failure is
            // recorded rather than thrown.
            ClusterServiceUtils.setState(clusterService, state);
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

            // A later applier tick retries; success clears the status.
            recording.initializeFailure = null;
            ClusterState touched = ClusterState.builder(clusterService.state())
                .metadata(Metadata.builder(clusterService.state().metadata()).putCustom(LanceNamespaceMetadata.TYPE, metadata))
                .version(clusterService.state().version() + 1)
                .build();
            ClusterServiceUtils.setState(clusterService, touched);
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
        assertNull(LanceNamespaceService.tableLocation(response));
        response.setLocation("s3://bucket/prefix/orders.lance/");
        assertEquals("s3://bucket/prefix/orders.lance", LanceNamespaceService.tableLocation(response));
        response.setLocation("/data/orders.lance");
        assertEquals("/data/orders.lance", LanceNamespaceService.tableLocation(response));
        assertNull(LanceNamespaceService.tableLocation(null));
    }

    public void testMergeStorageOptionsOverlaysEntryValuesOnCatalogValues() {
        StorageOptions entryOptions = StorageOptions.of(Map.of("aws_region", "ap-northeast-1"));
        assertEquals(entryOptions, LanceNamespaceService.mergeStorageOptions(null, entryOptions));
        assertEquals(entryOptions, LanceNamespaceService.mergeStorageOptions(Map.of(), entryOptions));
        StorageOptions merged = LanceNamespaceService.mergeStorageOptions(
            Map.of("aws_region", "us-east-1", "allow_http", "true"),
            entryOptions
        );
        assertEquals("ap-northeast-1", merged.asMap().get("aws_region"));
        assertEquals("true", merged.asMap().get("allow_http"));
    }
}
