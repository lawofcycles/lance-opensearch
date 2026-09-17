/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.util.List;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.lance.StorageOptions;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * Unit tests for {@link LanceNamespaceService}. Milestone 5-D1
 * migrated the service's registration state from a per-node
 * {@code CopyOnWriteArrayList} to cluster-state metadata routed
 * through a {@code TransportClusterManagerNodeAction}, so calls to
 * {@link LanceNamespaceService#register} in isolation no longer
 * mutate state without a live transport stack. The tests here
 * cover the surface that stands on its own: cadence exposure and
 * the metadata-backed {@link LanceNamespaceService#namespaces}
 * reader.
 *
 * <p>End-to-end register / unregister behaviour is exercised in
 * {@code LancePluginIT} where the plugin ships with a real
 * transport stack and REST endpoints.
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
        ThreadPool.terminate(threadPool, 30L, java.util.concurrent.TimeUnit.SECONDS);
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
}
