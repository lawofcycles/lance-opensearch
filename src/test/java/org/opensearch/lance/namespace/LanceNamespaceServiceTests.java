/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * Unit tests for {@link LanceNamespaceService}. The polling loop itself
 * (which opens Lance datasets, derives mappings, and refreshes indexes)
 * needs a live Lance table and lives in an integration test; here we
 * cover the surface that stands on its own: cadence exposure and
 * namespace registration book-keeping.
 *
 * <p>To keep the scheduled poll from firing during the test, the cadence
 * is set well beyond the test lifetime.
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
    private LanceNamespaceService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        client = new NoOpClient(threadPool);
        // A one-hour cadence keeps the scheduled poll from firing during a
        // unit test, so the tests only see the state we drive explicitly.
        service = new LanceNamespaceService(client, threadPool, TimeValue.timeValueHours(1), 1_000_000L);
    }

    @Override
    public void tearDown() throws Exception {
        client.close();
        ThreadPool.terminate(threadPool, 30L, java.util.concurrent.TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testCadenceReturnsConstructorValue() {
        assertEquals(TimeValue.timeValueHours(1), service.cadence());
    }

    public void testNamespacesStartsEmpty() {
        assertTrue("expected empty namespaces list, saw: " + service.namespaces(), service.namespaces().isEmpty());
    }

    public void testRegisterAddsUri() {
        // DirectoryNamespace.initialize does not eagerly stat the path, so a
        // register() call adds the URI to the namespace list even for a
        // directory that does not exist yet. Listing tables later would fail,
        // which is fine for the surface this test is covering.
        String path = "/nonexistent-" + randomAlphaOfLength(8);
        service.register(path);
        assertEquals(java.util.List.of(path), service.namespaces());
    }

    public void testRegisterIsIdempotent() {
        // Registering the same URI twice must not duplicate the entry, since
        // the polling loop would otherwise scan the same catalog twice.
        String path = "/duplicate-" + randomAlphaOfLength(8);
        service.register(path);
        service.register(path);
        assertEquals(java.util.List.of(path), service.namespaces());
    }

    public void testNamespacesReturnsUnmodifiableSnapshot() {
        service.register("/read-only-" + randomAlphaOfLength(8));
        java.util.List<String> before = service.namespaces();
        // The list returned by namespaces() is a copy of the internal state;
        // attempting to mutate it (or the internal state through it) must not
        // touch the service.
        expectThrows(UnsupportedOperationException.class, () -> before.add("/injected"));
        assertEquals(1, service.namespaces().size());
    }
}
