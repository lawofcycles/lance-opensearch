/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.arrow.memory.BufferAllocator;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link LanceNamespaceHandle}: concurrent calls share
 * the handle, a release waits for the calls in flight, and a call that
 * arrives after the release fails before reaching the implementation.
 */
public class LanceNamespaceHandleTests extends OpenSearchTestCase {

    public void testConcurrentCallsShareTheHandle() throws Exception {
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.tables = Set.of("orders");
        // Every caller waits on the gate, so all of them are inside the
        // implementation at the same time before any returns.
        int callers = 8;
        recording.listTablesGate = new CountDownLatch(1);
        LanceNamespaceHandle handle = new LanceNamespaceHandle(recording);
        AtomicInteger answered = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            Thread thread = new Thread(() -> {
                try {
                    handle.call(namespace -> namespace.listTables(new ListTablesRequest()));
                    answered.incrementAndGet();
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            threads.add(thread);
            thread.start();
        }
        try {
            assertTrue(recording.listTablesEntered.await(30, TimeUnit.SECONDS));
            // With readers sharing the lock every caller reaches the stub
            // while the first one is still held open.
            assertBusy(() -> assertEquals(callers, recording.listTablesIds.size()));
        } finally {
            recording.listTablesGate.countDown();
            for (Thread thread : threads) {
                thread.join(30_000);
            }
        }
        assertNull(failure.get());
        assertEquals(callers, answered.get());
    }

    public void testCloseWaitsForTheCallInFlightAndRunsOnce() throws Exception {
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.tables = Set.of("orders");
        recording.listTablesGate = new CountDownLatch(1);
        LanceNamespaceHandle handle = new LanceNamespaceHandle(recording);
        AtomicReference<Throwable> callFailure = new AtomicReference<>();
        Thread caller = new Thread(() -> {
            try {
                handle.call(namespace -> namespace.listTables(new ListTablesRequest()));
            } catch (Throwable t) {
                callFailure.set(t);
            }
        });
        caller.start();
        assertTrue(recording.listTablesEntered.await(30, TimeUnit.SECONDS));

        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer = new Thread(() -> {
            try {
                handle.close();
            } catch (Throwable t) {
                closeFailure.set(t);
            }
        });
        closer.start();
        try {
            // The closer is parked on the write lock until the call
            // returns; its thread is alive and the stub has not been
            // released.
            assertBusy(() -> assertEquals(Thread.State.WAITING, closer.getState()));
            assertEquals(0, recording.closeCalls.get());
            assertFalse(handle.isClosed());
        } finally {
            recording.listTablesGate.countDown();
        }
        caller.join(30_000);
        closer.join(30_000);
        assertNull(callFailure.get());
        assertNull(closeFailure.get());
        assertEquals(1, recording.closeCalls.get());
        assertFalse("close ran while listTables was still in flight", recording.closedWhileListing);
        assertTrue(handle.isClosed());

        // A second close is a no-op, and a call after the release does
        // not reach the implementation.
        handle.close();
        assertEquals(1, recording.closeCalls.get());
        int listingsBefore = recording.listTablesIds.size();
        LanceNamespaceHandle.ReleasedException e = expectThrows(
            LanceNamespaceHandle.ReleasedException.class,
            () -> handle.call(namespace -> namespace.listTables(new ListTablesRequest()))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("released"));
        assertEquals(listingsBefore, recording.listTablesIds.size());
    }

    public void testCallRunsBelowAPrivilegedFrame() throws Exception {
        // The agent's permission check walks the stack up to the nearest
        // doPrivileged frame; the handle has to put that frame between
        // the caller and the catalog implementation so the AWS SDK's
        // credential file reads are judged against the plugin's policy.
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.tables = Set.of("orders");
        LanceNamespaceHandle handle = new LanceNamespaceHandle(recording);
        assertFalse("a direct call must not see the frame", RecordingLanceNamespace.privilegedFrameOnStack());
        ListTablesResponse response = handle.call(namespace -> namespace.listTables(new ListTablesRequest()));
        assertEquals(Set.of("orders"), response.getTables());
        assertTrue("listTables did not run below a doPrivileged frame", recording.listTablesPrivileged);
    }

    public void testCallPropagatesTheImplementationException() {
        // doPrivilegedChecked must pass checked and unchecked failures
        // through unchanged so the service's error handling keeps
        // seeing the catalog's own exception.
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.childNamespaces = Set.of("db1");
        LanceNamespaceHandle handle = new LanceNamespaceHandle(recording);
        IllegalArgumentException unchecked = expectThrows(
            IllegalArgumentException.class,
            () -> handle.call(namespace -> namespace.listTables(new ListTablesRequest()))
        );
        assertTrue(unchecked.getMessage(), unchecked.getMessage().contains("cannot be null or empty"));
        Exception checked = expectThrows(Exception.class, () -> handle.call(namespace -> { throw new Exception("checked failure"); }));
        assertEquals("checked failure", checked.getMessage());
    }

    public void testCloseSkipsImplementationsThatAreNotCloseable() throws Exception {
        // The HTTP client based catalogs do not implement AutoCloseable;
        // the handle still stops accepting calls once released.
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        LanceNamespaceHandle handle = new LanceNamespaceHandle(new NotCloseableNamespace(recording));
        handle.close();
        assertTrue(handle.isClosed());
        assertEquals(0, recording.closeCalls.get());
        expectThrows(
            LanceNamespaceHandle.ReleasedException.class,
            () -> handle.call(namespace -> namespace.listTables(new ListTablesRequest()))
        );
    }

    /** Forwards the listing calls to a recording stub without exposing AutoCloseable. */
    private static final class NotCloseableNamespace implements LanceNamespace {
        private final RecordingLanceNamespace delegate;

        NotCloseableNamespace(RecordingLanceNamespace delegate) {
            this.delegate = delegate;
        }

        @Override
        public void initialize(Map<String, String> properties, BufferAllocator allocator) {
            delegate.initialize(properties, allocator);
        }

        @Override
        public String namespaceId() {
            return delegate.namespaceId();
        }

        @Override
        public ListTablesResponse listTables(ListTablesRequest request) {
            return delegate.listTables(request);
        }
    }
}
