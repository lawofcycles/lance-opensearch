/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;

/**
 * Test stub for {@link LanceNamespace}. Records the properties
 * {@code initialize} receives so tests can assert the factory passes
 * the registration config through unredacted, and answers the listing
 * calls from canned data. An optional failure injected into
 * {@code initialize} exercises the service's unavailable handling.
 *
 * <p>Two listing modes. The flat mode ({@link #tables},
 * {@link #childNamespaces}) mimics a Glue-style catalog: any non-empty
 * id answers the same table set, a root listing is rejected when child
 * namespaces exist. The tree mode ({@link #namespaceTree},
 * {@link #tableTree}) pins the namespace walk: {@code listNamespaces}
 * answers the children recorded for the exact parent id,
 * {@code listTables} answers only ids present in the table tree and
 * rejects every other id the way Iceberg and Unity reject listings at
 * the wrong level.
 *
 * <p>{@link #listTablesGate}, when set, holds every {@code listTables}
 * call open until the test counts it down, and {@link #listTablesEntered}
 * reports that a call has reached that point. Together with
 * {@link #closeCalls} and {@link #closedWhileListing} they let a test pin
 * the order of an in-flight listing and the handle's release.
 *
 * <p>{@link #initializePrivileged} and {@link #listTablesPrivileged}
 * record whether a {@code doPrivileged} frame of the agent's
 * {@code AccessController} was on the stack when the call arrived, the
 * frame the Java agent stops its protection domain walk at.
 */
class RecordingLanceNamespace implements LanceNamespace, AutoCloseable {

    private static final String ACCESS_CONTROLLER = "org.opensearch.secure_sm.AccessController";

    final List<Map<String, String>> initializeCalls = new ArrayList<>();
    /** Whether the last {@code initialize} ran below a {@code doPrivileged} frame. */
    boolean initializePrivileged;
    /** Whether the last {@code listTables} ran below a {@code doPrivileged} frame. */
    volatile boolean listTablesPrivileged;
    RuntimeException initializeFailure;
    Set<String> tables = Set.of();
    Set<String> childNamespaces;
    Map<String, String> tableLocations = Map.of();
    int describeTableCalls;
    final AtomicInteger closeCalls = new AtomicInteger();
    /** Set to true if {@link #close} ran while a {@code listTables} call was still in progress. */
    volatile boolean closedWhileListing;
    /** Every {@code listTables} call waits on this latch when it is non-null. */
    volatile CountDownLatch listTablesGate;
    /** Counted down once a {@code listTables} call has started waiting on {@link #listTablesGate}. */
    final CountDownLatch listTablesEntered = new CountDownLatch(1);
    private final AtomicInteger listingsInFlight = new AtomicInteger();

    /** Tree mode: children per exact parent id (dot-joined; the root is the empty string). */
    Map<String, Set<String>> namespaceTree;
    /** Tree mode: tables per exact namespace id (dot-joined); other ids reject the listing. */
    Map<String, Set<String>> tableTree;
    final List<List<String>> listTablesIds = new CopyOnWriteArrayList<>();
    final List<List<String>> listNamespacesIds = new CopyOnWriteArrayList<>();

    /**
     * Whether the calling thread's stack holds the frame the agent's
     * {@code StackCallerProtectionDomainChainExtractor} cuts at: a
     * {@code doPrivileged} or {@code doPrivilegedChecked} method of
     * {@code org.opensearch.secure_sm.AccessController}.
     */
    static boolean privilegedFrameOnStack() {
        return StackWalker.getInstance()
            .walk(
                frames -> frames.anyMatch(
                    frame -> ACCESS_CONTROLLER.equals(frame.getClassName()) && frame.getMethodName().startsWith("doPrivileged")
                )
            );
    }

    @Override
    public void initialize(Map<String, String> properties, BufferAllocator allocator) {
        initializeCalls.add(Map.copyOf(properties));
        initializePrivileged = privilegedFrameOnStack();
        if (initializeFailure != null) {
            throw initializeFailure;
        }
    }

    @Override
    public String namespaceId() {
        return "recording";
    }

    private static String joined(List<String> id) {
        return id == null ? "" : String.join(".", id);
    }

    @Override
    public ListTablesResponse listTables(ListTablesRequest request) {
        listTablesIds.add(request.getId() == null ? List.of() : List.copyOf(request.getId()));
        listTablesPrivileged = privilegedFrameOnStack();
        listingsInFlight.incrementAndGet();
        try {
            CountDownLatch gate = listTablesGate;
            if (gate != null) {
                listTablesEntered.countDown();
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while holding listTables open", e);
                }
            }
            return answerListTables(request);
        } finally {
            listingsInFlight.decrementAndGet();
        }
    }

    private ListTablesResponse answerListTables(ListTablesRequest request) {
        if (tableTree != null) {
            Set<String> found = tableTree.get(joined(request.getId()));
            if (found == null) {
                throw new IllegalArgumentException("no tables at namespace [" + joined(request.getId()) + "]");
            }
            return new ListTablesResponse().tables(found);
        }
        if (childNamespaces != null && (request.getId() == null || request.getId().isEmpty())) {
            // Mimic a catalog (Glue) that rejects a root table listing
            // because tables live inside child namespaces.
            throw new IllegalArgumentException("Namespace identifier cannot be null or empty");
        }
        return new ListTablesResponse().tables(tables);
    }

    @Override
    public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
        listNamespacesIds.add(request.getId() == null ? List.of() : List.copyOf(request.getId()));
        if (namespaceTree != null) {
            Set<String> children = namespaceTree.get(joined(request.getId()));
            return new ListNamespacesResponse().namespaces(children == null ? Set.of() : children);
        }
        if (childNamespaces == null) {
            return new ListNamespacesResponse().namespaces(Set.of());
        }
        return new ListNamespacesResponse().namespaces(childNamespaces);
    }

    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest request) {
        describeTableCalls++;
        String tableName = request.getId().get(request.getId().size() - 1);
        DescribeTableResponse response = new DescribeTableResponse();
        response.setLocation(tableLocations.get(tableName));
        return response;
    }

    @Override
    public void close() {
        if (listingsInFlight.get() > 0) {
            closedWhileListing = true;
        }
        closeCalls.incrementAndGet();
    }
}
