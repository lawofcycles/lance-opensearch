/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
 */
class RecordingLanceNamespace implements LanceNamespace {

    final List<Map<String, String>> initializeCalls = new ArrayList<>();
    RuntimeException initializeFailure;
    java.util.Set<String> tables = java.util.Set.of();
    java.util.Set<String> childNamespaces;
    Map<String, String> tableLocations = Map.of();

    @Override
    public void initialize(Map<String, String> properties, BufferAllocator allocator) {
        initializeCalls.add(Map.copyOf(properties));
        if (initializeFailure != null) {
            throw initializeFailure;
        }
    }

    @Override
    public String namespaceId() {
        return "recording";
    }

    @Override
    public ListTablesResponse listTables(ListTablesRequest request) {
        if (childNamespaces != null && (request.getId() == null || request.getId().isEmpty())) {
            // Mimic a catalog (Glue) that rejects a root table listing
            // because tables live inside child namespaces.
            throw new IllegalArgumentException("Namespace identifier cannot be null or empty");
        }
        return new ListTablesResponse().tables(tables);
    }

    @Override
    public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
        if (childNamespaces == null) {
            return new ListNamespacesResponse().namespaces(java.util.Set.of());
        }
        return new ListNamespacesResponse().namespaces(childNamespaces);
    }

    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest request) {
        String tableName = request.getId().get(request.getId().size() - 1);
        DescribeTableResponse response = new DescribeTableResponse();
        response.setLocation(tableLocations.get(tableName));
        return response;
    }
}
