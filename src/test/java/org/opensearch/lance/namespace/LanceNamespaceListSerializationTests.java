/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.List;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Round-trip serialisation tests for the namespace listing transport
 * classes, so the REST layer and the transport action can evolve
 * independently without breaking the wire shape.
 */
public class LanceNamespaceListSerializationTests extends OpenSearchTestCase {

    public void testNamespacesRequestRoundTrip() throws Exception {
        LanceNamespaceListRequest original = LanceNamespaceListRequest.namespaces();
        LanceNamespaceListRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceNamespaceListRequest(in);
            }
        }
        assertNull(restored.path());
        assertNull(restored.validate());
    }

    public void testTablesRequestRoundTrip() throws Exception {
        LanceNamespaceListRequest original = LanceNamespaceListRequest.tables("s3://bucket/root");
        LanceNamespaceListRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceNamespaceListRequest(in);
            }
        }
        assertEquals("s3://bucket/root", restored.path());
        assertNull(restored.validate());
    }

    public void testEmptyPathFailsValidation() {
        assertNotNull(LanceNamespaceListRequest.tables("").validate());
    }

    public void testNamespacesResponseRoundTrip() throws Exception {
        LanceNamespaceListResponse original = LanceNamespaceListResponse.namespaces(List.of("/data/a", "/data/b"));
        LanceNamespaceListResponse restored = roundTrip(original);
        assertEquals(List.of("/data/a", "/data/b"), restored.namespaces());
        assertNull(restored.path());
        assertNull(restored.tables());
        assertEquals(RestStatus.OK, restored.status());
    }

    public void testTablesResponseRoundTrip() throws Exception {
        LanceNamespaceListResponse original = LanceNamespaceListResponse.tables("/data/a", List.of("alpha", "bravo"));
        LanceNamespaceListResponse restored = roundTrip(original);
        assertNull(restored.namespaces());
        assertEquals("/data/a", restored.path());
        assertTrue(restored.registered());
        assertEquals(List.of("alpha", "bravo"), restored.tables());
        assertEquals(RestStatus.OK, restored.status());
    }

    public void testUnregisteredResponseRoundTrip() throws Exception {
        LanceNamespaceListResponse original = LanceNamespaceListResponse.unregistered("/data/missing");
        LanceNamespaceListResponse restored = roundTrip(original);
        assertNull(restored.namespaces());
        assertEquals("/data/missing", restored.path());
        assertFalse(restored.registered());
        assertNull(restored.tables());
        assertEquals(RestStatus.NOT_FOUND, restored.status());
    }

    private static LanceNamespaceListResponse roundTrip(LanceNamespaceListResponse original) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new LanceNamespaceListResponse(in);
            }
        }
    }
}
