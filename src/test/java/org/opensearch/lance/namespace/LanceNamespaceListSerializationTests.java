/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.List;
import java.util.Map;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
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
        LanceNamespaceListResponse.NamespaceInfo directory = new LanceNamespaceListResponse.NamespaceInfo(
            "/data/a",
            LanceNamespaceMetadata.Entry.TYPE_DIRECTORY,
            "/data/a",
            Map.of(),
            null
        );
        LanceNamespaceListResponse.NamespaceInfo unavailableRest = new LanceNamespaceListResponse.NamespaceInfo(
            "cat",
            LanceNamespaceMetadata.Entry.TYPE_REST,
            null,
            Map.of("uri", "http://catalog.example:8080", "header.Authorization", "***"),
            "connection refused"
        );
        LanceNamespaceListResponse original = LanceNamespaceListResponse.namespaces(List.of(directory, unavailableRest));
        LanceNamespaceListResponse restored = roundTrip(original);
        assertEquals(List.of(directory, unavailableRest), restored.namespaces());
        assertNull(restored.name());
        assertNull(restored.tables());
        assertEquals(RestStatus.OK, restored.status());
    }

    public void testNamespacesResponseXContentCarriesStatusAndError() throws Exception {
        LanceNamespaceListResponse response = LanceNamespaceListResponse.namespaces(
            List.of(
                new LanceNamespaceListResponse.NamespaceInfo("/data/a", "directory", "/data/a", Map.of(), null),
                new LanceNamespaceListResponse.NamespaceInfo("cat", "rest", null, Map.of("uri", "http://x"), "connection refused")
            )
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        assertTrue(json, json.contains("\"status\":\"available\""));
        assertTrue(json, json.contains("\"status\":\"unavailable\""));
        assertTrue(json, json.contains("\"error\":\"connection refused\""));
        assertTrue(json, json.contains("\"path\":\"/data/a\""));
    }

    public void testTablesResponseRoundTrip() throws Exception {
        LanceNamespaceListResponse original = LanceNamespaceListResponse.tables("/data/a", List.of("alpha", "bravo"));
        LanceNamespaceListResponse restored = roundTrip(original);
        assertNull(restored.namespaces());
        assertEquals("/data/a", restored.name());
        assertTrue(restored.registered());
        assertEquals(List.of("alpha", "bravo"), restored.tables());
        assertEquals(RestStatus.OK, restored.status());
    }

    public void testUnregisteredResponseRoundTrip() throws Exception {
        LanceNamespaceListResponse original = LanceNamespaceListResponse.unregistered("/data/missing");
        LanceNamespaceListResponse restored = roundTrip(original);
        assertNull(restored.namespaces());
        assertEquals("/data/missing", restored.name());
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
