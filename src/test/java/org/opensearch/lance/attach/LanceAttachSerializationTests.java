/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.lance.StorageOptions;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Round-trip serialisation tests for the attach transport classes, so
 * the REST layer and the transport action can evolve independently
 * without breaking the wire shape.
 */
public class LanceAttachSerializationTests extends OpenSearchTestCase {

    public void testRequestRoundTrip() throws Exception {
        LinkedHashMap<String, String> bodySubs = new LinkedHashMap<>();
        bodySubs.put("raw", "keyword");
        bodySubs.put("alt", "keyword");
        LinkedHashMap<String, String> titleSubs = new LinkedHashMap<>();
        titleSubs.put("exact", "keyword");
        LinkedHashMap<String, LinkedHashMap<String, String>> multiFields = new LinkedHashMap<>();
        multiFields.put("body", bodySubs);
        multiFields.put("title", titleSubs);
        LanceAttachRequest original = new LanceAttachRequest(
            "s3://bucket/tables/demo.lance",
            "demo-index",
            7L,
            StorageOptions.of(Map.of("aws_region", "eu-west-1")),
            multiFields
        );

        LanceAttachRequest restored = roundTrip(original);

        assertEquals(original.table(), restored.table());
        assertEquals(original.indexName(), restored.indexName());
        assertEquals(original.pinnedVersion(), restored.pinnedVersion());
        assertEquals(original.storageOptions().asMap(), restored.storageOptions().asMap());
        assertEquals(original.multiFields(), restored.multiFields());
        // Sub-field order drives the order of the emitted mapping, so it
        // has to survive the wire as declared.
        assertEquals(List.of("body", "title"), List.copyOf(restored.multiFields().keySet()));
        assertEquals(List.of("raw", "alt"), List.copyOf(restored.multiFields().get("body").keySet()));
        assertNull(restored.validate());
    }

    public void testRequestWithDefaultsRoundTrip() throws Exception {
        LanceAttachRequest original = new LanceAttachRequest("/tmp/demo.lance", null, null, null, null);

        LanceAttachRequest restored = roundTrip(original);

        assertEquals("/tmp/demo.lance", restored.table());
        assertNull(restored.indexName());
        assertTrue(restored.pinnedVersion().isEmpty());
        assertTrue(restored.storageOptions().isEmpty());
        assertTrue(restored.multiFields().isEmpty());
        assertNull(restored.validate());
    }

    public void testRequestValidation() {
        assertNotNull(new LanceAttachRequest("", null, null, null, null).validate());
        assertNotNull(new LanceAttachRequest("/tmp/demo.lance", null, -1L, null, null).validate());
    }

    public void testResponseRoundTrip() throws Exception {
        LanceAttachResponse original = new LanceAttachResponse(
            "demo",
            "/tmp/demo.lance",
            3L,
            1000L,
            4,
            "id",
            "{\"properties\":{\"id\":{\"type\":\"long\"}}}",
            List.of("column vec: stored only"),
            true
        );

        LanceAttachResponse restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceAttachResponse(in);
            }
        }

        assertEquals(original.index(), restored.index());
        assertEquals(original.table(), restored.table());
        assertEquals(original.version(), restored.version());
        assertEquals(original.rows(), restored.rows());
        assertEquals(original.fragments(), restored.fragments());
        assertEquals(original.derivedKeyField(), restored.derivedKeyField());
        assertEquals(original.derivedMappingJson(), restored.derivedMappingJson());
        assertEquals(original.notes(), restored.notes());
        assertEquals(original.alreadyAttached(), restored.alreadyAttached());
    }

    private static LanceAttachRequest roundTrip(LanceAttachRequest original) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new LanceAttachRequest(in);
            }
        }
    }
}
