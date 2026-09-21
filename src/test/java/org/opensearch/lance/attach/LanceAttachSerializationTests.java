/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
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
            null,
            StorageOptions.of(Map.of("aws_region", "eu-west-1")),
            multiFields
        );
        original.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(75));

        LanceAttachRequest restored = roundTrip(original);

        assertEquals(original.table(), restored.table());
        assertEquals(original.indexName(), restored.indexName());
        assertEquals(original.pinnedVersion(), restored.pinnedVersion());
        assertTrue(restored.tag().isEmpty());
        assertEquals(original.storageOptions().asMap(), restored.storageOptions().asMap());
        assertEquals(original.multiFields(), restored.multiFields());
        // The request is forwarded to the cluster manager, so the
        // manager-node timeout has to travel with it.
        assertEquals(TimeValue.timeValueSeconds(75), restored.clusterManagerNodeTimeout());
        // Sub-field order drives the order of the emitted mapping, so it
        // has to survive the wire as declared.
        assertEquals(List.of("body", "title"), List.copyOf(restored.multiFields().keySet()));
        assertEquals(List.of("raw", "alt"), List.copyOf(restored.multiFields().get("body").keySet()));
        assertNull(restored.validate());
    }

    public void testRequestWithDefaultsRoundTrip() throws Exception {
        LanceAttachRequest original = new LanceAttachRequest("/tmp/demo.lance", null, null, null, null, null);

        LanceAttachRequest restored = roundTrip(original);

        assertEquals("/tmp/demo.lance", restored.table());
        assertNull(restored.indexName());
        assertTrue(restored.pinnedVersion().isEmpty());
        assertTrue(restored.tag().isEmpty());
        assertTrue(restored.storageOptions().isEmpty());
        assertTrue(restored.multiFields().isEmpty());
        assertEquals(ClusterManagerNodeRequest.DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT, restored.clusterManagerNodeTimeout());
        assertNull(restored.validate());
    }

    public void testRequestWithTagRoundTrip() throws Exception {
        LanceAttachRequest original = new LanceAttachRequest("/tmp/demo.lance", "demo", null, "release-2026-09", null, null);

        LanceAttachRequest restored = roundTrip(original);

        assertEquals("release-2026-09", restored.tag().orElseThrow());
        assertTrue(restored.pinnedVersion().isEmpty());
        assertNull(restored.validate());
    }

    public void testRequestValidation() {
        assertNotNull(new LanceAttachRequest("", null, null, null, null, null).validate());
        assertNotNull(new LanceAttachRequest("/tmp/demo.lance", null, -1L, null, null, null).validate());
        // An empty tag name cannot resolve to anything.
        assertNotNull(new LanceAttachRequest("/tmp/demo.lance", null, null, "", null, null).validate());
        // A fixed version pin and a moving tag cannot both be honoured.
        ActionRequestValidationException both = new LanceAttachRequest("/tmp/demo.lance", null, 3L, "v1", null, null).validate();
        assertNotNull(both);
        assertTrue(both.getMessage(), both.getMessage().contains("[version] and [tag] are mutually exclusive"));
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
            true,
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
        assertEquals(original.luceneBoundExceeded(), restored.luceneBoundExceeded());
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
