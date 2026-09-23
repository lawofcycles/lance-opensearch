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
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.StorageOptions;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Round-trip serialisation tests for the attach transport classes, so
 * the REST layer and the transport action can evolve independently
 * without breaking the wire shape.
 */
public class LanceAttachSerializationTests extends OpenSearchTestCase {

    public void testRequestRoundTrip() throws Exception {
        LinkedHashMap<String, Object> overridesBody = new LinkedHashMap<>();
        overridesBody.put("ts", Map.of("type", "date", "format", "epoch_millis"));
        overridesBody.put("body", Map.of("type", "keyword", "fields", Map.of("raw", Map.of("type", "keyword"))));
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(
            overridesBody,
            Map.of("title", Map.of("exact", Map.of("type", "keyword")))
        );
        LanceAttachRequest original = new LanceAttachRequest(
            "s3://bucket/tables/demo.lance",
            "demo-index",
            7L,
            null,
            StorageOptions.of(Map.of("aws_region", "eu-west-1")),
            overrides
        );
        original.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(75));

        LanceAttachRequest restored = roundTrip(original);

        assertEquals(original.table(), restored.table());
        assertEquals(original.indexName(), restored.indexName());
        assertEquals(original.pinnedVersion(), restored.pinnedVersion());
        assertTrue(restored.tag().isEmpty());
        assertEquals(original.storageOptions().asMap(), restored.storageOptions().asMap());
        assertEquals(original.overrides(), restored.overrides());
        // The request is forwarded to the cluster manager, so the
        // manager-node timeout has to travel with it.
        assertEquals(TimeValue.timeValueSeconds(75), restored.clusterManagerNodeTimeout());
        // Declaration order drives the order of the emitted mapping, so
        // it has to survive the wire as declared.
        assertEquals(List.of("ts", "body", "title"), List.copyOf(restored.overrides().columns().keySet()));
        assertEquals("date", restored.overrides().columns().get("ts").type());
        assertEquals("epoch_millis", restored.overrides().columns().get("ts").format());
        assertEquals(List.of("raw"), List.copyOf(restored.overrides().subFields().get("body").keySet()));
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
        assertTrue(restored.overrides().isEmpty());
        assertFalse(restored.asyncDerive());
        assertEquals(ClusterManagerNodeRequest.DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT, restored.clusterManagerNodeTimeout());
        assertNull(restored.validate());
    }

    public void testRequestWithTextAnalyzerAndAsyncDeriveRoundTrip() throws Exception {
        LanceOverrides overrides = LanceOverrides.parseAttachClauses(
            Map.of("body", Map.of("type", "text_analyzer", "analyzer", "english", "derived_column_name", "body_tokens")),
            null
        );
        LanceAttachRequest original = new LanceAttachRequest("/tmp/demo.lance", null, null, null, null, overrides, null, true);

        LanceAttachRequest restored = roundTrip(original);

        assertTrue(restored.asyncDerive());
        LanceOverrides.Column column = restored.overrides().textAnalyzerColumns().get("body");
        assertEquals("english", column.analyzer());
        assertEquals("body_tokens", column.derivedColumn());
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
