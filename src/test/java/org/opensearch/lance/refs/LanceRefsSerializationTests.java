/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.refs;

import java.util.List;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Round-trip serialisation tests for the refs transport classes, so the
 * REST layer and the transport action can evolve independently without
 * breaking the wire shape.
 */
public class LanceRefsSerializationTests extends OpenSearchTestCase {

    public void testRequestRoundTrip() throws Exception {
        LanceRefsRequest original = new LanceRefsRequest("demo");

        LanceRefsRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceRefsRequest(in);
            }
        }

        assertEquals("demo", restored.index());
        assertArrayEquals(new String[] { "demo" }, restored.indices());
        assertNull(restored.validate());
    }

    public void testRequestValidation() {
        assertNotNull(new LanceRefsRequest("").validate());
        assertNotNull(new LanceRefsRequest((String) null).validate());
    }

    public void testResponseRoundTrip() throws Exception {
        LanceRefsResponse original = new LanceRefsResponse(
            "demo",
            "/tmp/demo.lance",
            List.of(new LanceRefsResponse.Tag("v1", 3L), new LanceRefsResponse.Tag("release", 12L)),
            List.of("experiment")
        );

        LanceRefsResponse restored = roundTrip(original);

        assertEquals("demo", restored.index());
        assertEquals("/tmp/demo.lance", restored.table());
        assertEquals(original.tags(), restored.tags());
        assertEquals(List.of("experiment"), restored.branches());
    }

    public void testEmptyResponseRoundTrip() throws Exception {
        LanceRefsResponse original = new LanceRefsResponse("demo", "/tmp/demo.lance", List.of(), List.of());

        LanceRefsResponse restored = roundTrip(original);

        assertTrue(restored.tags().isEmpty());
        assertTrue(restored.branches().isEmpty());
    }

    public void testResponseXContentShape() throws Exception {
        LanceRefsResponse response = new LanceRefsResponse(
            "demo",
            "/tmp/demo.lance",
            List.of(new LanceRefsResponse.Tag("v1", 3L)),
            List.of("experiment")
        );
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(
                "{\"index\":\"demo\",\"table\":\"/tmp/demo.lance\",\"tags\":[{\"name\":\"v1\",\"version\":3}],"
                    + "\"branches\":[{\"name\":\"experiment\"}]}",
                builder.toString()
            );
        }
    }

    private static LanceRefsResponse roundTrip(LanceRefsResponse original) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new LanceRefsResponse(in);
            }
        }
    }
}
