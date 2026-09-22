/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Pins the wire layout of {@code InternalOrder.writeTo} that the
 * translator's sub aggregation order decoding reads byte by byte (the
 * order classes expose neither the direction nor a compound's
 * elements). If an OpenSearch upgrade changes the layout, these tests
 * fail instead of the decoder silently refusing every metric ordered
 * terms at runtime: a compound order is -1 followed by the element
 * count, an aggregation order is 0 followed by the direction and the
 * path, and the {@code _key} ascending tie breaker is 4.
 */
public class BucketOrderWireFormatTests extends OpenSearchTestCase {

    private static StreamInput wireOf(BucketOrder order) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            order.writeTo(out);
            return out.bytes().streamInput();
        }
    }

    public void testAggregationOrderWritesIdDirectionAndPath() throws IOException {
        try (StreamInput in = wireOf(BucketOrder.aggregation("m", true))) {
            assertEquals("an aggregation order's id byte", 0, in.readByte());
            assertTrue("the direction boolean", in.readBoolean());
            assertEquals("the path", "m", in.readString());
            assertEquals("nothing follows the path", 0, in.available());
        }
    }

    public void testKeyAscendingWritesItsIdOnly() throws IOException {
        try (StreamInput in = wireOf(BucketOrder.key(true))) {
            assertEquals("the _key ascending id byte", 4, in.readByte());
            assertEquals("a simple order is its id alone", 0, in.available());
        }
    }

    public void testTermsBuilderWrapsAMetricOrderInACompoundWithTheKeyTieBreaker() throws IOException {
        TermsAggregationBuilder terms = new TermsAggregationBuilder("t").field("f").order(BucketOrder.aggregation("m", false));
        try (StreamInput in = wireOf(terms.order())) {
            assertEquals("a compound order's id byte", -1, in.readByte());
            assertEquals("the element count", 2, in.readVInt());
            assertEquals("the metric order's id byte", 0, in.readByte());
            assertFalse("the requested descending direction", in.readBoolean());
            assertEquals("the requested path", "m", in.readString());
            assertEquals("the trailing _key ascending tie breaker", 4, in.readByte());
            assertEquals("nothing follows the tie breaker", 0, in.available());
        }
    }
}
