/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Collections;
import java.util.List;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.lance.WireVersionTestSupport;
import org.opensearch.lance.dispatch.LanceStatisticsPrefetchNodeResponse.Outcome;
import org.opensearch.test.OpenSearchTestCase;

/** Wire shape of the statistics prefetch transport classes. */
public class LanceStatisticsPrefetchSerializationTests extends OpenSearchTestCase {

    private static DiscoveryNode node(String name) {
        return new DiscoveryNode(
            name,
            name,
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
    }

    public void testRequestRoundTripKeepsTheFieldsAndTheDataNodeSelector() throws Exception {
        LanceStatisticsPrefetchRequest original = new LanceStatisticsPrefetchRequest("demo", "/tables/demo.lance", 7L);
        LanceStatisticsPrefetchRequest restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceStatisticsPrefetchRequest(in);
            }
        }
        assertEquals("demo", restored.indexName());
        assertEquals("/tables/demo.lance", restored.tableUri());
        assertEquals(7L, restored.version());
        assertArrayEquals(new String[] { "data:true" }, restored.nodesIds());
        assertNull(restored.validate());
    }

    public void testNodeRequestStreamOpensWithTheWireVersionAndANewerOptionalBlockIsSteppedOver() throws Exception {
        LanceStatisticsPrefetchNodeRequest original = new LanceStatisticsPrefetchNodeRequest("demo", "s3://bucket/demo.lance", 12L);
        // The marker follows the parent task id the TransportRequest
        // base class writes.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in);
                assertEquals(LanceStatisticsPrefetchNodeRequest.WIRE_VERSION, in.readVInt());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                LanceStatisticsPrefetchNodeRequest restored = new LanceStatisticsPrefetchNodeRequest(in);
                assertEquals("demo", restored.indexName());
                assertEquals("s3://bucket/demo.lance", restored.tableUri());
                assertEquals(12L, restored.version());
            }
        }
        BytesReference newer = WireVersionTestSupport.asNextVersion(
            original,
            o -> TaskId.EMPTY_TASK_ID.writeTo(o),
            false,
            o -> o.writeBoolean(true)
        );
        try (StreamInput in = newer.streamInput()) {
            LanceStatisticsPrefetchNodeRequest restored = new LanceStatisticsPrefetchNodeRequest(in);
            assertEquals(12L, restored.version());
            assertEquals("the reader consumed the block", -1, in.read());
        }
    }

    public void testNodeResponseStreamOpensWithTheWireVersionAndANewerOptionalBlockIsSteppedOver() throws Exception {
        LanceStatisticsPrefetchNodeResponse original = new LanceStatisticsPrefetchNodeResponse(node("node-1"), Outcome.PENDING);
        // The marker follows the node the BaseNodeResponse base class writes.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                new DiscoveryNode(in);
                assertEquals(LanceStatisticsPrefetchNodeResponse.WIRE_VERSION, in.readVInt());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                LanceStatisticsPrefetchNodeResponse restored = new LanceStatisticsPrefetchNodeResponse(in);
                assertEquals(Outcome.PENDING, restored.outcome());
                assertEquals("node-1", restored.getNode().getId());
            }
        }
        BytesReference newer = WireVersionTestSupport.asNextVersion(original, o -> node("node-1").writeTo(o), false, o -> o.writeVInt(3));
        try (StreamInput in = newer.streamInput()) {
            LanceStatisticsPrefetchNodeResponse restored = new LanceStatisticsPrefetchNodeResponse(in);
            assertEquals(Outcome.PENDING, restored.outcome());
            assertEquals("the reader consumed the block", -1, in.read());
        }
    }

    public void testResponseRoundTripCountsTheOutcomes() throws Exception {
        LanceStatisticsPrefetchResponse original = new LanceStatisticsPrefetchResponse(
            new ClusterName("test"),
            List.of(
                new LanceStatisticsPrefetchNodeResponse(node("a"), Outcome.STARTED),
                new LanceStatisticsPrefetchNodeResponse(node("b"), Outcome.STARTED),
                new LanceStatisticsPrefetchNodeResponse(node("c"), Outcome.HELD)
            ),
            List.of()
        );
        LanceStatisticsPrefetchResponse restored;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                restored = new LanceStatisticsPrefetchResponse(in);
            }
        }
        assertEquals(3, restored.getNodes().size());
        assertEquals(2, restored.count(Outcome.STARTED));
        assertEquals(1, restored.count(Outcome.HELD));
        assertEquals(0, restored.count(Outcome.PENDING));
        assertTrue(restored.failures().isEmpty());
    }
}
