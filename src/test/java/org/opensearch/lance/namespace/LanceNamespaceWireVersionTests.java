/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.WireVersion;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The wire version marker of the namespace messages that travel between
 * nodes: the sync request and response the shard's node answers, and
 * the poll and update requests and responses the cluster manager
 * answers. Each opens with its {@code WIRE_VERSION} right after the
 * fields its OpenSearch base class writes, and refuses a stream of
 * another version by name.
 */
public class LanceNamespaceWireVersionTests extends OpenSearchTestCase {

    public void testSyncRequest() throws Exception {
        LanceIndexSyncRequest original = new LanceIndexSyncRequest("demo");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in);
                in.readOptionalWriteable(ShardId::new);
                assertEquals("demo", in.readOptionalString());
                assertEquals(LanceIndexSyncRequest.WIRE_VERSION, in.readVInt());
                assertEquals("nothing follows the marker", -1, in.read());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals("demo", new LanceIndexSyncRequest(in).index());
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            TaskId.EMPTY_TASK_ID.writeTo(out);
            out.writeOptionalWriteable(null);
            out.writeOptionalString("demo");
            out.writeVInt(LanceIndexSyncRequest.WIRE_VERSION + 1);
            assertRefused("LanceIndexSyncRequest", out, in -> new LanceIndexSyncRequest(in));
        }
    }

    public void testSyncResponse() throws Exception {
        LanceIndexFreshnessService.Outcome outcome = new LanceIndexFreshnessService.Outcome("demo", true, null, true, 3L, 4L, false, false);
        LanceIndexSyncResponse original = new LanceIndexSyncResponse(outcome);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(LanceIndexSyncResponse.WIRE_VERSION, in.readVInt());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(outcome, new LanceIndexSyncResponse(in).outcome());
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(LanceIndexSyncResponse.WIRE_VERSION + 1);
            out.writeString("demo");
            assertRefused("LanceIndexSyncResponse", out, in -> new LanceIndexSyncResponse(in));
        }
    }

    public void testPollRequest() throws Exception {
        LanceNamespacePollRequest original = new LanceNamespacePollRequest("catalog");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in);
                in.readTimeValue();
                assertEquals(LanceNamespacePollRequest.WIRE_VERSION, in.readVInt());
                assertEquals("catalog", in.readOptionalString());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals("catalog", new LanceNamespacePollRequest(in).name());
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            TaskId.EMPTY_TASK_ID.writeTo(out);
            out.writeTimeValue(ClusterManagerNodeRequest.DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT);
            out.writeVInt(LanceNamespacePollRequest.WIRE_VERSION + 1);
            out.writeOptionalString("catalog");
            assertRefused("LanceNamespacePollRequest", out, in -> new LanceNamespacePollRequest(in));
        }
    }

    public void testPollResponse() throws Exception {
        LanceNamespaceService.PollReport report = new LanceNamespaceService.PollReport(
            List.of("demo"),
            List.of(new LanceNamespaceService.PollReport.SkippedTable("ns", "t", "ns_t", "no key")),
            Map.of("broken", "boom")
        );
        LanceNamespacePollResponse original = new LanceNamespacePollResponse(report);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(LanceNamespacePollResponse.WIRE_VERSION, in.readVInt());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(report, new LanceNamespacePollResponse(in).report());
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(LanceNamespacePollResponse.WIRE_VERSION + 1);
            out.writeStringCollection(List.of("demo"));
            assertRefused("LanceNamespacePollResponse", out, in -> new LanceNamespacePollResponse(in));
        }
    }

    public void testUpdateRequest() throws Exception {
        LanceNamespaceUpdateRequest original = LanceNamespaceUpdateRequest.register("/data/root", StorageOptions.empty(), "{}");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in);
                in.readTimeValue();
                assertEquals(LanceNamespaceUpdateRequest.WIRE_VERSION, in.readVInt());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                LanceNamespaceUpdateRequest restored = new LanceNamespaceUpdateRequest(in);
                assertEquals(LanceNamespaceUpdateRequest.Operation.REGISTER, restored.operation());
                assertEquals("/data/root", restored.rootUri());
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            TaskId.EMPTY_TASK_ID.writeTo(out);
            out.writeTimeValue(ClusterManagerNodeRequest.DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT);
            out.writeVInt(LanceNamespaceUpdateRequest.WIRE_VERSION + 1);
            out.writeVInt(LanceNamespaceUpdateRequest.Operation.REGISTER.ordinal());
            assertRefused("LanceNamespaceUpdateRequest", out, in -> new LanceNamespaceUpdateRequest(in));
        }
    }

    public void testUpdateResponse() throws Exception {
        LanceNamespaceUpdateResponse original = new LanceNamespaceUpdateResponse(true, false);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertTrue("the acknowledged bit of the base class comes first", in.readBoolean());
                assertEquals(LanceNamespaceUpdateResponse.WIRE_VERSION, in.readVInt());
                assertFalse(in.readBoolean());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                LanceNamespaceUpdateResponse restored = new LanceNamespaceUpdateResponse(in);
                assertTrue(restored.isAcknowledged());
                assertFalse(restored.changed());
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeBoolean(true);
            out.writeVInt(LanceNamespaceUpdateResponse.WIRE_VERSION + 1);
            out.writeBoolean(false);
            assertRefused("LanceNamespaceUpdateResponse", out, in -> new LanceNamespaceUpdateResponse(in));
        }
    }

    private interface Reader {
        Object read(StreamInput in) throws IOException;
    }

    /** Reads {@code out} with {@code reader} and asserts the refusal names {@code format} and both numbers. */
    private static void assertRefused(String format, BytesStreamOutput out, Reader reader) throws IOException {
        try (StreamInput in = out.bytes().streamInput()) {
            IOException refused = expectThrows(IOException.class, () -> reader.read(in));
            assertEquals(WireVersion.mismatchMessage(format, 2, 1), refused.getMessage());
            assertEquals(
                format + " wire version [2] does not match this node's [1]: every node must run the same plugin version",
                refused.getMessage()
            );
        }
    }
}
