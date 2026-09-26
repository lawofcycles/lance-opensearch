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
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.WireVersion;
import org.opensearch.lance.WireVersionTestSupport;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The wire version marker of the namespace messages that travel between
 * nodes: the sync request and response the shard's node answers, and
 * the poll and update requests and responses the cluster manager
 * answers. Each opens with its {@code WIRE_VERSION} right after the
 * fields its OpenSearch base class writes, steps over an optional block
 * of a version it does not know, and refuses a critical one by name.
 */
public class LanceNamespaceWireVersionTests extends OpenSearchTestCase {

    /** What a {@link ClusterManagerNodeRequest} writes before the marker: the parent task id and its own timeout. */
    private static Writeable clusterManagerPrelude(ClusterManagerNodeRequest<?> request) {
        return out -> {
            TaskId.EMPTY_TASK_ID.writeTo(out);
            out.writeTimeValue(request.clusterManagerNodeTimeout());
        };
    }

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
        Writeable prelude = out -> {
            TaskId.EMPTY_TASK_ID.writeTo(out);
            out.writeOptionalWriteable(null);
            out.writeOptionalString("demo");
        };
        assertEquals("demo", ((LanceIndexSyncRequest) readNextVersion(original, prelude, LanceIndexSyncRequest::new)).index());
        assertRefusesCritical("LanceIndexSyncRequest", original, prelude, LanceIndexSyncRequest::new);
    }

    public void testSyncResponse() throws Exception {
        LanceIndexFreshnessService.Outcome outcome = new LanceIndexFreshnessService.Outcome(
            "demo",
            true,
            null,
            true,
            3L,
            4L,
            true,
            false,
            "Mapper for [body] conflicts with existing mapper"
        );
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
        Writeable prelude = WireVersionTestSupport.NO_PRELUDE;
        assertEquals(outcome, ((LanceIndexSyncResponse) readNextVersion(original, prelude, LanceIndexSyncResponse::new)).outcome());
        assertRefusesCritical(
            "LanceIndexSyncResponse",
            LanceIndexSyncResponse.WIRE_VERSION,
            original,
            prelude,
            LanceIndexSyncResponse::new
        );
    }

    public void testSyncResponseOfAVersion1NodeCarriesNoMappingError() throws Exception {
        // The stream a node of the previous plugin version writes: the
        // base fields with marker 1 and no block.
        LanceIndexFreshnessService.Outcome outcome = new LanceIndexFreshnessService.Outcome("demo", true, null, true, 3L, 4L, true, false);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            WireVersion.write(out, 1);
            out.writeString(outcome.index());
            out.writeBoolean(outcome.checked());
            out.writeOptionalString(outcome.reason());
            out.writeBoolean(outcome.moved());
            out.writeLong(outcome.servedVersion());
            out.writeLong(outcome.targetVersion());
            out.writeBoolean(outcome.mappingChanged());
            out.writeBoolean(outcome.rebuilt());
            try (StreamInput in = out.bytes().streamInput()) {
                LanceIndexSyncResponse restored = new LanceIndexSyncResponse(in);
                assertEquals(outcome, restored.outcome());
                assertNull(restored.outcome().mappingError());
                assertEquals(-1, in.read());
            }
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
        Writeable prelude = clusterManagerPrelude(original);
        assertEquals("catalog", ((LanceNamespacePollRequest) readNextVersion(original, prelude, LanceNamespacePollRequest::new)).name());
        assertRefusesCritical("LanceNamespacePollRequest", original, prelude, LanceNamespacePollRequest::new);
    }

    public void testPollResponse() throws Exception {
        LanceNamespaceService.PollReport report = new LanceNamespaceService.PollReport(
            List.of("demo"),
            List.of(new LanceNamespaceService.PollReport.SkippedTable("ns", "t", "ns_t", "no key")),
            Map.of("broken", "boom"),
            Map.of("glue", "namespace listing below [glue, restricted] failed: AccessDenied")
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
        Writeable prelude = WireVersionTestSupport.NO_PRELUDE;
        assertEquals(report, ((LanceNamespacePollResponse) readNextVersion(original, prelude, LanceNamespacePollResponse::new)).report());
        assertRefusesCritical(
            "LanceNamespacePollResponse",
            LanceNamespacePollResponse.WIRE_VERSION,
            original,
            prelude,
            LanceNamespacePollResponse::new
        );
    }

    public void testPollResponseOfAVersion1NodeCarriesNoPartialListings() throws Exception {
        // The stream a node of the previous plugin version writes: the
        // base fields with marker 1 and no block.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            WireVersion.write(out, 1);
            out.writeStringCollection(List.of("demo"));
            out.writeVInt(0);
            out.writeMap(Map.of("broken", "boom"), StreamOutput::writeString, StreamOutput::writeString);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceNamespaceService.PollReport restored = new LanceNamespacePollResponse(in).report();
                assertEquals(List.of("demo"), restored.surfaced());
                assertEquals(Map.of("broken", "boom"), restored.unavailable());
                assertEquals(Map.of(), restored.partial());
                assertEquals(-1, in.read());
            }
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
        Writeable prelude = clusterManagerPrelude(original);
        LanceNamespaceUpdateRequest restored = (LanceNamespaceUpdateRequest) readNextVersion(
            original,
            prelude,
            LanceNamespaceUpdateRequest::new
        );
        assertEquals("/data/root", restored.rootUri());
        assertRefusesCritical("LanceNamespaceUpdateRequest", original, prelude, LanceNamespaceUpdateRequest::new);
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
        Writeable prelude = out -> out.writeBoolean(true);
        LanceNamespaceUpdateResponse restored = (LanceNamespaceUpdateResponse) readNextVersion(
            original,
            prelude,
            LanceNamespaceUpdateResponse::new
        );
        assertTrue(restored.isAcknowledged());
        assertFalse(restored.changed());
        assertRefusesCritical("LanceNamespaceUpdateResponse", original, prelude, LanceNamespaceUpdateResponse::new);
    }

    private interface Reader {
        Object read(StreamInput in) throws IOException;
    }

    /** Reads the stream of the next version with one optional block appended, asserting the block is consumed. */
    private static Object readNextVersion(Writeable original, Writeable prelude, Reader reader) throws IOException {
        BytesReference newer = WireVersionTestSupport.asNextVersion(original, prelude, false, out -> out.writeString("a later field"));
        try (StreamInput in = newer.streamInput()) {
            Object read = reader.read(in);
            assertEquals("the reader consumed the block", -1, in.read());
            return read;
        }
    }

    /** Reads the stream of the next version with one critical block appended and asserts the refusal names {@code format}. */
    private static void assertRefusesCritical(String format, Writeable original, Writeable prelude, Reader reader) throws IOException {
        assertRefusesCritical(format, 1, original, prelude, reader);
    }

    /**
     * Reads the stream of the next version after {@code current} with one
     * critical block appended and asserts the refusal names
     * {@code format} and both versions.
     */
    private static void assertRefusesCritical(String format, int current, Writeable original, Writeable prelude, Reader reader)
        throws IOException {
        BytesReference newer = WireVersionTestSupport.asNextVersion(original, prelude, true, out -> out.writeString("a later constraint"));
        int next = current + 1;
        try (StreamInput in = newer.streamInput()) {
            IOException refused = expectThrows(IOException.class, () -> reader.read(in));
            assertEquals(WireVersion.criticalBlockMessage(format, next, next, current), refused.getMessage());
            assertEquals(
                format
                    + " wire version ["
                    + next
                    + "] adds fields in version ["
                    + next
                    + "] that this node's ["
                    + current
                    + "] cannot ignore: "
                    + "upgrade this node before sending it this message",
                refused.getMessage()
            );
        }
    }
}
