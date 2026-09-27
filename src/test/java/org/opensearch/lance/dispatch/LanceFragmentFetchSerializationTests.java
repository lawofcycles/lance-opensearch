/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.WireVersionTestSupport;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

/** Wire shape of the fetch round transport classes. */
public class LanceFragmentFetchSerializationTests extends OpenSearchTestCase {

    public void testRequestRoundTrip() throws Exception {
        HitProjection projection = new HitProjection(
            new FetchSourceContext(true, new String[] { "id" }, new String[0]),
            null,
            List.of(),
            List.of(new FieldAndFormat("title", null)),
            false
        );
        LanceFragmentFetchRequest original = new LanceFragmentFetchRequest(
            "s3://bucket/tables/demo.lance",
            "demo",
            StorageOptions.of(Map.of("region", "us-east-1")),
            7L,
            new long[] { (2L << 32) | 5L, 3L, (2L << 32) | 1L },
            projection
        );
        // The marker follows the parent task id the ActionRequest base
        // class writes.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TaskId.readFromStream(in);
                assertEquals(LanceFragmentFetchRequest.WIRE_VERSION, in.readVInt());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                LanceFragmentFetchRequest restored = new LanceFragmentFetchRequest(in);
                assertEquals(original.tableUri(), restored.tableUri());
                assertEquals(original.indexName(), restored.indexName());
                assertEquals(original.storageOptions().asMap(), restored.storageOptions().asMap());
                assertEquals(7L, restored.version());
                assertEquals(Optional.of(7L), restored.versionOrEmpty());
                assertArrayEquals(original.rowAddrs(), restored.rowAddrs());
                assertEquals(projection, restored.projection());
                assertNull(restored.validate());
                assertEquals("the reader consumed the stream", -1, in.read());
            }
        }
        // A block of the next version is stepped over.
        BytesReference newer = WireVersionTestSupport.asNextVersion(
            original,
            o -> TaskId.EMPTY_TASK_ID.writeTo(o),
            false,
            o -> o.writeBoolean(true)
        );
        try (StreamInput in = newer.streamInput()) {
            LanceFragmentFetchRequest restored = new LanceFragmentFetchRequest(in);
            assertArrayEquals(original.rowAddrs(), restored.rowAddrs());
            assertEquals("the reader consumed the block", -1, in.read());
        }
    }

    public void testRequestNamesTheVersionAndACancellableTask() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new LanceFragmentFetchRequest("/t.lance", "demo", StorageOptions.empty(), -1L, new long[0], HitProjection.NONE)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("names the version"));
        LanceFragmentFetchRequest request = new LanceFragmentFetchRequest(
            "/t.lance",
            "demo",
            StorageOptions.empty(),
            3L,
            new long[] { 1L, 2L },
            null
        );
        assertEquals(HitProjection.NONE, request.projection());
        Task task = request.createTask(1L, "transport", LanceFragmentFetchAction.NAME, TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        assertTrue(task instanceof CancellableTask);
        assertEquals("lance fragment fetch on [demo], 2 rows", task.getDescription());
    }

    public void testResponseRoundTrip() throws Exception {
        SearchHit first = new SearchHit(0, "0-3", Collections.emptyMap(), Collections.emptyMap());
        first.sourceRef(new BytesArray("{\"id\":3}"));
        SearchHit second = new SearchHit(4, "1-0", Collections.emptyMap(), Collections.emptyMap());
        LanceFragmentQueryResponse.Profile profile = new LanceFragmentQueryResponse.Profile(0L, 3L, 2L, 2L, 5L, 6L, 0L);
        LanceFragmentFetchResponse original = new LanceFragmentFetchResponse(List.of(first, second), profile);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(LanceFragmentFetchResponse.WIRE_VERSION, in.readVInt());
            }
            try (StreamInput in = out.bytes().streamInput()) {
                LanceFragmentFetchResponse restored = new LanceFragmentFetchResponse(in);
                assertEquals(2, restored.hits().size());
                assertEquals("0-3", restored.hits().get(0).getId());
                assertEquals("{\"id\":3}", restored.hits().get(0).getSourceAsString());
                assertEquals("1-0", restored.hits().get(1).getId());
                assertEquals(profile, restored.profile());
                assertEquals("the reader consumed the stream", -1, in.read());
            }
        }
        assertEquals(
            "a response built without a profile reports none",
            LanceFragmentQueryResponse.Profile.NONE,
            new LanceFragmentFetchResponse(List.of(), null).profile()
        );
        BytesReference newer = WireVersionTestSupport.asNextVersion(
            original,
            WireVersionTestSupport.NO_PRELUDE,
            false,
            o -> o.writeVLong(1L)
        );
        try (StreamInput in = newer.streamInput()) {
            LanceFragmentFetchResponse restored = new LanceFragmentFetchResponse(in);
            assertEquals(2, restored.hits().size());
            assertEquals("the reader consumed the block", -1, in.read());
        }
    }
}
