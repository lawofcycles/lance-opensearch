/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.WireVersion;
import org.opensearch.tasks.Task;

/**
 * The fetch round's request to one data node: render the rows named by
 * {@link #rowAddrs()} of the table version the query round read into
 * {@link org.opensearch.search.SearchHit}s under the body's per hit
 * projections. The node is the one whose query round collected those
 * rows, so its snapshot cache holds the version and its fetch cache may
 * hold the rows. The hits come back in the order of the addresses, with
 * neither score nor sort values: the coordinator kept those from the
 * query round and stamps them on the rendered hits.
 *
 * <p>{@link #version()} is the manifest version the coordinator
 * enumerated fragments from and every executor of the query round read,
 * never unresolved: a row address is only meaningful against the version
 * it was collected from. The wire format opens with {@link #WIRE_VERSION}
 * after the fields the OpenSearch base class writes (see
 * {@link WireVersion}).
 */
public final class LanceFragmentFetchRequest extends ActionRequest {

    /** The wire format's version, the first field the request writes after its base class. */
    public static final int WIRE_VERSION = 1;

    private final String tableUri;
    private final String indexName;
    private final StorageOptions storageOptions;
    private final long version;
    private final long[] rowAddrs;
    private final HitProjection projection;

    public LanceFragmentFetchRequest(
        String tableUri,
        String indexName,
        StorageOptions storageOptions,
        long version,
        long[] rowAddrs,
        HitProjection projection
    ) {
        if (version < 0) {
            throw new IllegalArgumentException("a fetch request names the version its rows were collected from, got " + version);
        }
        this.tableUri = tableUri;
        this.indexName = indexName;
        this.storageOptions = storageOptions;
        this.version = version;
        this.rowAddrs = rowAddrs.clone();
        this.projection = projection == null ? HitProjection.NONE : projection;
    }

    public LanceFragmentFetchRequest(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceFragmentFetchRequest", WIRE_VERSION);
        this.tableUri = in.readString();
        this.indexName = in.readString();
        this.storageOptions = StorageOptions.readFromStream(in);
        this.version = in.readLong();
        this.rowAddrs = in.readLongArray();
        this.projection = HitProjection.read(in);
        reader.finish();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        WireVersion.write(out, WIRE_VERSION);
        out.writeString(tableUri);
        out.writeString(indexName);
        storageOptions.writeTo(out);
        out.writeLong(version);
        out.writeLongArray(rowAddrs);
        projection.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * The task is cancellable like the query round's
     * ({@link LanceFragmentQueryTask}): a coordinator task that is
     * cancelled, or whose fetch request timed out, stops the takes on
     * this node at their next batch boundary.
     */
    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new LanceFragmentQueryTask(
            id,
            type,
            action,
            "lance fragment fetch on [" + indexName + "], " + rowAddrs.length + " rows",
            parentTaskId,
            headers
        );
    }

    public String tableUri() {
        return tableUri;
    }

    public String indexName() {
        return indexName;
    }

    public StorageOptions storageOptions() {
        return storageOptions;
    }

    /** The manifest version the rows belong to; never negative. */
    public long version() {
        return version;
    }

    /** {@link #version()} in the shape the snapshot cache takes. */
    public Optional<Long> versionOrEmpty() {
        return Optional.of(version);
    }

    /** The Lance row addresses ({@code fragmentId << 32 | offset}) to render, in response order. */
    public long[] rowAddrs() {
        return rowAddrs;
    }

    /** The per hit projections of the body, never {@code null}. */
    public HitProjection projection() {
        return projection;
    }
}
