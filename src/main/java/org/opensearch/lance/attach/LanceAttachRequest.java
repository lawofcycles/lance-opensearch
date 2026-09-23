/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import java.io.IOException;
import java.util.Optional;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.StorageOptions;

/**
 * Request for {@link LanceAttachAction}: the parsed body of
 * {@code POST /_lance/attach}.
 *
 * <p>A {@link ClusterManagerNodeRequest} so the transport action can
 * forward it to the elected cluster manager; the inherited
 * {@code clusterManagerNodeTimeout} bounds how long the node that
 * received the REST call waits for a manager to be known.
 */
public final class LanceAttachRequest extends ClusterManagerNodeRequest<LanceAttachRequest> {

    private final String table;
    private final String indexName;
    private final Long pinnedVersion;
    private final String tag;
    private final StorageOptions storageOptions;
    private final LanceOverrides overrides;
    private final String indexPlacement;
    private final boolean asyncDerive;

    public LanceAttachRequest(
        String table,
        String indexName,
        Long pinnedVersion,
        String tag,
        StorageOptions storageOptions,
        LanceOverrides overrides
    ) {
        this(table, indexName, pinnedVersion, tag, storageOptions, overrides, null, false);
    }

    public LanceAttachRequest(
        String table,
        String indexName,
        Long pinnedVersion,
        String tag,
        StorageOptions storageOptions,
        LanceOverrides overrides,
        String indexPlacement
    ) {
        this(table, indexName, pinnedVersion, tag, storageOptions, overrides, indexPlacement, false);
    }

    /**
     * @param table          Lance table URI to attach.
     * @param indexName      explicit index name, or {@code null} to derive it
     *                       from the table directory name.
     * @param pinnedVersion  manifest version to pin, or {@code null} to
     *                       follow the latest version.
     * @param tag            Lance tag to follow, or {@code null}. Not
     *                       accepted together with {@code pinnedVersion}.
     * @param storageOptions object-store options for the table.
     * @param overrides      per-column mapping overrides, already merged
     *                       from the {@code overrides} and legacy
     *                       {@code multi_fields} clauses.
     * @param indexPlacement {@code "node_local"} to build and read search
     *                       structures from per-node shallow clones,
     *                       {@code "in_table"} or {@code null} for the
     *                       default in-table commits.
     * @param asyncDerive    {@code true} to run the text_analyzer backfill
     *                       in the background after the attach answers;
     *                       {@code false} (the default) blocks the attach
     *                       until the derived tokens columns are written
     *                       and indexed.
     */
    public LanceAttachRequest(
        String table,
        String indexName,
        Long pinnedVersion,
        String tag,
        StorageOptions storageOptions,
        LanceOverrides overrides,
        String indexPlacement,
        boolean asyncDerive
    ) {
        this.table = table;
        this.indexName = indexName;
        this.pinnedVersion = pinnedVersion;
        this.tag = tag;
        this.storageOptions = storageOptions == null ? StorageOptions.empty() : storageOptions;
        this.overrides = overrides == null ? LanceOverrides.EMPTY : overrides;
        this.indexPlacement = indexPlacement;
        this.asyncDerive = asyncDerive;
    }

    public LanceAttachRequest(StreamInput in) throws IOException {
        super(in);
        this.table = in.readString();
        this.indexName = in.readOptionalString();
        this.pinnedVersion = in.readOptionalLong();
        this.tag = in.readOptionalString();
        this.storageOptions = StorageOptions.readFromStream(in);
        // The overrides travel as their canonical JSON: one string
        // instead of a hand-rolled nested map encoding, and the same
        // bytes that end up in the index setting. Declaration order
        // survives because the JSON object preserves it.
        this.overrides = LanceOverrides.parse(in.readString());
        this.indexPlacement = in.readOptionalString();
        this.asyncDerive = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(table);
        out.writeOptionalString(indexName);
        out.writeOptionalLong(pinnedVersion);
        out.writeOptionalString(tag);
        storageOptions.writeTo(out);
        out.writeString(overrides.toJson());
        out.writeOptionalString(indexPlacement);
        out.writeBoolean(asyncDerive);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException ex = null;
        if (table == null || table.isEmpty()) {
            ex = new ActionRequestValidationException();
            ex.addValidationError("[table] is required");
        }
        if (pinnedVersion != null && pinnedVersion < 0) {
            if (ex == null) {
                ex = new ActionRequestValidationException();
            }
            ex.addValidationError("[version] must be a non-negative integer");
        }
        if (tag != null && tag.isEmpty()) {
            if (ex == null) {
                ex = new ActionRequestValidationException();
            }
            ex.addValidationError("[tag] must not be empty");
        }
        if (pinnedVersion != null && tag != null) {
            // A version is an immutable pin and a tag is a moving one; the
            // engine cannot honour both, so refuse instead of picking.
            if (ex == null) {
                ex = new ActionRequestValidationException();
            }
            ex.addValidationError("[version] and [tag] are mutually exclusive");
        }
        if (indexPlacement != null && !"in_table".equals(indexPlacement) && !"node_local".equals(indexPlacement)) {
            if (ex == null) {
                ex = new ActionRequestValidationException();
            }
            ex.addValidationError("[index_placement] must be 'in_table' or 'node_local'");
        }
        return ex;
    }

    public String table() {
        return table;
    }

    /** Explicit index name, or {@code null} when it should derive from the table name. */
    public String indexName() {
        return indexName;
    }

    public Optional<Long> pinnedVersion() {
        return Optional.ofNullable(pinnedVersion);
    }

    /** Lance tag the index should follow, or empty when it does not follow a tag. */
    public Optional<String> tag() {
        return Optional.ofNullable(tag);
    }

    public StorageOptions storageOptions() {
        return storageOptions;
    }

    public LanceOverrides overrides() {
        return overrides;
    }

    /**
     * Requested {@code index.lance.index_placement}, or empty for the
     * default ({@code in_table}).
     */
    public Optional<String> indexPlacement() {
        return Optional.ofNullable(indexPlacement);
    }

    /** Whether the text_analyzer backfill runs in the background after the attach answers. */
    public boolean asyncDerive() {
        return asyncDerive;
    }
}
