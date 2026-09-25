/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.WireVersion;

/**
 * Response for {@link LanceIndexSyncAction}: the
 * {@link LanceIndexFreshnessService.Outcome} of the check. Rendered as
 * {@code {"index", "checked", "reason"?, "moved", "served_version",
 * "target_version", "mapping_changed", "rebuilt", "mapping_error"?}};
 * {@code reason} is present only when the index was not checked (it is
 * pinned to a version), {@code mapping_error} only when the check sent
 * a mapping update the cluster manager refused. Opens with
 * {@link #WIRE_VERSION} (see {@link WireVersion}); version 2 added the
 * refused update's message as an optional block, so a node of the
 * previous plugin version answers without it.
 */
public final class LanceIndexSyncResponse extends ActionResponse implements ToXContentObject {

    /** The wire format's version, the first field the response writes; 2 added the mapping error. */
    public static final int WIRE_VERSION = 2;

    private final LanceIndexFreshnessService.Outcome outcome;

    public LanceIndexSyncResponse(LanceIndexFreshnessService.Outcome outcome) {
        this.outcome = outcome;
    }

    public LanceIndexSyncResponse(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceIndexSyncResponse", WIRE_VERSION);
        String index = in.readString();
        boolean checked = in.readBoolean();
        String reason = in.readOptionalString();
        boolean moved = in.readBoolean();
        long servedVersion = in.readLong();
        long targetVersion = in.readLong();
        boolean mappingChanged = in.readBoolean();
        boolean rebuilt = in.readBoolean();
        String mappingError = reader.block(2, StreamInput::readOptionalString, null);
        reader.finish();
        this.outcome = new LanceIndexFreshnessService.Outcome(
            index,
            checked,
            reason,
            moved,
            servedVersion,
            targetVersion,
            mappingChanged,
            rebuilt,
            mappingError
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        WireVersion.write(out, WIRE_VERSION);
        out.writeString(outcome.index());
        out.writeBoolean(outcome.checked());
        out.writeOptionalString(outcome.reason());
        out.writeBoolean(outcome.moved());
        out.writeLong(outcome.servedVersion());
        out.writeLong(outcome.targetVersion());
        out.writeBoolean(outcome.mappingChanged());
        out.writeBoolean(outcome.rebuilt());
        // A caller of the previous version reads the outcome without the
        // message; the check itself ran the same, so the block is
        // never critical.
        WireVersion.writeBlock(out, false, o -> o.writeOptionalString(outcome.mappingError()));
    }

    public LanceIndexFreshnessService.Outcome outcome() {
        return outcome;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", outcome.index());
        builder.field("checked", outcome.checked());
        if (outcome.reason() != null) {
            builder.field("reason", outcome.reason());
        }
        builder.field("moved", outcome.moved());
        builder.field("served_version", outcome.servedVersion());
        builder.field("target_version", outcome.targetVersion());
        builder.field("mapping_changed", outcome.mappingChanged());
        builder.field("rebuilt", outcome.rebuilt());
        if (outcome.mappingError() != null) {
            builder.field("mapping_error", outcome.mappingError());
        }
        return builder.endObject();
    }
}
