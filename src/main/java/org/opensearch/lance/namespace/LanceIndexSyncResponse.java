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

/**
 * Response for {@link LanceIndexSyncAction}: the
 * {@link LanceIndexFreshnessService.Outcome} of the check. Rendered as
 * {@code {"index", "checked", "reason"?, "moved", "served_version",
 * "target_version", "mapping_changed", "rebuilt"}}; {@code reason} is
 * present only when the index was not checked (it is pinned to a
 * version).
 */
public final class LanceIndexSyncResponse extends ActionResponse implements ToXContentObject {

    private final LanceIndexFreshnessService.Outcome outcome;

    public LanceIndexSyncResponse(LanceIndexFreshnessService.Outcome outcome) {
        this.outcome = outcome;
    }

    public LanceIndexSyncResponse(StreamInput in) throws IOException {
        super(in);
        this.outcome = new LanceIndexFreshnessService.Outcome(
            in.readString(),
            in.readBoolean(),
            in.readOptionalString(),
            in.readBoolean(),
            in.readLong(),
            in.readLong(),
            in.readBoolean(),
            in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(outcome.index());
        out.writeBoolean(outcome.checked());
        out.writeOptionalString(outcome.reason());
        out.writeBoolean(outcome.moved());
        out.writeLong(outcome.servedVersion());
        out.writeLong(outcome.targetVersion());
        out.writeBoolean(outcome.mappingChanged());
        out.writeBoolean(outcome.rebuilt());
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
        return builder.endObject();
    }
}
