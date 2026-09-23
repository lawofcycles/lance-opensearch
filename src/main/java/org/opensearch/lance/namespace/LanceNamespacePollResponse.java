/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Response for {@link LanceNamespacePollAction}: the cycle's
 * {@link LanceNamespaceService.PollReport}. Rendered as
 * {@code {"surfaced": [index, ...], "skipped": [{"namespace", "table",
 * "index", "reason"}, ...], "unavailable": {"name": "error", ...}}}.
 */
public final class LanceNamespacePollResponse extends ActionResponse implements ToXContentObject {

    private final LanceNamespaceService.PollReport report;

    public LanceNamespacePollResponse(LanceNamespaceService.PollReport report) {
        this.report = report;
    }

    public LanceNamespacePollResponse(StreamInput in) throws IOException {
        super(in);
        List<String> surfaced = in.readStringList();
        int skippedCount = in.readVInt();
        List<LanceNamespaceService.PollReport.SkippedTable> skipped = new ArrayList<>(skippedCount);
        for (int i = 0; i < skippedCount; i++) {
            skipped.add(
                new LanceNamespaceService.PollReport.SkippedTable(
                    in.readOptionalString(),
                    in.readOptionalString(),
                    in.readString(),
                    in.readString()
                )
            );
        }
        Map<String, String> unavailable = in.readOrderedMap(StreamInput::readString, StreamInput::readString);
        this.report = new LanceNamespaceService.PollReport(surfaced, skipped, unavailable);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(report.surfaced());
        out.writeVInt(report.skipped().size());
        for (LanceNamespaceService.PollReport.SkippedTable skipped : report.skipped()) {
            out.writeOptionalString(skipped.namespace());
            out.writeOptionalString(skipped.table());
            out.writeString(skipped.index());
            out.writeString(skipped.reason());
        }
        out.writeMap(report.unavailable(), StreamOutput::writeString, StreamOutput::writeString);
    }

    public LanceNamespaceService.PollReport report() {
        return report;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("surfaced", report.surfaced());
        builder.startArray("skipped");
        for (LanceNamespaceService.PollReport.SkippedTable skipped : report.skipped()) {
            builder.startObject();
            if (skipped.namespace() != null) {
                builder.field("namespace", skipped.namespace());
            }
            if (skipped.table() != null) {
                builder.field("table", skipped.table());
            }
            builder.field("index", skipped.index());
            builder.field("reason", skipped.reason());
            builder.endObject();
        }
        builder.endArray();
        builder.startObject("unavailable");
        for (Map.Entry<String, String> entry : report.unavailable().entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        return builder.endObject();
    }
}
