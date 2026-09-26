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
import org.opensearch.lance.WireVersion;

/**
 * Response for {@link LanceNamespacePollAction}: the cycle's
 * {@link LanceNamespaceService.PollReport}. Rendered as
 * {@code {"surfaced": [index, ...], "skipped": [{"namespace", "table",
 * "index", "reason"}, ...], "unavailable": {"name": "error", ...},
 * "partial": {"name": "error", ...}}}.
 * Opens with {@link #WIRE_VERSION} (see {@link WireVersion}).
 */
public final class LanceNamespacePollResponse extends ActionResponse implements ToXContentObject {

    /** The wire format's version, the first field the response writes; 2 added the partial listings. */
    public static final int WIRE_VERSION = 2;

    private final LanceNamespaceService.PollReport report;

    public LanceNamespacePollResponse(LanceNamespaceService.PollReport report) {
        this.report = report;
    }

    public LanceNamespacePollResponse(StreamInput in) throws IOException {
        super(in);
        WireVersion.Reader reader = WireVersion.read(in, "LanceNamespacePollResponse", WIRE_VERSION);
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
        Map<String, String> partial = reader.block(
            2,
            block -> block.readOrderedMap(StreamInput::readString, StreamInput::readString),
            Map.of()
        );
        this.report = new LanceNamespaceService.PollReport(surfaced, skipped, unavailable, partial);
        reader.finish();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        WireVersion.write(out, WIRE_VERSION);
        out.writeStringCollection(report.surfaced());
        out.writeVInt(report.skipped().size());
        for (LanceNamespaceService.PollReport.SkippedTable skipped : report.skipped()) {
            out.writeOptionalString(skipped.namespace());
            out.writeOptionalString(skipped.table());
            out.writeString(skipped.index());
            out.writeString(skipped.reason());
        }
        out.writeMap(report.unavailable(), StreamOutput::writeString, StreamOutput::writeString);
        // A caller of the previous version reads the report without the
        // partial listings; the cycle itself ran the same, so the block
        // is never critical.
        WireVersion.writeBlock(out, false, o -> o.writeMap(report.partial(), StreamOutput::writeString, StreamOutput::writeString));
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
        builder.startObject("partial");
        for (Map.Entry<String, String> entry : report.partial().entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        return builder.endObject();
    }
}
