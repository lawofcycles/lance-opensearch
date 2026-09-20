/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Response for {@link LanceBuildIndexesAction}: per index kind (FTS /
 * scalar / vector), which Lance indexes were built or optimised, which
 * columns were skipped and why, and which columns failed with Lance's
 * message. Echoes the column and fragment filters the caller supplied.
 * {@link #toXContent} produces the {@code POST /_lance/build_indexes/{index}}
 * body:
 *
 * <pre>
 * {
 *   "index": "demo",
 *   "built":   {"fts": ["body"], "scalar": ["id"], "vector": []},
 *   "skipped": {"fts": [], "scalar": [{"column": "category", "reason": "..."}], "vector": []},
 *   "failed":  {"fts": [], "scalar": [], "vector": [{"column": "embedding", "reason": "..."}]}
 * }
 * </pre>
 *
 * Two echo fields follow only when the request carried them:
 * {@code "columns_filter": [...]} for the request's {@code columns} and
 * {@code "fragment_ids": [...]} for its {@code fragment_ids}.
 *
 * <p>The HTTP status travels with the response ({@link #status()}) so the
 * REST layer can answer 500 or 400 while still returning the body above,
 * which is how a caller learns which columns did land when one failed.
 * The status is decided in {@link TransportLanceBuildIndexesAction}.
 */
public final class LanceBuildIndexesResponse extends ActionResponse implements StatusToXContentObject {

    /** One skipped or failed column with the reason. */
    public static final class ColumnResult implements Writeable, ToXContentObject {
        private final String column;
        private final String reason;

        public ColumnResult(String column, String reason) {
            this.column = Objects.requireNonNull(column, "column");
            this.reason = Objects.requireNonNull(reason, "reason");
        }

        public ColumnResult(StreamInput in) throws IOException {
            this.column = in.readString();
            this.reason = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(column);
            out.writeString(reason);
        }

        public String column() {
            return column;
        }

        public String reason() {
            return reason;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("column", column).field("reason", reason).endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ColumnResult other)) {
                return false;
            }
            return column.equals(other.column) && reason.equals(other.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(column, reason);
        }

        @Override
        public String toString() {
            return column + ": " + reason;
        }
    }

    /**
     * The three per-kind lists one build pass produces: names that
     * received a commit, columns left alone with a reason, columns whose
     * build threw with Lance's message.
     */
    public static final class KindResult implements Writeable {
        private final List<String> built;
        private final List<ColumnResult> skipped;
        private final List<ColumnResult> failed;

        public KindResult(List<String> built, List<ColumnResult> skipped, List<ColumnResult> failed) {
            this.built = List.copyOf(built);
            this.skipped = List.copyOf(skipped);
            this.failed = List.copyOf(failed);
        }

        public KindResult(StreamInput in) throws IOException {
            this.built = List.copyOf(in.readStringList());
            this.skipped = List.copyOf(in.readList(ColumnResult::new));
            this.failed = List.copyOf(in.readList(ColumnResult::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(built);
            out.writeList(skipped);
            out.writeList(failed);
        }

        public List<String> built() {
            return built;
        }

        public List<ColumnResult> skipped() {
            return skipped;
        }

        public List<ColumnResult> failed() {
            return failed;
        }
    }

    private final String index;
    private final KindResult fts;
    private final KindResult scalar;
    private final KindResult vector;
    private final List<String> columnsFilter;
    private final List<Integer> fragmentIds;
    private final RestStatus status;

    public LanceBuildIndexesResponse(
        String index,
        KindResult fts,
        KindResult scalar,
        KindResult vector,
        List<String> columnsFilter,
        List<Integer> fragmentIds,
        RestStatus status
    ) {
        this.index = index;
        this.fts = Objects.requireNonNull(fts, "fts");
        this.scalar = Objects.requireNonNull(scalar, "scalar");
        this.vector = Objects.requireNonNull(vector, "vector");
        this.columnsFilter = columnsFilter == null ? null : List.copyOf(columnsFilter);
        this.fragmentIds = fragmentIds == null ? null : List.copyOf(fragmentIds);
        this.status = Objects.requireNonNull(status, "status");
    }

    public LanceBuildIndexesResponse(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.fts = new KindResult(in);
        this.scalar = new KindResult(in);
        this.vector = new KindResult(in);
        List<String> columnsFilterIn = in.readOptionalStringList();
        this.columnsFilter = columnsFilterIn == null ? null : List.copyOf(columnsFilterIn);
        this.fragmentIds = in.readBoolean() ? List.copyOf(in.readList(StreamInput::readVInt)) : null;
        this.status = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        fts.writeTo(out);
        scalar.writeTo(out);
        vector.writeTo(out);
        out.writeOptionalStringCollection(columnsFilter);
        if (fragmentIds == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeCollection(fragmentIds, StreamOutput::writeVInt);
        }
        RestStatus.writeTo(out, status);
    }

    public String index() {
        return index;
    }

    public KindResult fts() {
        return fts;
    }

    public KindResult scalar() {
        return scalar;
    }

    public KindResult vector() {
        return vector;
    }

    public List<String> ftsBuilt() {
        return fts.built();
    }

    public List<String> scalarBuilt() {
        return scalar.built();
    }

    public List<String> vectorBuilt() {
        return vector.built();
    }

    /** Column filter echoed from the request, or {@code null} when none was given. */
    public List<String> columnsFilter() {
        return columnsFilter;
    }

    /** Fragment filter echoed from the request, or {@code null} when none was given. */
    public List<Integer> fragmentIds() {
        return fragmentIds;
    }

    /** True when at least one column, of any kind, is listed under {@code failed}. */
    public boolean hasFailures() {
        return !fts.failed().isEmpty() || !scalar.failed().isEmpty() || !vector.failed().isEmpty();
    }

    @Override
    public RestStatus status() {
        return status;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("index", index);
        b.startObject("built");
        b.field("fts", fts.built());
        b.field("scalar", scalar.built());
        b.field("vector", vector.built());
        b.endObject();
        b.startObject("skipped");
        columnResults(b, "fts", fts.skipped(), params);
        columnResults(b, "scalar", scalar.skipped(), params);
        columnResults(b, "vector", vector.skipped(), params);
        b.endObject();
        b.startObject("failed");
        columnResults(b, "fts", fts.failed(), params);
        columnResults(b, "scalar", scalar.failed(), params);
        columnResults(b, "vector", vector.failed(), params);
        b.endObject();
        if (columnsFilter != null) {
            b.field("columns_filter", columnsFilter);
        }
        if (fragmentIds != null) {
            b.field("fragment_ids", fragmentIds);
        }
        return b.endObject();
    }

    private static void columnResults(XContentBuilder b, String name, List<ColumnResult> results, Params params) throws IOException {
        b.startArray(name);
        for (ColumnResult result : results) {
            result.toXContent(b, params);
        }
        b.endArray();
    }
}
