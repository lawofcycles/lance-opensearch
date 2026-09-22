/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 *   "built":   {"fts": [{"column": "body", "type": "INVERTED"}], "scalar": [{"column": "id", "type": "BTREE"}], "vector": []},
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

    /** One built Lance index: the column (or optimized index name) and the index type. */
    public static final class BuiltResult implements Writeable, ToXContentObject {
        private final String column;
        private final String type;

        public BuiltResult(String column, String type) {
            this.column = Objects.requireNonNull(column, "column");
            this.type = Objects.requireNonNull(type, "type");
        }

        public BuiltResult(StreamInput in) throws IOException {
            this.column = in.readString();
            this.type = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(column);
            out.writeString(type);
        }

        public String column() {
            return column;
        }

        public String type() {
            return type;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("column", column).field("type", type).endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BuiltResult other)) {
                return false;
            }
            return column.equals(other.column) && type.equals(other.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(column, type);
        }

        @Override
        public String toString() {
            return column + ": " + type;
        }
    }

    /**
     * The three per-kind lists one build pass produces: indexes that
     * received a commit with the type that was built, columns left alone
     * with a reason, columns whose build threw with Lance's message.
     */
    public static final class KindResult implements Writeable {
        private final List<BuiltResult> built;
        private final List<ColumnResult> skipped;
        private final List<ColumnResult> failed;

        public KindResult(List<BuiltResult> built, List<ColumnResult> skipped, List<ColumnResult> failed) {
            this.built = List.copyOf(built);
            this.skipped = List.copyOf(skipped);
            this.failed = List.copyOf(failed);
        }

        public KindResult(StreamInput in) throws IOException {
            this.built = List.copyOf(in.readList(BuiltResult::new));
            this.skipped = List.copyOf(in.readList(ColumnResult::new));
            this.failed = List.copyOf(in.readList(ColumnResult::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(built);
            out.writeList(skipped);
            out.writeList(failed);
        }

        public List<BuiltResult> built() {
            return built;
        }

        public List<ColumnResult> skipped() {
            return skipped;
        }

        public List<ColumnResult> failed() {
            return failed;
        }
    }

    /**
     * One data node's leg of a {@code node_local} build: its per-kind
     * results, or an {@code error} message when the whole leg failed
     * (the node was unreachable or its clone could not be created).
     */
    public static final class NodeResult implements Writeable {
        private final KindResult fts;
        private final KindResult scalar;
        private final KindResult vector;
        private final String error;

        public NodeResult(KindResult fts, KindResult scalar, KindResult vector, String error) {
            this.fts = fts;
            this.scalar = scalar;
            this.vector = vector;
            this.error = error;
        }

        public NodeResult(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                this.fts = new KindResult(in);
                this.scalar = new KindResult(in);
                this.vector = new KindResult(in);
            } else {
                this.fts = null;
                this.scalar = null;
                this.vector = null;
            }
            this.error = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (fts != null) {
                out.writeBoolean(true);
                fts.writeTo(out);
                scalar.writeTo(out);
                vector.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
            out.writeOptionalString(error);
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

        public String error() {
            return error;
        }
    }

    private final String index;
    private final KindResult fts;
    private final KindResult scalar;
    private final KindResult vector;
    private final List<String> columnsFilter;
    private final List<Integer> fragmentIds;
    private final RestStatus status;
    /**
     * Per data node outcomes of a {@code node_local} build, keyed by node
     * id; {@code null} for the in-table build, whose single commit has no
     * per-node story.
     */
    private final Map<String, NodeResult> nodes;

    public LanceBuildIndexesResponse(
        String index,
        KindResult fts,
        KindResult scalar,
        KindResult vector,
        List<String> columnsFilter,
        List<Integer> fragmentIds,
        RestStatus status
    ) {
        this(index, fts, scalar, vector, columnsFilter, fragmentIds, status, null);
    }

    public LanceBuildIndexesResponse(
        String index,
        KindResult fts,
        KindResult scalar,
        KindResult vector,
        List<String> columnsFilter,
        List<Integer> fragmentIds,
        RestStatus status,
        Map<String, NodeResult> nodes
    ) {
        this.index = index;
        this.fts = Objects.requireNonNull(fts, "fts");
        this.scalar = Objects.requireNonNull(scalar, "scalar");
        this.vector = Objects.requireNonNull(vector, "vector");
        this.columnsFilter = columnsFilter == null ? null : List.copyOf(columnsFilter);
        this.fragmentIds = fragmentIds == null ? null : List.copyOf(fragmentIds);
        this.status = Objects.requireNonNull(status, "status");
        this.nodes = nodes == null ? null : new LinkedHashMap<>(nodes);
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
        if (in.readBoolean()) {
            int size = in.readVInt();
            LinkedHashMap<String, NodeResult> read = new LinkedHashMap<>();
            for (int i = 0; i < size; i++) {
                read.put(in.readString(), new NodeResult(in));
            }
            this.nodes = read;
        } else {
            this.nodes = null;
        }
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
        if (nodes == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(nodes.size());
            for (Map.Entry<String, NodeResult> entry : nodes.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
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

    public List<BuiltResult> ftsBuilt() {
        return fts.built();
    }

    public List<BuiltResult> scalarBuilt() {
        return scalar.built();
    }

    public List<BuiltResult> vectorBuilt() {
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

    /** Per node outcomes of a {@code node_local} build, or {@code null} for the in-table build. */
    public Map<String, NodeResult> nodes() {
        return nodes;
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
        builtResults(b, "fts", fts.built(), params);
        builtResults(b, "scalar", scalar.built(), params);
        builtResults(b, "vector", vector.built(), params);
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
        if (nodes != null) {
            b.startObject("nodes");
            for (Map.Entry<String, NodeResult> entry : nodes.entrySet()) {
                b.startObject(entry.getKey());
                NodeResult node = entry.getValue();
                if (node.error() != null) {
                    b.field("error", node.error());
                } else {
                    b.startObject("built");
                    builtResults(b, "fts", node.fts().built(), params);
                    builtResults(b, "scalar", node.scalar().built(), params);
                    builtResults(b, "vector", node.vector().built(), params);
                    b.endObject();
                    b.startObject("skipped");
                    columnResults(b, "fts", node.fts().skipped(), params);
                    columnResults(b, "scalar", node.scalar().skipped(), params);
                    columnResults(b, "vector", node.vector().skipped(), params);
                    b.endObject();
                    b.startObject("failed");
                    columnResults(b, "fts", node.fts().failed(), params);
                    columnResults(b, "scalar", node.scalar().failed(), params);
                    columnResults(b, "vector", node.vector().failed(), params);
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }
        if (columnsFilter != null) {
            b.field("columns_filter", columnsFilter);
        }
        if (fragmentIds != null) {
            b.field("fragment_ids", fragmentIds);
        }
        return b.endObject();
    }

    private static void builtResults(XContentBuilder b, String name, List<BuiltResult> results, Params params) throws IOException {
        b.startArray(name);
        for (BuiltResult result : results) {
            result.toXContent(b, params);
        }
        b.endArray();
    }

    private static void columnResults(XContentBuilder b, String name, List<ColumnResult> results, Params params) throws IOException {
        b.startArray(name);
        for (ColumnResult result : results) {
            result.toXContent(b, params);
        }
        b.endArray();
    }
}
