/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request for {@link LanceBuildIndexesAction}: the parsed body of
 * {@code POST /_lance/build_indexes/{index}}.
 *
 * <p>Implements {@link IndicesRequest} so a security plugin can apply
 * index-level permissions to the one index named in the path.
 */
public final class LanceBuildIndexesRequest extends ActionRequest implements IndicesRequest {

    private final String index;
    private final List<String> columns;
    private final List<String> ftsColumns;
    private final List<Integer> fragmentIds;
    private final boolean optimize;
    private final boolean retrain;
    private final String tokenizer;
    private final boolean withPosition;

    /**
     * @param index       OpenSearch index whose Lance table gets the indexes.
     * @param columns     restrict the build to these columns, or {@code null}
     *                    for every indexable column.
     * @param ftsColumns  Utf8 columns that receive an FTS (inverted) index in
     *                    this build even though they carry none yet, or
     *                    {@code null}. Without this list a Utf8 column
     *                    without an FTS index is a keyword column and gets a
     *                    BTree index instead. Columns named here are left out
     *                    of the scalar build. Not accepted together with
     *                    {@code optimize}.
     * @param fragmentIds build only over these fragments, or {@code null} for
     *                    the whole table. Not accepted together with
     *                    {@code optimize}.
     * @param optimize    merge existing indexes over uncovered fragments
     *                    instead of creating new ones.
     * @param retrain     with {@code optimize}, rebuild instead of merging.
     * @param tokenizer   Lance {@code base_tokenizer} for the FTS indexes this
     *                    request creates, or {@code null} for Lance's
     *                    {@code simple}. Passed to Lance verbatim. Requires
     *                    {@code ftsColumns}, because an existing index keeps
     *                    the tokenizer it was built with.
     * @param withPosition store token positions in the FTS indexes this
     *                    request creates (Lance {@code with_position}).
     *                    {@code lance_match_phrase} needs them; Lance's
     *                    default, and this one, is {@code false}. Requires
     *                    {@code ftsColumns} for the same reason as
     *                    {@code tokenizer}.
     */
    public LanceBuildIndexesRequest(
        String index,
        List<String> columns,
        List<String> ftsColumns,
        List<Integer> fragmentIds,
        boolean optimize,
        boolean retrain,
        String tokenizer,
        boolean withPosition
    ) {
        this.index = index;
        this.columns = columns == null ? null : List.copyOf(columns);
        this.ftsColumns = ftsColumns == null ? null : List.copyOf(ftsColumns);
        this.fragmentIds = fragmentIds == null ? null : List.copyOf(fragmentIds);
        this.optimize = optimize;
        this.retrain = retrain;
        this.tokenizer = tokenizer;
        this.withPosition = withPosition;
    }

    public LanceBuildIndexesRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.columns = in.readOptionalStringList();
        this.fragmentIds = in.readBoolean() ? in.readList(StreamInput::readVInt) : null;
        this.optimize = in.readBoolean();
        this.retrain = in.readBoolean();
        this.ftsColumns = in.readOptionalStringList();
        this.tokenizer = in.readOptionalString();
        this.withPosition = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeOptionalStringCollection(columns);
        if (fragmentIds == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeCollection(fragmentIds, StreamOutput::writeVInt);
        }
        out.writeBoolean(optimize);
        out.writeBoolean(retrain);
        out.writeOptionalStringCollection(ftsColumns);
        out.writeOptionalString(tokenizer);
        out.writeBoolean(withPosition);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException ex = null;
        if (index == null || index.isEmpty()) {
            ex = add(ex, "index is required");
        }
        if (optimize && fragmentIds != null) {
            ex = add(
                ex,
                "fragment_ids is not supported with optimize=true (Lance OptimizeOptions covers every out-of-index fragment automatically)"
            );
        }
        if (retrain && !optimize) {
            ex = add(ex, "retrain is only valid with optimize=true");
        }
        if (ftsColumns != null && ftsColumns.isEmpty()) {
            ex = add(ex, "fts_columns must name at least one Utf8 column; omit it to build no new FTS index");
        }
        if (ftsColumns != null && optimize) {
            ex = add(ex, "fts_columns is only valid with optimize=false (optimize extends existing indexes and creates none)");
        }
        if (tokenizer != null && tokenizer.isEmpty()) {
            ex = add(ex, "tokenizer must not be empty; omit it to use Lance's default (simple)");
        }
        if (tokenizer != null && ftsColumns == null) {
            ex = add(
                ex,
                "tokenizer applies to the FTS indexes this request creates; name them in fts_columns "
                    + "(an existing FTS index keeps the tokenizer it was built with)"
            );
        }
        if (withPosition && ftsColumns == null) {
            ex = add(
                ex,
                "with_position applies to the FTS indexes this request creates; name them in fts_columns "
                    + "(an existing FTS index keeps the position setting it was built with)"
            );
        }
        return ex;
    }

    private static ActionRequestValidationException add(ActionRequestValidationException ex, String error) {
        if (ex == null) {
            ex = new ActionRequestValidationException();
        }
        ex.addValidationError(error);
        return ex;
    }

    @Override
    public String[] indices() {
        return new String[] { index };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    public String index() {
        return index;
    }

    /** Column filter, or {@code null} when every indexable column is a target. */
    public List<String> columns() {
        return columns;
    }

    /**
     * Utf8 columns that get a new FTS index in this build, or {@code null}
     * when the request creates no FTS index.
     */
    public List<String> ftsColumns() {
        return ftsColumns;
    }

    /** Fragment filter, or {@code null} for the whole table. */
    public List<Integer> fragmentIds() {
        return fragmentIds;
    }

    public boolean optimize() {
        return optimize;
    }

    public boolean retrain() {
        return retrain;
    }

    /**
     * Lance {@code base_tokenizer} for FTS indexes created by this request,
     * or {@code null} when the caller left the choice to the default.
     */
    public String tokenizer() {
        return tokenizer;
    }

    /**
     * Whether the FTS indexes created by this request store token
     * positions ({@code lance_match_phrase} needs them). Default false.
     */
    public boolean withPosition() {
        return withPosition;
    }
}
