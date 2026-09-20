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
    private final List<Integer> fragmentIds;
    private final boolean optimize;
    private final boolean retrain;
    private final String tokenizer;

    /**
     * @param index       OpenSearch index whose Lance table gets the indexes.
     * @param columns     restrict the build to these columns, or {@code null}
     *                    for every indexable column.
     * @param fragmentIds build only over these fragments, or {@code null} for
     *                    the whole table. Not accepted together with
     *                    {@code optimize}.
     * @param optimize    merge existing indexes over uncovered fragments
     *                    instead of creating new ones.
     * @param retrain     with {@code optimize}, rebuild instead of merging.
     * @param tokenizer   Lance {@code base_tokenizer} for the FTS indexes this
     *                    request creates, or {@code null} for Lance's
     *                    {@code simple}. Passed to Lance verbatim. Only valid
     *                    without {@code optimize}: an existing index keeps the
     *                    tokenizer it was built with.
     */
    public LanceBuildIndexesRequest(
        String index,
        List<String> columns,
        List<Integer> fragmentIds,
        boolean optimize,
        boolean retrain,
        String tokenizer
    ) {
        this.index = index;
        this.columns = columns == null ? null : List.copyOf(columns);
        this.fragmentIds = fragmentIds == null ? null : List.copyOf(fragmentIds);
        this.optimize = optimize;
        this.retrain = retrain;
        this.tokenizer = tokenizer;
    }

    public LanceBuildIndexesRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.columns = in.readOptionalStringList();
        this.fragmentIds = in.readBoolean() ? in.readList(StreamInput::readVInt) : null;
        this.optimize = in.readBoolean();
        this.retrain = in.readBoolean();
        this.tokenizer = in.readOptionalString();
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
        out.writeOptionalString(tokenizer);
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
        if (tokenizer != null && tokenizer.isEmpty()) {
            ex = add(ex, "tokenizer must not be empty; omit it to use Lance's default (simple)");
        }
        if (tokenizer != null && optimize) {
            ex = add(ex, "tokenizer is only valid with optimize=false (an existing FTS index keeps the tokenizer it was built with)");
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
}
