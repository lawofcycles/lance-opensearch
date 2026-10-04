/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * Request for {@link LanceExplainAction}: the index named in
 * {@code GET /_plugins/_lance/explain/<index>} and the parsed search body whose
 * plan is explained. The body is optional on the wire; an absent body
 * plans as {@code _search} without a body does, a {@code match_all}
 * page of ten.
 *
 * <p>Implements {@link IndicesRequest.Replaceable} so a security plugin
 * can apply index-level permissions to the one index named in the path:
 * the plugin resolves {@link #indices()} against the caller's role and
 * writes the resolved name back through {@link #indices(String...)}.
 * Without the setter the security plugin treats the request as one
 * over all indices, and only a role on {@code *} grants it.
 */
public final class LanceExplainRequest extends ActionRequest implements IndicesRequest.Replaceable {

    private String index;
    private final SearchSourceBuilder source;

    /**
     * @param index OpenSearch index whose plan is explained
     * @param source parsed search body, or null when the request had none
     */
    public LanceExplainRequest(String index, SearchSourceBuilder source) {
        this.index = index;
        this.source = source;
    }

    public LanceExplainRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.source = in.readOptionalWriteable(SearchSourceBuilder::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeOptionalWriteable(source);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (index == null || index.isEmpty()) {
            ActionRequestValidationException ex = new ActionRequestValidationException();
            ex.addValidationError("index is required");
            return ex;
        }
        return null;
    }

    @Override
    public String[] indices() {
        return new String[] { index };
    }

    /**
     * Replace the index the request names. The path names exactly one
     * index and {@link #indicesOptions()} expands no wildcard, so a
     * caller that resolved {@link #indices()} hands back one name;
     * anything else is a programming error on the caller's side and
     * is refused rather than silently narrowed to the first element.
     *
     * @throws IllegalArgumentException when {@code indices} is not
     *     exactly one non-empty name
     */
    @Override
    public IndicesRequest indices(String... indices) {
        if (indices == null || indices.length != 1 || indices[0] == null || indices[0].isEmpty()) {
            throw new IllegalArgumentException(
                "explain request names exactly one index; got " + (indices == null ? "null" : Arrays.toString(indices))
            );
        }
        this.index = indices[0];
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    public String index() {
        return index;
    }

    public SearchSourceBuilder source() {
        return source;
    }
}
