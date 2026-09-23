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

/**
 * Request for {@link LanceExplainAction}: the index named in
 * {@code GET /<index>/_lance/explain} and the parsed search body whose
 * plan is explained. The body is optional on the wire; an absent body
 * plans as {@code _search} without a body does, a {@code match_all}
 * page of ten.
 *
 * <p>Implements {@link IndicesRequest} so a security plugin can apply
 * index-level permissions to the one index named in the path.
 */
public final class LanceExplainRequest extends ActionRequest implements IndicesRequest {

    private final String index;
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
