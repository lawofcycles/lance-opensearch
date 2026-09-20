/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.refs;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request for {@link LanceRefsAction}: the index named in
 * {@code GET /_lance/refs/{index}}.
 *
 * <p>Implements {@link IndicesRequest} so a security plugin can apply
 * index-level permissions to the one index named in the path.
 */
public final class LanceRefsRequest extends ActionRequest implements IndicesRequest {

    private final String index;

    /**
     * @param index OpenSearch index whose Lance table's tags and branches
     *              are listed.
     */
    public LanceRefsRequest(String index) {
        this.index = index;
    }

    public LanceRefsRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
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
}
