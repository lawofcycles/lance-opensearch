/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request for {@link LanceNamespaceListAction}. A {@code null} path
 * asks for the registered namespace roots; a non-null path asks for
 * the tables under that registered root.
 */
public final class LanceNamespaceListRequest extends ActionRequest {

    private final String path;

    /** List the registered namespace roots. */
    public static LanceNamespaceListRequest namespaces() {
        return new LanceNamespaceListRequest((String) null);
    }

    /** List the tables under the namespace registered at {@code path}. */
    public static LanceNamespaceListRequest tables(String path) {
        return new LanceNamespaceListRequest(path);
    }

    private LanceNamespaceListRequest(String path) {
        this.path = path;
    }

    public LanceNamespaceListRequest(StreamInput in) throws IOException {
        super(in);
        this.path = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(path);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (path != null && path.isEmpty()) {
            ActionRequestValidationException ex = new ActionRequestValidationException();
            ex.addValidationError("path must not be empty");
            return ex;
        }
        return null;
    }

    /** Registered root to list tables for, or {@code null} to list the roots themselves. */
    public String path() {
        return path;
    }
}
