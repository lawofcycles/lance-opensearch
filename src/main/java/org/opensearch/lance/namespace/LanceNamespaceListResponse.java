/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;
import java.util.List;

import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Response for {@link LanceNamespaceListAction}. Carries one of three
 * shapes, mirrored by {@link #toXContent}:
 * <ul>
 *   <li>{@code {"namespaces": [...]}} when the request had no path.</li>
 *   <li>{@code {"path": "...", "tables": [...]}} when the path is a
 *       registered namespace.</li>
 *   <li>{@code {"registered": false, "path": "..."}} with status 404
 *       when the path is not registered, so "unknown namespace" and
 *       "registered but empty" stay distinguishable.</li>
 * </ul>
 */
public final class LanceNamespaceListResponse extends ActionResponse implements StatusToXContentObject {

    private final List<String> namespaces;
    private final String path;
    private final boolean registered;
    private final List<String> tables;

    public static LanceNamespaceListResponse namespaces(List<String> namespaces) {
        return new LanceNamespaceListResponse(List.copyOf(namespaces), null, false, null);
    }

    public static LanceNamespaceListResponse tables(String path, List<String> tables) {
        return new LanceNamespaceListResponse(null, path, true, List.copyOf(tables));
    }

    public static LanceNamespaceListResponse unregistered(String path) {
        return new LanceNamespaceListResponse(null, path, false, null);
    }

    private LanceNamespaceListResponse(List<String> namespaces, String path, boolean registered, List<String> tables) {
        this.namespaces = namespaces;
        this.path = path;
        this.registered = registered;
        this.tables = tables;
    }

    public LanceNamespaceListResponse(StreamInput in) throws IOException {
        super(in);
        this.namespaces = in.readOptionalStringList();
        this.path = in.readOptionalString();
        this.registered = in.readBoolean();
        this.tables = in.readOptionalStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStringCollection(namespaces);
        out.writeOptionalString(path);
        out.writeBoolean(registered);
        out.writeOptionalStringCollection(tables);
    }

    /** Registered roots, or {@code null} when the request carried a path. */
    public List<String> namespaces() {
        return namespaces;
    }

    /** The path the request asked about, or {@code null} for a root listing. */
    public String path() {
        return path;
    }

    /** True when {@link #path()} names a registered namespace. */
    public boolean registered() {
        return registered;
    }

    /** Sorted table names under {@link #path()}, or {@code null} when not registered. */
    public List<String> tables() {
        return tables;
    }

    @Override
    public RestStatus status() {
        return path != null && !registered ? RestStatus.NOT_FOUND : RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (path == null) {
            builder.field("namespaces", namespaces);
        } else if (!registered) {
            builder.field("registered", false).field("path", path);
        } else {
            builder.field("path", path).field("tables", tables);
        }
        return builder.endObject();
    }
}
