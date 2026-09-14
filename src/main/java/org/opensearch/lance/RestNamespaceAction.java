/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.List;
import java.util.Map;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

/**
 * POST /_lance/namespace {"path": "/data"} registers a catalog directory.
 *
 * <p>The path is validated against the {@code lance.allowed_table_roots}
 * node setting the same way {@link RestAttachAction} does. This keeps a
 * single allowlist covering both entry points that can introduce a new
 * Lance table root to the plugin.
 */
public class RestNamespaceAction extends BaseRestHandler {

    private final LanceNamespaceService service;
    private final AllowedTableRoots allowedRoots;

    public RestNamespaceAction(LanceNamespaceService service, AllowedTableRoots allowedRoots) {
        this.service = service;
        this.allowedRoots = allowedRoots;
    }

    @Override
    public String getName() {
        return "lance_namespace";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_lance/namespace"), new Route(RestRequest.Method.GET, "/_lance/namespace"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (request.method() == RestRequest.Method.GET) {
            return channel -> {
                try (XContentBuilder b = channel.newBuilder()) {
                    b.startObject().field("namespaces", service.namespaces()).endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, b));
                }
            };
        }
        Map<String, Object> body = request.hasContent()
            ? XContentHelper.convertToMap(request.content(), false, request.getMediaType()).v2()
            : Map.of();
        Object rawPath = body.get("path");
        if (rawPath == null) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "[path] is required"));
        }
        if (!(rawPath instanceof String)) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(RestStatus.BAD_REQUEST, "[path] must be a string, got " + rawPath.getClass().getSimpleName())
            );
        }
        String path = (String) rawPath;
        if (path.isEmpty()) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, "[path] must not be empty"));
        }
        if (!allowedRoots.allows(path)) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(
                    RestStatus.FORBIDDEN,
                    "path [" + path + "] is not under any of the configured lance.allowed_table_roots"
                )
            );
        }
        return channel -> {
            service.register(path);
            try (XContentBuilder b = channel.newBuilder()) {
                b.startObject().field("registered", path).field("note", "tables surface as indexes within the poll cadence").endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, b));
            }
        };
    }
}
