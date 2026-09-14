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
        return List.of(
            new Route(RestRequest.Method.POST, "/_lance/namespace"),
            new Route(RestRequest.Method.GET, "/_lance/namespace"),
            new Route(RestRequest.Method.DELETE, "/_lance/namespace")
        );
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
        if (request.method() == RestRequest.Method.DELETE) {
            // DELETE only stops the polling of that path. Already-surfaced
            // indexes stay; the operator can delete them via
            // DELETE /{index} if they want the tables to disappear. This
            // matches the "the namespace registration is separate from the
            // OpenSearch index lifecycle" contract in the RFC.
            return channel -> {
                boolean removed = service.unregister(path);
                try (XContentBuilder b = channel.newBuilder()) {
                    b.startObject().field("unregistered", removed).field("path", path).endObject();
                    channel.sendResponse(new BytesRestResponse(removed ? RestStatus.OK : RestStatus.NOT_FOUND, b));
                }
            };
        }
        // POST: register (with allowlist check).
        if (!allowedRoots.allows(path)) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(
                    RestStatus.FORBIDDEN,
                    "path [" + path + "] is not under any of the configured lance.allowed_table_roots"
                )
            );
        }
        // Path existence check for filesystem-scheme paths. Object-store
        // schemes (s3://, gs://, azure://, ...) route through Lance's own
        // storage layer and cannot be probed from here; skip the check for
        // those and let Lance surface the error on the first list_tables
        // call. Filesystem paths that do not exist as a directory are
        // rejected up front so the poller does not spin against a
        // typo'd path forever.
        if (!path.contains("://")) {
            try {
                java.nio.file.Path fsPath = java.nio.file.Path.of(path);
                if (!java.nio.file.Files.exists(fsPath)) {
                    return channel -> channel.sendResponse(
                        new BytesRestResponse(RestStatus.BAD_REQUEST, "path [" + path + "] does not exist")
                    );
                }
                if (!java.nio.file.Files.isDirectory(fsPath)) {
                    return channel -> channel.sendResponse(
                        new BytesRestResponse(RestStatus.BAD_REQUEST, "path [" + path + "] exists but is not a directory")
                    );
                }
            } catch (java.nio.file.InvalidPathException e) {
                return channel -> channel.sendResponse(
                    new BytesRestResponse(RestStatus.BAD_REQUEST, "path [" + path + "] is not a valid filesystem path: " + e.getReason())
                );
            }
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
