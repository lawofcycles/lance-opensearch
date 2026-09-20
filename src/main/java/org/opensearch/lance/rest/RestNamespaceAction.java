/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.util.List;
import java.util.Map;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.namespace.LanceNamespaceListAction;
import org.opensearch.lance.namespace.LanceNamespaceListRequest;
import org.opensearch.lance.namespace.LanceNamespaceUpdateAction;
import org.opensearch.lance.namespace.LanceNamespaceUpdateRequest;
import org.opensearch.lance.namespace.LanceNamespaceUpdateResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestBuilderListener;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * REST surface for namespace registration and listing.
 *
 * <ul>
 *   <li>{@code POST /_lance/namespace {"path": "/data"}} registers a
 *       catalog directory through {@link LanceNamespaceUpdateAction}.</li>
 *   <li>{@code DELETE /_lance/namespace {"path": "/data"}} unregisters it
 *       through the same action.</li>
 *   <li>{@code GET /_lance/namespace} lists the registered roots and
 *       {@code POST /_lance/namespace/tables {"path": "/data"}} lists the
 *       tables under one root, both through
 *       {@link LanceNamespaceListAction}.</li>
 * </ul>
 *
 * <p>The handler only parses the body and hands the request to the
 * transport action. Allowlist and path existence checks live in the
 * transport action so they run after a security plugin has evaluated
 * the caller's privileges, and nothing about the path (whether it is
 * registered, whether it exists) is revealed to a caller who lacks them.
 */
public class RestNamespaceAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "lance_namespace";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST, "/_lance/namespace"),
            new Route(RestRequest.Method.GET, "/_lance/namespace"),
            new Route(RestRequest.Method.DELETE, "/_lance/namespace"),
            new Route(RestRequest.Method.POST, "/_lance/namespace/tables")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (request.method() == RestRequest.Method.GET) {
            return channel -> client.execute(
                LanceNamespaceListAction.INSTANCE,
                LanceNamespaceListRequest.namespaces(),
                new RestStatusToXContentListener<>(channel)
            );
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
        // POST /_lance/namespace/tables is a read-only listing endpoint.
        // POST is used (rather than GET with a query parameter) because
        // registered paths can contain slashes, scheme prefixes
        // (s3://bucket/root), and other characters that make URL-encoded
        // path segments fragile. Body-with-path matches the shape of the
        // register / unregister calls right above.
        if (request.path().endsWith("/tables")) {
            return channel -> client.execute(
                LanceNamespaceListAction.INSTANCE,
                LanceNamespaceListRequest.tables(path),
                new RestStatusToXContentListener<>(channel)
            );
        }
        if (request.method() == RestRequest.Method.DELETE) {
            // DELETE only stops the polling of that path. Already-surfaced
            // indexes stay; the operator can delete them via
            // DELETE /{index} if they want the tables to disappear. This
            // matches the "the namespace registration is separate from the
            // OpenSearch index lifecycle" contract in the RFC.
            return channel -> client.execute(
                LanceNamespaceUpdateAction.INSTANCE,
                LanceNamespaceUpdateRequest.unregister(path),
                new RestBuilderListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(LanceNamespaceUpdateResponse response, XContentBuilder b) throws Exception {
                        boolean removed = response.changed();
                        b.startObject().field("unregistered", removed).field("path", path).endObject();
                        return new BytesRestResponse(removed ? RestStatus.OK : RestStatus.NOT_FOUND, b);
                    }
                }
            );
        }
        StorageOptions storageOptions;
        try {
            storageOptions = StorageOptions.parseFromRequestField(body.get("storage_options"), "[lance_namespace]");
        } catch (IllegalArgumentException e) {
            String message = e.getMessage();
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, message));
        }
        return channel -> client.execute(
            LanceNamespaceUpdateAction.INSTANCE,
            LanceNamespaceUpdateRequest.register(path, storageOptions),
            new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(LanceNamespaceUpdateResponse response, XContentBuilder b) throws Exception {
                    b.startObject()
                        .field("registered", path)
                        .field("note", "tables surface as indexes within the poll cadence")
                        .endObject();
                    return new BytesRestResponse(RestStatus.OK, b);
                }
            }
        );
    }
}
