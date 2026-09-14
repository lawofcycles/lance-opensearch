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

/** POST /_lance/namespace {"path": "/data"} registers a catalog directory. */
public class RestNamespaceAction extends BaseRestHandler {

    private final LanceNamespaceService service;

    public RestNamespaceAction(LanceNamespaceService service) {
        this.service = service;
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
        Map<String, Object> body = XContentHelper.convertToMap(request.content(), false, request.getMediaType()).v2();
        String path = (String) body.get("path");
        return channel -> {
            service.register(path);
            try (XContentBuilder b = channel.newBuilder()) {
                b.startObject().field("registered", path).field("note", "tables surface as indexes within the poll cadence").endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, b));
            }
        };
    }
}
