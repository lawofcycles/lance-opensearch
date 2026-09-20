/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.lance.index.LanceBuildIndexesAction;
import org.opensearch.lance.index.LanceBuildIndexesRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * POST /_lance/build_indexes/{index} [{"columns": [...], "fragment_ids": [...], "optimize": bool, "retrain": bool}]
 *
 * Manual index build endpoint. The handler parses the body and hands a
 * {@link LanceBuildIndexesRequest} to {@link LanceBuildIndexesAction};
 * resolving the table, running the Lance builders, and refreshing the
 * index happen in the transport action so a security plugin evaluates
 * the caller's index-level privilege before the plugin opens the table.
 */
public class RestBuildIndexesAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "lance_build_indexes";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_lance/build_indexes/{index}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String indexName = request.param("index");
        Map<String, Object> body = request.hasContent()
            ? XContentHelper.convertToMap(request.content(), false, request.getMediaType()).v2()
            : Map.of();
        @SuppressWarnings("unchecked")
        List<String> columnsFilterRaw = (List<String>) body.get("columns");
        @SuppressWarnings("unchecked")
        List<Number> fragmentIdsRaw = (List<Number>) body.get("fragment_ids");
        boolean optimize = Boolean.TRUE.equals(body.get("optimize"));
        boolean retrain = Boolean.TRUE.equals(body.get("retrain"));

        if (optimize && fragmentIdsRaw != null) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(
                    RestStatus.BAD_REQUEST,
                    "fragment_ids is not supported with optimize=true (Lance OptimizeOptions covers every out-of-index fragment automatically)"
                )
            );
        }
        if (retrain && !optimize) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(RestStatus.BAD_REQUEST, "retrain is only valid with optimize=true")
            );
        }

        List<Integer> fragmentIds = null;
        if (fragmentIdsRaw != null) {
            fragmentIds = new ArrayList<>(fragmentIdsRaw.size());
            for (Number n : fragmentIdsRaw) {
                fragmentIds.add(n.intValue());
            }
        }
        LanceBuildIndexesRequest build = new LanceBuildIndexesRequest(indexName, columnsFilterRaw, fragmentIds, optimize, retrain);
        return channel -> client.execute(LanceBuildIndexesAction.INSTANCE, build, new RestToXContentListener<>(channel));
    }
}
