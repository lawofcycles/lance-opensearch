/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.util.List;

import org.opensearch.core.common.Strings;
import org.opensearch.lance.stats.LanceStatsAction;
import org.opensearch.lance.stats.LanceStatsRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;
import org.opensearch.transport.client.node.NodeClient;

/**
 * GET /_plugins/_lance/stats and GET /_plugins/_lance/{node_id}/stats
 *
 * Per node figures of the plugin's snapshot cache, off-heap column store,
 * native memory accounting and full-text probe limit. The handler only
 * builds a {@link LanceStatsRequest}; the numbers are read on each node by
 * the transport action, so nothing blocks on the REST thread.
 *
 * <p>The deprecated path template in {@link #replacedRoutes()} names its
 * parameter {@code node_id} as well, so {@code request.param("node_id")}
 * reads the node ids on both paths.
 */
public class RestLanceStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "lance_stats";
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return List.of(
            new ReplacedRoute(RestRequest.Method.GET, "/_plugins/_lance/stats", "/_lance/stats"),
            new ReplacedRoute(RestRequest.Method.GET, "/_plugins/_lance/{node_id}/stats", "/_lance/stats/{node_id}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] nodeIds = Strings.splitStringByCommaToArray(request.param("node_id"));
        LanceStatsRequest stats = new LanceStatsRequest(nodeIds);
        return channel -> client.execute(LanceStatsAction.INSTANCE, stats, new RestActions.NodesResponseRestListener<>(channel));
    }
}
