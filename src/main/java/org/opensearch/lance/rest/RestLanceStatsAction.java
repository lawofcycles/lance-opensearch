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
 * GET /_lance/stats and GET /_lance/stats/{nodeId}
 *
 * Per node figures of the plugin's snapshot cache, off-heap column store,
 * native memory accounting and full-text probe limit. The handler only
 * builds a {@link LanceStatsRequest}; the numbers are read on each node by
 * the transport action, so nothing blocks on the REST thread.
 */
public class RestLanceStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "lance_stats";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_lance/stats"), new Route(RestRequest.Method.GET, "/_lance/stats/{nodeId}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] nodeIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        LanceStatsRequest stats = new LanceStatsRequest(nodeIds);
        return channel -> client.execute(LanceStatsAction.INSTANCE, stats, new RestActions.NodesResponseRestListener<>(channel));
    }
}
