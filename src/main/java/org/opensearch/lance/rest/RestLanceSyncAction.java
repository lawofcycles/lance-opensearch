/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.util.List;

import org.opensearch.lance.namespace.LanceIndexSyncAction;
import org.opensearch.lance.namespace.LanceIndexSyncRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * POST /{index}/_lance/sync
 *
 * Runs the freshness check of one Lance backed index now instead of at
 * the next {@code lance.namespace.poll_cadence} tick. The handler only
 * builds a {@link LanceIndexSyncRequest} and hands it to
 * {@link LanceIndexSyncAction}, which routes it to the node holding the
 * index's shard; the table open and the mapping update happen there,
 * on the generic pool. Idempotent: a check that finds the shard at the
 * table's version does nothing and answers {@code moved: false}.
 */
public class RestLanceSyncAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "lance_sync";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/{index}/_lance/sync"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        LanceIndexSyncRequest sync = new LanceIndexSyncRequest(request.param("index"));
        return channel -> client.execute(LanceIndexSyncAction.INSTANCE, sync, new RestToXContentListener<>(channel));
    }
}
