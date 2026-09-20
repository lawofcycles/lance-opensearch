/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.util.List;

import org.opensearch.lance.refs.LanceRefsAction;
import org.opensearch.lance.refs.LanceRefsRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * GET /_lance/refs/{index}
 *
 * Lists the tags and branches of the Lance table behind a Lance-backed
 * index. The handler only builds a {@link LanceRefsRequest} and hands it
 * to {@link LanceRefsAction}; resolving the table and opening it happen
 * in the transport action so a security plugin evaluates the caller's
 * index-level privilege first and no native I/O runs on the REST thread.
 */
public class RestRefsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "lance_refs";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_lance/refs/{index}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        LanceRefsRequest refs = new LanceRefsRequest(request.param("index"));
        return channel -> client.execute(LanceRefsAction.INSTANCE, refs, new RestToXContentListener<>(channel));
    }
}
