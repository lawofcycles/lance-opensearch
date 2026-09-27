/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.lance.plan.explain.LanceExplainAction;
import org.opensearch.lance.plan.explain.LanceExplainRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

/**
 * GET and POST /_plugins/_lance/explain/{index}
 *
 * Explains the plan the fragment coordinator would execute for a search
 * body against a Lance-backed index without executing anything: the
 * route, the logical and physical trees, the per node plan and the
 * refinements a data node could still apply. The handler
 * parses the body into a {@link SearchSourceBuilder} (the same parse
 * {@code _search} applies) and hands it to {@link LanceExplainAction};
 * resolving the index, reading its schema and translating happen in the
 * transport action so a security plugin evaluates the caller's
 * index-level privilege first and no native I/O runs on the REST
 * thread.
 *
 * <p>The index comes after the verb, as in {@code build_indexes/{index}}
 * and {@code refs/{index}}, because {@code /_plugins/_lance/{index}/_explain}
 * would put an {@code {index}} wildcard on the same path segment as the
 * {@code {node_id}} of {@code /_plugins/_lance/{node_id}/stats}, which
 * the REST path trie rejects at registration.
 */
public class RestLanceExplainAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "lance_explain";
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        // GET with a body, the shape _search and _validate/query accept, and
        // POST for the clients that cannot send a body on a GET.
        return List.of(
            new ReplacedRoute(RestRequest.Method.GET, "/_plugins/_lance/explain/{index}", "/{index}/_lance/explain"),
            new ReplacedRoute(RestRequest.Method.POST, "/_plugins/_lance/explain/{index}", "/{index}/_lance/explain")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SearchSourceBuilder source = null;
        if (request.hasContentOrSourceParam()) {
            source = new SearchSourceBuilder();
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                source.parseXContent(parser);
            }
        }
        LanceExplainRequest explain = new LanceExplainRequest(request.param("index"), source);
        return channel -> client.execute(LanceExplainAction.INSTANCE, explain, new RestToXContentListener<>(channel));
    }
}
