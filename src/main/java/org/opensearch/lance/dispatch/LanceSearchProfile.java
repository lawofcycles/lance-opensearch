/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.lance.plan.execute.FragmentFanOut;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;

/**
 * The {@code profile} object of a fragment path search whose body carries
 * {@code profile: true}. The stock search path profiles Lucene's query
 * tree and collectors per shard under {@code profile.shards}; the
 * fragment path runs no shard, so it reports what its executors measured
 * instead, under {@code profile.lance}:
 *
 * <pre>
 * "profile": {
 *   "lance": {
 *     "nodes": {
 *       "&lt;node id&gt;": {
 *         "query": {"millis": 12, "fts_scans": 1},
 *         "fetch": {"millis": 3, "take_count": 4, "take_rows": 47, "take_columns": 8, "take_millis": 9}
 *       }
 *     }
 *   }
 * }
 * </pre>
 *
 * {@code query.millis} is the executor's query phase (the page collected,
 * the count and the aggregations included), {@code query.fts_scans} the
 * Lance full text scans the request ran on that node (the hits scans of
 * its full text Weights and the count-only scans behind
 * {@code hits.total}), {@code fetch.millis} its
 * fetch phase (the rows behind the hits materialised), and the
 * {@code take_*} figures the {@code _rowaddr IN (...)} take scans the
 * request issued on that node, whichever phase issued them: how many,
 * the row addresses they carried, the columns they projected summed
 * over the scans, and their wall time
 * ({@link LanceFragmentQueryResponse.Profile}). A node that answered
 * several requests of the same search (several fragment groups, several
 * targets) reports the sum. A request answered from the result cache
 * ran on no executor and reports {@code "lance": {"cached": true}}.
 *
 * <p>The per node figures are gathered as the responses arrive, by
 * wrapping the transport handler of every per node request
 * ({@link #observing}); the merge never sees which node a response came
 * from. The object is rendered by {@link Profiled}, a
 * {@link SearchResponse} that adds {@code profile} after the stock fields;
 * the coordinator answers the REST layer of its own node, so the response
 * is rendered and never serialised.
 */
final class LanceSearchProfile {

    /** Node id to the summed figures of that node, in node id order so the object renders the same on every request. */
    private final Map<String, LanceFragmentQueryResponse.Profile> nodes = new ConcurrentSkipListMap<>();

    /** Adds what {@code node} reported in {@code response} to its figures. */
    void record(DiscoveryNode node, LanceFragmentQueryResponse response) {
        nodes.merge(node.getId(), response.profile(), LanceFragmentQueryResponse.Profile::plus);
    }

    /** The figures gathered so far, by node id. */
    Map<String, LanceFragmentQueryResponse.Profile> nodes() {
        return nodes;
    }

    /**
     * {@code sender} with every response it receives recorded under the
     * node it was sent to before the fan-out sees it.
     */
    FragmentFanOut.Sender observing(FragmentFanOut.Sender sender) {
        return (node, request, handler) -> sender.send(node, request, new TransportResponseHandler<>() {
            @Override
            public LanceFragmentQueryResponse read(StreamInput in) throws IOException {
                return handler.read(in);
            }

            @Override
            public void handleResponse(LanceFragmentQueryResponse response) {
                record(node, response);
                handler.handleResponse(response);
            }

            @Override
            public void handleException(TransportException exp) {
                handler.handleException(exp);
            }

            @Override
            public String executor() {
                return handler.executor();
            }
        });
    }

    /**
     * {@code response} with this profile rendered under {@code profile};
     * {@code cached} when the answer came from the result cache and no
     * executor ran.
     */
    SearchResponse attachTo(SearchResponse response, boolean cached) {
        return new Profiled(response, cached ? null : this);
    }

    /**
     * A search response that renders {@code profile.lance} after the
     * stock fields. The stock {@code profile} object is never set on a
     * fragment path response, so the key is not rendered twice.
     */
    static final class Profiled extends SearchResponse {

        /** The figures to render, or null for an answer from the result cache. */
        private final LanceSearchProfile profile;

        Profiled(SearchResponse response, LanceSearchProfile profile) {
            super(
                response.getInternalResponse(),
                response.getScrollId(),
                response.getTotalShards(),
                response.getSuccessfulShards(),
                response.getSkippedShards(),
                response.getTook().millis(),
                response.getPhaseTook(),
                response.getShardFailures(),
                response.getClusters(),
                response.pointInTimeId()
            );
            this.profile = profile;
        }

        @Override
        public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            super.innerToXContent(builder, params);
            if (!getProfileResults().isEmpty()) {
                return builder;
            }
            builder.startObject("profile");
            builder.startObject("lance");
            if (profile == null) {
                builder.field("cached", true);
            } else {
                builder.startObject("nodes");
                for (Map.Entry<String, LanceFragmentQueryResponse.Profile> node : profile.nodes().entrySet()) {
                    LanceFragmentQueryResponse.Profile figures = node.getValue();
                    builder.startObject(node.getKey());
                    builder.startObject("query");
                    builder.field("millis", figures.queryMillis());
                    builder.field("fts_scans", figures.ftsScans());
                    builder.endObject();
                    builder.startObject("fetch");
                    builder.field("millis", figures.fetchMillis());
                    builder.field("take_count", figures.takeCount());
                    builder.field("take_rows", figures.takeRows());
                    builder.field("take_columns", figures.takeColumns());
                    builder.field("take_millis", figures.takeMillis());
                    builder.endObject();
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
            return builder;
        }
    }
}
