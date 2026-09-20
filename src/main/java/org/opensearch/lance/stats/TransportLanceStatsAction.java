/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Serves {@link LanceStatsAction}: fans a {@link LanceStatsNodeRequest} out
 * to the requested nodes and has each one run
 * {@link LanceStatsCollector#collect()}. OpenSearch's {@code _nodes/stats}
 * sections are a fixed enum in core with no plugin hook, so the plugin
 * exposes its figures through its own nodes action, as the k-NN plugin
 * does. The per node work reads a few counters and runs on the management
 * pool, off the transport worker.
 */
public final class TransportLanceStatsAction extends TransportNodesAction<
    LanceStatsRequest,
    LanceStatsResponse,
    LanceStatsNodeRequest,
    LanceStatsNodeResponse> {

    private final LanceStatsCollector collector;

    @Inject
    public TransportLanceStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        LanceStatsCollector collector
    ) {
        super(
            LanceStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            LanceStatsRequest::new,
            LanceStatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            LanceStatsNodeResponse.class
        );
        this.collector = collector;
    }

    @Override
    protected LanceStatsResponse newResponse(
        LanceStatsRequest request,
        List<LanceStatsNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new LanceStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected LanceStatsNodeRequest newNodeRequest(LanceStatsRequest request) {
        return new LanceStatsNodeRequest();
    }

    @Override
    protected LanceStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new LanceStatsNodeResponse(in);
    }

    @Override
    protected LanceStatsNodeResponse nodeOperation(LanceStatsNodeRequest request) {
        return new LanceStatsNodeResponse(clusterService.localNode(), collector.collect());
    }
}
