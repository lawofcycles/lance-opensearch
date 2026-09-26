/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

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
 * Serves {@link LanceRequestCacheClearAction}: every node drops the
 * entries of the named indexes from its {@link LanceRequestCache} and
 * answers how many it dropped. The per node work walks the cache's keys
 * once and runs on the management pool, off the transport worker.
 */
public final class TransportLanceRequestCacheClearAction extends TransportNodesAction<
    LanceRequestCacheClearRequest,
    LanceRequestCacheClearResponse,
    LanceRequestCacheClearNodeRequest,
    LanceRequestCacheClearNodeResponse> {

    private final LanceRequestCache requestCache;

    @Inject
    public TransportLanceRequestCacheClearAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        LanceRequestCache requestCache
    ) {
        super(
            LanceRequestCacheClearAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            LanceRequestCacheClearRequest::new,
            LanceRequestCacheClearNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            LanceRequestCacheClearNodeResponse.class
        );
        this.requestCache = requestCache;
    }

    @Override
    protected LanceRequestCacheClearResponse newResponse(
        LanceRequestCacheClearRequest request,
        List<LanceRequestCacheClearNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new LanceRequestCacheClearResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected LanceRequestCacheClearNodeRequest newNodeRequest(LanceRequestCacheClearRequest request) {
        return new LanceRequestCacheClearNodeRequest(request.indexUuids());
    }

    @Override
    protected LanceRequestCacheClearNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new LanceRequestCacheClearNodeResponse(in);
    }

    @Override
    protected LanceRequestCacheClearNodeResponse nodeOperation(LanceRequestCacheClearNodeRequest request) {
        return new LanceRequestCacheClearNodeResponse(clusterService.localNode(), requestCache.invalidateIndexes(request.indexUuids()));
    }
}
