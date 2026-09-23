/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Serves {@link LanceNamespacePollAction}. Cluster manager scoped, like
 * the register call: whichever node receives
 * {@code POST /_lance/namespace/_poll} forwards it to the elected
 * cluster manager, where the scheduled poll runs and where the
 * {@link LanceNamespaceService} holds the catalog handles. The cycle
 * lists catalogs and opens the tables it surfaces, so it runs on the
 * generic pool.
 */
public final class TransportLanceNamespacePollAction extends TransportClusterManagerNodeAction<
    LanceNamespacePollRequest,
    LanceNamespacePollResponse> {

    private final LanceNamespaceService namespaceService;

    @Inject
    public TransportLanceNamespacePollAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        LanceNamespaceService namespaceService
    ) {
        super(
            LanceNamespacePollAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            LanceNamespacePollRequest::new,
            indexNameExpressionResolver
        );
        this.namespaceService = namespaceService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected LanceNamespacePollResponse read(StreamInput in) throws IOException {
        return new LanceNamespacePollResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(LanceNamespacePollRequest request, ClusterState state) {
        // The cycle ends in CreateIndex calls, so it waits behind the
        // metadata write block like the register call does.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(
        LanceNamespacePollRequest request,
        ClusterState state,
        ActionListener<LanceNamespacePollResponse> listener
    ) {
        // Already on the generic pool (executor() above): run the cycle
        // inline and answer with its report.
        try {
            listener.onResponse(new LanceNamespacePollResponse(namespaceService.pollNow(request.name())));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
