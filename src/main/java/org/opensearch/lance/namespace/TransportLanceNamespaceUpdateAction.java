/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.Priority;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Cluster-manager-scoped handler for
 * {@link LanceNamespaceUpdateAction}. Routes register / unregister
 * mutations to whichever node currently holds the cluster manager
 * role, submits an {@link AckedClusterStateUpdateTask} against
 * {@link LanceNamespaceMetadata}, and only acknowledges to the
 * caller once every follower has applied the new state. That
 * removes the split-brain race the previous per-node
 * {@code CopyOnWriteArrayList} would show whenever a search request
 * landed on a follower before its cache caught up.
 */
public final class TransportLanceNamespaceUpdateAction extends TransportClusterManagerNodeAction<
    LanceNamespaceUpdateRequest,
    AcknowledgedResponse> {

    private static final Logger LOG = LogManager.getLogger(TransportLanceNamespaceUpdateAction.class);

    @Inject
    public TransportLanceNamespaceUpdateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            LanceNamespaceUpdateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            LanceNamespaceUpdateRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        // Metadata mutations are cheap, so pinning to management
        // matches how the built-in cluster admin actions run.
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(LanceNamespaceUpdateRequest request, ClusterState state) {
        // Namespace mutations tag along with metadata writes, so
        // block behind the metadata-write block instead of the
        // heavier read block set the search paths use.
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void clusterManagerOperation(
        LanceNamespaceUpdateRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        clusterService.submitStateUpdateTask(
            "lance-namespace-" + request.operation().name().toLowerCase(java.util.Locale.ROOT) + " [" + request.rootUri() + "]",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.NORMAL, request, ActionListener.wrap(response -> {
                listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
            }, listener::onFailure)) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    LanceNamespaceMetadata existing = currentState.metadata().custom(LanceNamespaceMetadata.TYPE);
                    if (existing == null) {
                        existing = LanceNamespaceMetadata.EMPTY;
                    }
                    LanceNamespaceMetadata next = switch (request.operation()) {
                        case REGISTER -> existing.withRegistered(
                            new LanceNamespaceMetadata.Entry(request.rootUri(), request.storageOptions())
                        );
                        case UNREGISTER -> existing.withUnregistered(request.rootUri());
                    };
                    if (next == existing) {
                        return currentState;
                    }
                    Metadata.Builder metadata = Metadata.builder(currentState.metadata()).putCustom(LanceNamespaceMetadata.TYPE, next);
                    return ClusterState.builder(currentState).metadata(metadata).build();
                }

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    LOG.warn("lance namespace update [{}] failed: {}", request.rootUri(), e.toString());
                    super.onFailure(source, e);
                }
            }
        );
    }
}
