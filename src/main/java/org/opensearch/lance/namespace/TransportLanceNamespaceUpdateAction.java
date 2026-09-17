/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
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
 * caller once every follower has applied the new state. The
 * response carries a {@link LanceNamespaceUpdateResponse#changed()}
 * bit so the REST layer can 404 a request that hit a URI the
 * cluster never had, even when the caller's local view lags
 * behind the manager.
 */
public final class TransportLanceNamespaceUpdateAction extends TransportClusterManagerNodeAction<
    LanceNamespaceUpdateRequest,
    LanceNamespaceUpdateResponse> {

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
    protected LanceNamespaceUpdateResponse read(StreamInput in) throws IOException {
        return new LanceNamespaceUpdateResponse(in);
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
        ActionListener<LanceNamespaceUpdateResponse> listener
    ) {
        // Capture whether the state update actually transitioned so
        // the REST layer can distinguish "already at target state"
        // (unregister of an unknown path, register of a duplicate
        // path) from a real change. Local reads on a follower would
        // race a recent register from another node, so the
        // authoritative decision has to happen here on the manager.
        AtomicBoolean changed = new AtomicBoolean(false);
        clusterService.submitStateUpdateTask(
            "lance-namespace-" + request.operation().name().toLowerCase(java.util.Locale.ROOT) + " [" + request.rootUri() + "]",
            new AckedClusterStateUpdateTask<LanceNamespaceUpdateResponse>(Priority.NORMAL, request, ActionListener.wrap(response -> {
                listener.onResponse(new LanceNamespaceUpdateResponse(response.isAcknowledged(), changed.get()));
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
                        // No-op: leave the changed flag at false so
                        // the caller can 404 an unregister of an
                        // unknown path.
                        return currentState;
                    }
                    changed.set(true);
                    Metadata.Builder metadata = Metadata.builder(currentState.metadata()).putCustom(LanceNamespaceMetadata.TYPE, next);
                    return ClusterState.builder(currentState).metadata(metadata).build();
                }

                @Override
                protected LanceNamespaceUpdateResponse newResponse(boolean acknowledged) {
                    return new LanceNamespaceUpdateResponse(acknowledged, changed.get());
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
