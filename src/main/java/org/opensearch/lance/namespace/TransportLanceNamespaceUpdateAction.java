/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
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
import org.opensearch.core.rest.RestStatus;
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
 *
 * <p>Register requests are also validated here (allowlist, filesystem
 * path existence) rather than in the REST handler, so the checks run
 * only after the {@link ActionFilters} chain has admitted the caller.
 */
public final class TransportLanceNamespaceUpdateAction extends TransportClusterManagerNodeAction<
    LanceNamespaceUpdateRequest,
    LanceNamespaceUpdateResponse> {

    private static final Logger LOG = LogManager.getLogger(TransportLanceNamespaceUpdateAction.class);

    private final AllowedTableRoots allowedRoots;

    @Inject
    public TransportLanceNamespaceUpdateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AllowedTableRoots allowedRoots
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
        this.allowedRoots = allowedRoots;
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
        if (request.operation() == LanceNamespaceUpdateRequest.Operation.REGISTER) {
            RegisterDecision decision;
            try {
                decision = decideRegister(allowedRoots, state.metadata().custom(LanceNamespaceMetadata.TYPE), request.toEntry());
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            if (decision == RegisterDecision.ALREADY_REGISTERED) {
                // Acknowledge without a state update so a repeated
                // register stays a no-op even if the directory has since
                // gone away.
                listener.onResponse(new LanceNamespaceUpdateResponse(true, false));
                return;
            }
        }
        submitUpdate(request, listener);
    }

    /** Outcome of {@link #decideRegister} when the request is not rejected. */
    enum RegisterDecision {
        /** Submit the cluster state update. */
        PROCEED,
        /** The root is already registered; acknowledge without an update. */
        ALREADY_REGISTERED
    }

    /**
     * Decides what a register request should do, in this order: the
     * allowlist, then the duplicate lookup, then the filesystem existence
     * check. Throws {@link OpenSearchStatusException} (403) for a root
     * outside {@code lance.allowed_table_roots} and
     * {@link IllegalArgumentException} (400) for a filesystem path that is
     * not an existing directory.
     *
     * <p>The allowlist runs before the duplicate lookup so a root that is
     * already registered but has since been removed from the allowlist is
     * refused rather than acknowledged; it is a string comparison, so the
     * ordering costs no I/O and reveals nothing about the path. The
     * existence check runs last so a repeated register of a known root
     * stays a no-op even if the directory has gone away.
     *
     * <p>The allowlist and the existence check apply to directory
     * registrations, whose root the request itself names. A rest or
     * glue registration has no root to check here — its tables come
     * from the catalog at poll time — so the poll validates every
     * table location the catalog returns against the same allowlist
     * before surfacing it.
     *
     * <p>These checks live here rather than in the REST handler so they
     * sit behind the {@link ActionFilters} chain: a caller without the
     * privilege gets the security plugin's response regardless of whether
     * the path exists. Object-store schemes ({@code s3://}, {@code gs://},
     * ...) route through Lance's own storage layer and cannot be probed
     * from here; Lance surfaces a missing root on the first list-tables
     * call.
     */
    static RegisterDecision decideRegister(
        AllowedTableRoots allowedRoots,
        LanceNamespaceMetadata current,
        LanceNamespaceMetadata.Entry entry
    ) {
        boolean directory = LanceNamespaceMetadata.Entry.TYPE_DIRECTORY.equals(entry.type());
        if (directory && !allowedRoots.allows(entry.rootUri())) {
            throw new OpenSearchStatusException(
                "path [" + entry.rootUri() + "] is not under any of the configured lance.allowed_table_roots",
                RestStatus.FORBIDDEN
            );
        }
        if (current != null && current.withRegistered(entry) == current) {
            return RegisterDecision.ALREADY_REGISTERED;
        }
        if (!directory) {
            return RegisterDecision.PROCEED;
        }
        String path = entry.rootUri();
        if (path.contains("://")) {
            return RegisterDecision.PROCEED;
        }
        Path fsPath;
        try {
            fsPath = Path.of(path);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("path [" + path + "] is not a valid filesystem path: " + e.getReason(), e);
        }
        if (!Files.exists(fsPath)) {
            throw new IllegalArgumentException("path [" + path + "] does not exist");
        }
        if (!Files.isDirectory(fsPath)) {
            throw new IllegalArgumentException("path [" + path + "] exists but is not a directory");
        }
        return RegisterDecision.PROCEED;
    }

    private void submitUpdate(LanceNamespaceUpdateRequest request, ActionListener<LanceNamespaceUpdateResponse> listener) {
        // Capture whether the state update actually transitioned so
        // the REST layer can distinguish "already at target state"
        // (unregister of an unknown path, register of a duplicate
        // path) from a real change. Local reads on a follower would
        // race a recent register from another node, so the
        // authoritative decision has to happen here on the manager.
        AtomicBoolean changed = new AtomicBoolean(false);
        clusterService.submitStateUpdateTask(
            "lance-namespace-" + request.operation().name().toLowerCase(Locale.ROOT) + " [" + request.name() + "]",
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
                        case REGISTER -> existing.withRegistered(request.toEntry());
                        case UNREGISTER -> existing.withUnregistered(request.name());
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
                    LOG.warn("lance namespace update [{}] failed: {}", request.name(), e.toString());
                    super.onFailure(source, e);
                }
            }
        );
    }
}
