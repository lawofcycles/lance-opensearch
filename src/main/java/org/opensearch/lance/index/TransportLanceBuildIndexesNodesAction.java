/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import java.io.IOException;
import java.util.List;

import org.lance.Dataset;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceLocalClones;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Serves {@link LanceBuildIndexesNodesAction}: on each data node, makes
 * sure this node's shallow clone of the index's table exists at the
 * source version the coordinator resolved, runs the builders against the
 * clone, and reports the per-node outcome. The commits land in the
 * clone's own manifest chain; the source table is only ever read.
 *
 * <p>Runs on the generic pool because {@code Dataset} opens, the shallow
 * clone, and the builders all block on native I/O.
 */
public final class TransportLanceBuildIndexesNodesAction extends TransportNodesAction<
    LanceBuildIndexesNodesRequest,
    LanceBuildIndexesNodesResponse,
    LanceBuildIndexesNodeRequest,
    LanceBuildIndexesNodeResponse> {

    private final ClusterService clusterService;

    @Inject
    public TransportLanceBuildIndexesNodesAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(
            LanceBuildIndexesNodesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            LanceBuildIndexesNodesRequest::new,
            LanceBuildIndexesNodeRequest::new,
            ThreadPool.Names.GENERIC,
            LanceBuildIndexesNodeResponse.class
        );
        this.clusterService = clusterService;
    }

    @Override
    protected LanceBuildIndexesNodesResponse newResponse(
        LanceBuildIndexesNodesRequest request,
        List<LanceBuildIndexesNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new LanceBuildIndexesNodesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected LanceBuildIndexesNodeRequest newNodeRequest(LanceBuildIndexesNodesRequest request) {
        return new LanceBuildIndexesNodeRequest(request.request(), request.sourceVersion());
    }

    @Override
    protected LanceBuildIndexesNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new LanceBuildIndexesNodeResponse(in);
    }

    @Override
    protected LanceBuildIndexesNodeResponse nodeOperation(LanceBuildIndexesNodeRequest nodeRequest) {
        LanceBuildIndexesRequest request = nodeRequest.request();
        IndexMetadata metadata = clusterService.state().metadata().index(request.index());
        if (metadata == null) {
            throw new IndexNotFoundException(request.index());
        }
        String tableUri = metadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
        StorageOptions storageOptions = StorageOptions.fromIndexSettings(metadata.getSettings());
        LanceLocalClones clones = LanceLocalClones.instance();
        if (clones == null) {
            throw new IllegalStateException("node_local build requested but this node has no clone service");
        }
        try {
            LanceLocalClones.CloneLocation clone = clones.ensure(
                metadata.getIndex().getName(),
                tableUri,
                storageOptions,
                nodeRequest.sourceVersion(),
                false
            );
            TransportLanceBuildIndexesAction.BuildOutcome outcome;
            String mappingJson;
            LanceOverrides overrides = LanceOverrides.of(metadata.getSettings());
            try (Dataset dataset = LanceRegistry.openDataset(clone.uri(), StorageOptions.empty())) {
                outcome = TransportLanceBuildIndexesAction.runBuilders(dataset, request, overrides);
                // Re-derive the mapping from the clone with the indexes it
                // now carries (preserving attach-time overrides) so the
                // coordinator can flip a first-built FTS column from
                // keyword to lance_text.
                mappingJson = RestAttachAction.derive(dataset, overrides, true).mappingJson();
            }
            return new LanceBuildIndexesNodeResponse(
                clusterService.localNode(),
                TransportLanceBuildIndexesAction.toKindResult(outcome.fts()),
                TransportLanceBuildIndexesAction.toKindResult(outcome.scalar()),
                TransportLanceBuildIndexesAction.toKindResult(outcome.vector()),
                TransportLanceBuildIndexesAction.statusOf(outcome.failures()),
                mappingJson
            );
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("node_local build failed on this node: " + e.getMessage(), e);
        }
    }
}
