/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.dispatch.LanceStatisticsPrefetchNodeResponse.Outcome;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.plan.metadata.TableStatisticsCache;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Serves {@link LanceStatisticsPrefetchAction}: every data node starts
 * the collection of the named table version in its
 * {@link TableStatisticsCache} and answers whether it started one, held
 * the version already or was collecting it. The node operation only
 * queues the collection (the cache runs it on the generic pool and
 * opens the table there); it runs on the generic pool itself because it
 * reads the cluster state for the index's settings, off the transport
 * worker. An index the node's cluster state does not know fails the
 * node's leg, which the caller reports and ignores.
 */
public final class TransportLanceStatisticsPrefetchAction extends TransportNodesAction<
    LanceStatisticsPrefetchRequest,
    LanceStatisticsPrefetchResponse,
    LanceStatisticsPrefetchNodeRequest,
    LanceStatisticsPrefetchNodeResponse> {

    private final LanceWarmCache warmCache;

    @Inject
    public TransportLanceStatisticsPrefetchAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        LanceWarmCache warmCache
    ) {
        super(
            LanceStatisticsPrefetchAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            LanceStatisticsPrefetchRequest::new,
            LanceStatisticsPrefetchNodeRequest::new,
            ThreadPool.Names.GENERIC,
            LanceStatisticsPrefetchNodeResponse.class
        );
        this.warmCache = warmCache;
    }

    @Override
    protected LanceStatisticsPrefetchResponse newResponse(
        LanceStatisticsPrefetchRequest request,
        List<LanceStatisticsPrefetchNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new LanceStatisticsPrefetchResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected LanceStatisticsPrefetchNodeRequest newNodeRequest(LanceStatisticsPrefetchRequest request) {
        return new LanceStatisticsPrefetchNodeRequest(request.indexName(), request.tableUri(), request.version());
    }

    @Override
    protected LanceStatisticsPrefetchNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new LanceStatisticsPrefetchNodeResponse(in);
    }

    @Override
    protected LanceStatisticsPrefetchNodeResponse nodeOperation(LanceStatisticsPrefetchNodeRequest request) {
        IndexMetadata index = clusterService.state().metadata().index(request.indexName());
        if (index == null) {
            throw new IndexNotFoundException(request.indexName());
        }
        Settings settings = index.getSettings();
        String table = settings.get(LanceEngineFactory.TABLE_SETTING, "");
        if (table.isEmpty()) {
            throw new IllegalArgumentException("index [" + request.indexName() + "] is not backed by a Lance table");
        }
        StorageOptions storageOptions = StorageOptions.fromIndexSettings(settings);
        TableStatisticsCache statistics = warmCache.tableStatistics();
        String tableUri = request.tableUri();
        long version = request.version();
        Outcome outcome;
        if (statistics.prefetch(tableUri, version, () -> LanceRegistry.openDataset(table, storageOptions, Optional.of(version)))) {
            outcome = Outcome.STARTED;
        } else if (statistics.peek(tableUri, version) != null) {
            outcome = Outcome.HELD;
        } else {
            outcome = Outcome.PENDING;
        }
        return new LanceStatisticsPrefetchNodeResponse(clusterService.localNode(), outcome);
    }
}
