/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.single.shard.TransportSingleShardAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Serves {@link LanceIndexSyncAction}. A single shard action: the
 * request is routed to the node that holds the index's primary shard
 * (a Lance backed index has one shard and no replica), where the
 * {@link LanceIndexFreshnessService} that checks the index lives. The
 * check opens the table and may send a mapping update, so it runs on
 * the generic pool.
 */
public final class TransportLanceIndexSyncAction extends TransportSingleShardAction<LanceIndexSyncRequest, LanceIndexSyncResponse> {

    private final IndicesService indicesService;
    private final LanceIndexFreshnessService freshnessService;

    @Inject
    public TransportLanceIndexSyncAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService,
        LanceIndexFreshnessService freshnessService
    ) {
        super(
            LanceIndexSyncAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            LanceIndexSyncRequest::new,
            ThreadPool.Names.GENERIC
        );
        this.indicesService = indicesService;
        this.freshnessService = freshnessService;
    }

    @Override
    protected LanceIndexSyncResponse shardOperation(LanceIndexSyncRequest request, ShardId shardId) throws IOException {
        IndexShard shard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        return new LanceIndexSyncResponse(freshnessService.syncNow(shard));
    }

    @Override
    protected Writeable.Reader<LanceIndexSyncResponse> getResponseReader() {
        return LanceIndexSyncResponse::new;
    }

    @Override
    protected boolean resolveIndex(LanceIndexSyncRequest request) {
        return true;
    }

    @Override
    protected ShardsIterator shards(ClusterState state, InternalRequest request) {
        IndexRoutingTable routingTable = state.routingTable().index(request.concreteIndex());
        if (routingTable == null) {
            throw new IllegalStateException("index [" + request.concreteIndex() + "] has no shard routing");
        }
        // One shard per Lance backed index; its primary is the copy
        // whose node runs the check.
        return routingTable.shard(0).primaryShardIt();
    }
}
