/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsAction;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.index.store.StoreStats;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.tasks.Task;

/**
 * ActionFilter that makes the {@code store} group of the indices stats
 * API ({@code indices:monitor/stats}) report the data file total of a
 * Lance backed index's table instead of the size of its shard directory.
 *
 * <p>{@code IndexShard.storeStats()} measures the shard's Lucene
 * directory, which for a Lance backed index holds the bootstrap commit
 * alone, and {@code Engine} has no hook to answer differently. The shard
 * engine does report the table's size, as the manifest of the version the
 * index follows records it, in {@code DocsStats.totalSizeInBytes}; this
 * filter copies that number into the {@code store} of every Lance backed
 * shard of the response, so {@code _stats}, {@code _cat/indices} and
 * {@code _cat/shards}, which all go through this action, show the table.
 * No manifest is opened here: the engine opened it on the data node when
 * it computed the doc stats, so the two groups describe the same version.
 *
 * <p>A request that asks for {@code store} without {@code docs}
 * ({@code GET /<index>/_stats/store}) is widened to ask for {@code docs}
 * as well and the docs group is removed from the answer again, so the
 * caller sees the groups it asked for. The shards of an index that is not
 * Lance backed keep the store the data node measured. {@code _cluster/stats}
 * and {@code _nodes/stats} read the shards inside the node and never pass
 * through this action; their store stays the shard directory's size.
 */
public final class LanceIndicesStatsActionFilter implements ActionFilter {

    private final ClusterService clusterService;

    public LanceIndicesStatsActionFilter(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * After the security plugin's filter ({@code Integer.MIN_VALUE}),
     * which authorises the request this filter only reshapes the answer
     * of, and after the other Lance filters ({@code Integer.MIN_VALUE + 100}
     * and {@code + 101}); they act on different actions, the distinct
     * value only keeps their order explicit.
     */
    @Override
    public int order() {
        return Integer.MIN_VALUE + 102;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (!IndicesStatsAction.NAME.equals(action) || !(request instanceof IndicesStatsRequest statsRequest) || !statsRequest.store()) {
            chain.proceed(task, action, request, listener);
            return;
        }
        boolean stripDocs = !statsRequest.docs();
        if (stripDocs) {
            statsRequest.docs(true);
        }
        chain.proceed(task, action, request, ActionListener.map(listener, response -> {
            if (response instanceof IndicesStatsResponse stats) {
                withTableStoreSizes(stats, clusterService.state().metadata(), stripDocs);
            }
            return response;
        }));
    }

    /**
     * Replace, in place, the {@code store} of every shard of a Lance
     * backed index in {@code response} with its {@code docs.totalSizeInBytes},
     * keeping the reserved bytes the data node reported, and drop the
     * {@code docs} group of every shard when {@code stripDocs} is set.
     * The shards are edited rather than rebuilt because a new
     * {@code ShardStats} needs the {@code ShardPath} the response does
     * not carry; {@code CommonStats} exposes its groups as writable fields
     * for this kind of edit, and the response has been read by nobody
     * yet when the filter sees it, so its cached totals are built from
     * the edited shards.
     *
     * @param metadata the cluster metadata the shards' indexes are looked
     *                 up in; a shard whose index (by name and uuid) is
     *                 missing from it is left as it is
     */
    static void withTableStoreSizes(IndicesStatsResponse response, Metadata metadata, boolean stripDocs) {
        for (ShardStats shard : response.getShards()) {
            CommonStats stats = shard.getStats();
            if (stats == null) {
                continue;
            }
            if (stats.docs != null && stats.store != null && isLanceBacked(metadata, shard)) {
                stats.store = new StoreStats.Builder().sizeInBytes(stats.docs.getTotalSizeInBytes())
                    .reservedSize(stats.store.getReservedSize().getBytes())
                    .build();
            }
            if (stripDocs) {
                stats.docs = null;
            }
        }
    }

    private static boolean isLanceBacked(Metadata metadata, ShardStats shard) {
        IndexMetadata index = metadata.index(shard.getShardRouting().index());
        if (index == null) {
            return false;
        }
        String table = LanceEngineFactory.tableOf(index.getSettings());
        return table != null && !table.isEmpty();
    }
}
