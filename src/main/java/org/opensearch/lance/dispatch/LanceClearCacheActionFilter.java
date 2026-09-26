/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.Client;

/**
 * ActionFilter that gives {@code POST /<index>/_cache/clear} an effect
 * on the coordinator result cache and the data nodes' fetch cache. The stock
 * {@code TransportClearIndicesCacheAction} clears the caches of the
 * shards, and the result cache ({@link LanceRequestCache}) sits on the
 * coordinating nodes, keyed by index uuid, out of its reach, as does the
 * fetch cache ({@code LanceFetchCache}) on the data nodes. When the
 * request asks for the request cache (`request=true`, or no cache named,
 * which the stock action reads as every cache) and names a Lance backed
 * index, this filter fans a {@link LanceRequestCacheClearAction} out to
 * every node to drop that index's entries, then lets the stock action
 * proceed so the shard caches are cleared as before. The fan out's
 * outcome does not change the stock response: a node that failed to
 * answer is logged, and the caller's request completes on the stock
 * action's answer.
 */
public final class LanceClearCacheActionFilter implements ActionFilter {

    private static final Logger LOGGER = LogManager.getLogger(LanceClearCacheActionFilter.class);

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Client client;

    public LanceClearCacheActionFilter(
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.client = client;
    }

    @Override
    public int order() {
        // After the security plugin's filter (Integer.MIN_VALUE), which
        // authorises the stock clear request the fan out below follows.
        return Integer.MIN_VALUE + 100;
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
        if (!ClearIndicesCacheAction.NAME.equals(action) || !(request instanceof ClearIndicesCacheRequest clear)) {
            chain.proceed(task, action, request, listener);
            return;
        }
        if (!clearsRequestCache(clear)) {
            chain.proceed(task, action, request, listener);
            return;
        }
        List<String> uuids = lanceBackedUuids(clear);
        if (uuids.isEmpty()) {
            chain.proceed(task, action, request, listener);
            return;
        }
        client.execute(LanceRequestCacheClearAction.INSTANCE, new LanceRequestCacheClearRequest(uuids), ActionListener.wrap(response -> {
            if (!response.failures().isEmpty()) {
                LOGGER.warn(
                    "lance.request_cache: {} node(s) did not answer the clear of {}: {}",
                    response.failures().size(),
                    Arrays.toString(clear.indices()),
                    response.failures().get(0).getMessage()
                );
            } else {
                LOGGER.debug("lance.request_cache: cleared {} entries of {}", response.dropped(), Arrays.toString(clear.indices()));
            }
            chain.proceed(task, action, request, listener);
        }, e -> {
            LOGGER.warn("lance.request_cache: could not clear the result cache of {}: {}", Arrays.toString(clear.indices()), e.toString());
            chain.proceed(task, action, request, listener);
        }));
    }

    /**
     * Whether the stock action would clear the shard request cache for
     * this request: it was asked for, or no cache was named (the stock
     * action then clears every cache), the same reading
     * {@code IndicesService.clearIndexShardCache} applies.
     */
    static boolean clearsRequestCache(ClearIndicesCacheRequest request) {
        if (request.requestCache()) {
            return true;
        }
        return !request.queryCache() && !request.fieldDataCache() && !request.fileCache() && request.fields().length == 0;
    }

    /** The uuids of the Lance backed indexes the request resolves to; empty when it resolves to none or cannot be resolved. */
    private List<String> lanceBackedUuids(ClearIndicesCacheRequest request) {
        Index[] concrete;
        try {
            concrete = indexNameExpressionResolver.concreteIndices(clusterService.state(), request);
        } catch (Exception e) {
            // The stock action reports the resolution error itself.
            return List.of();
        }
        Metadata metadata = clusterService.state().metadata();
        List<String> uuids = new ArrayList<>();
        for (Index index : concrete) {
            IndexMetadata indexMetadata = metadata.index(index);
            if (indexMetadata == null) {
                continue;
            }
            String table = indexMetadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
            if (table != null && !table.isEmpty()) {
                uuids.add(index.getUUID());
            }
        }
        return uuids;
    }
}
