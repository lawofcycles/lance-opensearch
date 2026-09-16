/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.tasks.Task;

/**
 * ActionFilter that intercepts {@code indices:data/read/search} for
 * Lance-backed indexes when {@code lance.dispatch.mode} is set to
 * {@code fragment}, and short-circuits the standard shard fan-out.
 *
 * <p>Milestone 1 of the shard-free dispatch prototype. The filter's job
 * at this milestone is deliberately narrow: prove that the intercept
 * fires, that non-Lance indexes and mixed requests are left untouched,
 * and that the short-circuited response can be returned through the
 * standard {@link ActionListener}. Real fragment dispatch and
 * per-node execution land in the next milestones; this class currently
 * emits an empty {@link SearchResponse} so the intercept can be
 * exercised end-to-end.
 *
 * <p>The filter reads {@link org.opensearch.lance.LancePlugin#LANCE_DISPATCH_MODE_SETTING}
 * on every call so runtime updates via cluster settings take effect
 * without a restart. When the mode is {@code shard} (default) or the
 * request touches even one non-Lance index, the filter simply calls
 * {@code chain.proceed(...)} and lets OpenSearch's normal machinery
 * run.
 */
public class LanceDispatchActionFilter implements ActionFilter {

    private static final Logger LOGGER = LogManager.getLogger(LanceDispatchActionFilter.class);

    /** Transport action name for the top-level search request. */
    private static final String SEARCH_ACTION_NAME = "indices:data/read/search";

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * Current dispatch mode. Held as {@link AtomicReference} so the
     * cluster-settings update consumer can hot-swap the value without
     * synchronising the {@link #apply} hot path.
     */
    private final AtomicReference<LanceDispatchMode> mode;

    public LanceDispatchActionFilter(
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        LanceDispatchMode initialMode
    ) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.mode = new AtomicReference<>(initialMode);
    }

    /**
     * Called from the cluster-settings listener installed by
     * {@code LancePlugin.createComponents}. Only the raw string form
     * arrives from settings, so we parse it here.
     */
    public void setMode(String rawValue) {
        LanceDispatchMode next = LanceDispatchMode.parse(rawValue);
        LanceDispatchMode previous = mode.getAndSet(next);
        if (previous != next) {
            LOGGER.info("lance.dispatch.mode changed [{} -> {}]", previous, next);
        }
    }

    public LanceDispatchMode currentMode() {
        return mode.get();
    }

    @Override
    public int order() {
        // Run before OpenSearch's built-in search resolution filters
        // fire so the short-circuit avoids paying their cost when we
        // are going to bypass shard fan-out anyway. Integer.MIN_VALUE
        // is reserved for security, so keep some headroom.
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
        if (mode.get() != LanceDispatchMode.FRAGMENT || !SEARCH_ACTION_NAME.equals(action) || !(request instanceof SearchRequest)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        SearchRequest searchRequest = (SearchRequest) request;
        if (!allTargetIndexesAreLanceBacked(searchRequest)) {
            // Mixed and non-Lance requests continue on the standard
            // shard fan-out. Fragment-level dispatch for mixed queries
            // is deferred to a later milestone; the current milestone
            // is only concerned with fully Lance-backed requests.
            chain.proceed(task, action, request, listener);
            return;
        }

        LOGGER.info("lance.dispatch.mode=fragment: intercepting search for Lance-backed indices {}", (Object) searchRequest.indices());

        @SuppressWarnings("unchecked")
        Response response = (Response) buildStubResponse();
        listener.onResponse(response);
    }

    /**
     * Check whether every concrete index resolved from the request is
     * Lance-backed. A concrete index is Lance-backed if its metadata
     * carries a non-empty {@link LanceEngineFactory#TABLE_SETTING}
     * value; that setting is where {@code RestAttachAction} and the
     * namespace poller stamp the Lance table URI onto the OpenSearch
     * index at creation time.
     *
     * <p>The check runs against a {@link SetOnce}-captured cluster
     * state snapshot so a state change mid-request does not swap the
     * answer under us. On failure to resolve any index (deleted between
     * request submission and this filter running) we conservatively
     * return {@code false} so the standard code path can produce the
     * appropriate {@code IndexNotFoundException}.
     */
    private boolean allTargetIndexesAreLanceBacked(SearchRequest searchRequest) {
        Metadata metadata = clusterService.state().metadata();
        Index[] concrete;
        try {
            concrete = indexNameExpressionResolver.concreteIndices(clusterService.state(), searchRequest);
        } catch (Exception e) {
            // Unresolvable indices (missing, closed, etc.) drop us to
            // the standard path so the user sees the usual OpenSearch
            // error, not a silent no-op.
            return false;
        }
        if (concrete.length == 0) {
            return false;
        }
        for (Index index : concrete) {
            IndexMetadata indexMetadata = metadata.index(index);
            if (indexMetadata == null) {
                return false;
            }
            String tableSetting = indexMetadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
            if (tableSetting == null || tableSetting.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Empty response used for Milestone 1 of the shard-free prototype.
     * Real query dispatch and result aggregation land in later
     * milestones; this exists purely so the intercept can be exercised
     * end-to-end and integration tests can distinguish a fragment-mode
     * response from a shard-mode response.
     */
    private SearchResponse buildStubResponse() {
        SearchHits emptyHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
        SearchResponseSections sections = new SearchResponseSections(emptyHits, null, null, false, false, null, 1);
        return new SearchResponse(
            sections,
            null,
            /* totalShards */ 1,
            /* successfulShards */ 1,
            /* skippedShards */ 0,
            /* tookInMillis */ 0L,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }
}
