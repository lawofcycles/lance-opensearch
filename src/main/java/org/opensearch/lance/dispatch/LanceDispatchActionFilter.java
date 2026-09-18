/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.query.LanceKnnFilterTranslator;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.Client;

/**
 * ActionFilter that intercepts {@code indices:data/read/search} for
 * Lance-backed indices and hands the request off to the plugin's own
 * shard-free coordinator instead of OpenSearch's standard shard
 * fan-out.
 *
 * <p>The filter owns three responsibilities:
 * <ul>
 *   <li>Recognise whether the request is fragment-dispatchable
 *       ({@link #allLanceBacked} plus {@link #isDispatchable} plus
 *       {@link LanceAggregationSupport#isSupported}) — reject shapes
 *       the fragment executor cannot yet answer (search_after,
 *       highlighter, suggester, post_filter, or
 *       cross-index metrics).</li>
 *   <li>Delegate the request to {@link LanceCoordinatorAction} via
 *       {@link Client#execute(org.opensearch.action.ActionType,
 *       org.opensearch.action.ActionRequest, ActionListener)}.
 *       The coordinator has {@code TransportService} injected and
 *       can fan out the fragment-level work to every data node;
 *       single-node clusters take the same path with a fan-out of
 *       one local hop.</li>
 *   <li>Fall through to the standard shard fan-out via
 *       {@code chain.proceed} for anything else (non-Lance targets,
 *       unsupported query or aggregation shapes, cross-index
 *       requests with metrics). The shard path still exists as a
 *       safety net for shapes the fragment executor has not yet
 *       taken over.</li>
 * </ul>
 *
 * <p>The heavy lifting — opening the Lance dataset, enumerating
 * fragments, grouping them by node, scanning, and merging partials
 * — lives in {@link TransportLanceCoordinatorAction} and
 * {@link TransportLanceFragmentQueryAction}. The filter is
 * effectively a routing switch.
 */
public class LanceDispatchActionFilter implements ActionFilter {

    private static final Logger LOGGER = LogManager.getLogger(LanceDispatchActionFilter.class);

    /** Transport action name for the top-level search request. */
    private static final String SEARCH_ACTION_NAME = "indices:data/read/search";

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Client client;

    public LanceDispatchActionFilter(
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
        if (!SEARCH_ACTION_NAME.equals(action) || !(request instanceof SearchRequest)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        SearchRequest searchRequest = (SearchRequest) request;
        Index[] concrete = resolveConcreteIndexes(searchRequest);
        if (concrete == null || concrete.length == 0 || !allLanceBacked(concrete)) {
            // Mixed and non-Lance requests continue on the standard
            // shard fan-out.
            chain.proceed(task, action, request, listener);
            return;
        }

        if (!isDispatchable(searchRequest)) {
            // search_after / highlighter / suggester / post_filter,
            // or a top-level query builder outside the fragment
            // executor's supported shape. Fall through so the
            // standard path can still answer.
            chain.proceed(task, action, request, listener);
            return;
        }

        if (!LanceAggregationSupport.isSupported(searchRequest.source())) {
            // Aggregation shape the fragment executor cannot answer
            // yet (scripts, missing values, sub-aggregations, or a
            // metric on a non-ValuesSource builder).
            chain.proceed(task, action, request, listener);
            return;
        }

        if (LanceAggregationSupport.hasAggregations(searchRequest.source()) && concrete.length > 1) {
            // Cross-index aggregation needs a partial-reduce path
            // (Milestone 5-D) that merges partials across independent
            // Lance datasets. Until then multi-index aggregation
            // requests route through the shard path.
            chain.proceed(task, action, request, listener);
            return;
        }

        try {
            // Delegate to the coordinator transport action. It has
            // TransportService injected and can fan out fragment
            // queries to every data node. In single-node clusters
            // the fan-out reduces to a local executeLocally hop so
            // the same code path serves both.
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> typedListener = (ActionListener<SearchResponse>) listener;
            client.execute(LanceCoordinatorAction.INSTANCE, searchRequest, typedListener);
        } catch (Exception e) {
            LOGGER.warn("fragment dispatch failed for {}; falling back to shard path", (Object) searchRequest.indices(), e);
            chain.proceed(task, action, request, listener);
        }
    }

    /**
     * Resolve the request's index expressions against the current
     * cluster state, returning {@code null} on failure so the caller
     * can drop back to the standard code path and surface the usual
     * OpenSearch error rather than a silent no-op.
     */
    private Index[] resolveConcreteIndexes(SearchRequest searchRequest) {
        try {
            return indexNameExpressionResolver.concreteIndices(clusterService.state(), searchRequest);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * True when every concrete index is Lance-backed, judged by the
     * presence of {@link LanceEngineFactory#TABLE_SETTING} on the
     * index metadata. That setting is stamped by
     * {@code RestAttachAction} and the namespace poller at index
     * creation time.
     */
    private boolean allLanceBacked(Index[] concrete) {
        Metadata metadata = clusterService.state().metadata();
        for (Index index : concrete) {
            IndexMetadata indexMetadata = metadata.index(index);
            if (indexMetadata == null) {
                return false;
            }
            Settings settings = indexMetadata.getSettings();
            String tableSetting = settings.get(LanceEngineFactory.TABLE_SETTING);
            if (tableSetting == null || tableSetting.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Decide whether the fragment executor can answer this request.
     *
     * <p>Rejected shapes (fall through to shard path):
     * <ul>
     *   <li>{@code suggest}, {@code highlighter} — need FTS
     *       positional / candidate APIs Lance does not surface
     *       yet.</li>
     *   <li>{@code search_after} without {@code sort} — the per-fragment
     *       executor drives {@link
     *       org.apache.lucene.search.IndexSearcher#searchAfter} which
     *       requires a matching Sort. Without one the shard path's
     *       score-order search_after is used instead.</li>
     * </ul>
     *
     * <p>Accepted shapes (fragment path answers end-to-end):
     * <ul>
     *   <li>any top-level query the local {@link
     *       org.opensearch.index.query.QueryShardContext} can translate
     *       (match on {@code lance_text}, knn on {@code lance_knn},
     *       term / terms / range / exists / bool combinations, ...).
     *       The receiving node ships the {@link QueryBuilder} across
     *       the wire and re-parses it via {@code
     *       QueryShardContext.toQuery}, so per-node mapping
     *       decisions apply. The coordinator additionally translates
     *       pure-filter shapes into Lance SQL for metadata-only row
     *       counting, but the accept/reject decision does not
     *       depend on that translation succeeding.</li>
     *   <li>sort clauses — the per-node executor drives {@link
     *       org.apache.lucene.search.IndexSearcher#search(org.apache.lucene.search.Query,
     *       int, org.apache.lucene.search.Sort)} and each hit carries
     *       its sort values back for the coordinator merge.</li>
     *   <li>aggregations that pass {@link
     *       LanceAggregationSupport#isSupported}.</li>
     * </ul>
     */
    private boolean isDispatchable(SearchRequest searchRequest) {
        SearchSourceBuilder source = searchRequest.source();
        if (source == null) {
            return true;
        }
        if (source.suggest() != null || source.highlighter() != null) {
            return false;
        }
        // search_after depends on sort — Lucene's searchAfter takes a
        // FieldDoc whose fields correspond to the Sort clauses. A
        // score-order search_after is a shard-path shape.
        if (source.searchAfter() != null && (source.sorts() == null || source.sorts().isEmpty())) {
            return false;
        }
        return true;
    }
}
