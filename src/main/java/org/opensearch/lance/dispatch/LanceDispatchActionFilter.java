/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

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
import org.opensearch.lance.dispatch.LanceMetricAggregator.MetricSpec;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.query.LanceKnnFilterTranslator;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.Client;

/**
 * ActionFilter that intercepts {@code indices:data/read/search} for
 * Lance-backed indexes when {@code lance.dispatch.mode} is set to
 * {@code fragment}, and hands the request off to the plugin's own
 * shard-free coordinator instead of OpenSearch's standard shard
 * fan-out.
 *
 * <p>Milestone 5-C3 of the shard-free dispatch prototype. The
 * filter now owns three responsibilities:
 * <ul>
 *   <li>Recognise whether the request is fragment-dispatchable by
 *       checking the target indexes ({@link #allLanceBacked}), the
 *       top-level query ({@link LanceKnnFilterTranslator}), the
 *       aggregations block ({@link LanceMetricAggregator#parseSupported}),
 *       and the presence of features the coordinator does not yet
 *       handle (sorts, from &gt; 0, search_after, highlighter,
 *       suggester, post_filter, or cross-index metrics).</li>
 *   <li>Delegate the request to {@link LanceCoordinatorAction} via
 *       {@link Client#execute(org.opensearch.action.ActionType,
 *       org.opensearch.action.ActionRequest, ActionListener)}.
 *       The coordinator has {@code TransportService} injected and
 *       can fan out the fragment-level work to every data node;
 *       single-node clusters take the same path with a fan-out of
 *       one local hop.</li>
 *   <li>Fall through to the standard shard fan-out via
 *       {@code chain.proceed} for anything else (dispatch mode
 *       {@code shard}, non-Lance targets, unsupported query or
 *       aggregation shapes, or cross-index requests with metrics).</li>
 * </ul>
 *
 * <p>The heavy lifting — opening the Lance dataset, enumerating
 * fragments, grouping them by node, scanning, and merging partials
 * — moves to {@link TransportLanceCoordinatorAction} and
 * {@link TransportLanceFragmentQueryAction}. The filter is now
 * effectively a routing switch.
 */
public class LanceDispatchActionFilter implements ActionFilter {

    private static final Logger LOGGER = LogManager.getLogger(LanceDispatchActionFilter.class);

    /** Transport action name for the top-level search request. */
    private static final String SEARCH_ACTION_NAME = "indices:data/read/search";

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Client client;

    /**
     * Current dispatch mode. Held as {@link AtomicReference} so the
     * cluster-settings update consumer can hot-swap the value without
     * synchronising the {@link #apply} hot path.
     */
    private final AtomicReference<LanceDispatchMode> mode;

    public LanceDispatchActionFilter(
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        LanceDispatchMode initialMode
    ) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.client = client;
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
        Index[] concrete = resolveConcreteIndexes(searchRequest);
        if (concrete == null || concrete.length == 0 || !allLanceBacked(concrete)) {
            // Mixed and non-Lance requests continue on the standard
            // shard fan-out.
            chain.proceed(task, action, request, listener);
            return;
        }

        Optional<String> filter = resolveDispatchFilter(searchRequest);
        if (filter.isEmpty()) {
            // Sorts / from > 0 / search_after / highlighter /
            // suggester / post_filter, or a top-level query builder
            // outside the LanceKnnFilterTranslator whitelist. Fall
            // through so the standard path can still answer.
            chain.proceed(task, action, request, listener);
            return;
        }

        Optional<List<MetricSpec>> metrics = LanceMetricAggregator.parseSupported(searchRequest.source());
        if (metrics.isEmpty()) {
            // Aggregation shape the fragment executor cannot answer
            // yet (bucket, script, missing-value, sub-aggregation,
            // or a metric on a non-ValuesSource builder).
            chain.proceed(task, action, request, listener);
            return;
        }

        if (!metrics.get().isEmpty() && concrete.length > 1) {
            // Cross-index metric aggregation needs a partial-reduce
            // path (Milestone 5-D) that merges partials across
            // independent Lance datasets. Until then multi-index
            // metric requests route through the shard path.
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
     * Decide whether the request can be answered by the fragment
     * executor and, if so, what SQL filter to push into Lance.
     *
     * <p>The three possible outcomes are:
     * <ul>
     *   <li>{@link Optional#empty()} — the request carries features
     *       the fragment executor cannot answer yet (sorts,
     *       from &gt; 0, search_after, highlighter, suggester,
     *       post_filter) or its top-level query is outside the
     *       {@link LanceKnnFilterTranslator} whitelist. The caller
     *       falls through to the standard shard path.</li>
     *   <li>{@code Optional.of("")} — match_all or an empty request
     *       body. Signals to the coordinator that no filter needs to
     *       be pushed into Lance.</li>
     *   <li>{@code Optional.of(sql)} — the top-level query is a
     *       {@code term}, {@code terms}, {@code exists},
     *       {@code range}, or {@code bool} combination of those,
     *       already translated to Lance SQL and ready for the
     *       coordinator to feed to {@code Dataset.countRows(sql)}
     *       and {@code ScanOptions.filter(sql)}.</li>
     * </ul>
     *
     * <p>A translator failure on a nested clause is treated as
     * "not dispatchable" rather than a request error: the shard path
     * can still answer the query.
     */
    private Optional<String> resolveDispatchFilter(SearchRequest searchRequest) {
        SearchSourceBuilder source = searchRequest.source();
        if (source == null) {
            return Optional.of("");
        }
        if (source.sorts() != null
            || source.suggest() != null
            || source.highlighter() != null
            || source.postFilter() != null
            || source.searchAfter() != null
            || source.from() > 0) {
            return Optional.empty();
        }
        QueryBuilder query = source.query();
        if (query == null || query instanceof MatchAllQueryBuilder) {
            return Optional.of("");
        }
        if (query instanceof TermQueryBuilder
            || query instanceof TermsQueryBuilder
            || query instanceof ExistsQueryBuilder
            || query instanceof RangeQueryBuilder
            || query instanceof BoolQueryBuilder) {
            try {
                return Optional.of(LanceKnnFilterTranslator.toLanceSql(query));
            } catch (IllegalArgumentException e) {
                LOGGER.debug("fragment dispatch declined for query [{}]: {}", query.getName(), e.getMessage());
                return Optional.empty();
            }
        }
        return Optional.empty();
    }
}
