/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Arrays;

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
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
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
 *       the fragment executor cannot answer correctly: suggester,
 *       highlighter, score-only {@code search_after}, {@code
 *       collapse}, {@code rescore}, pipeline aggregations, {@code
 *       min_score}, {@code terminate_after}, {@code stored_fields},
 *       {@code docvalue_fields}, {@code explain}, and cross-index
 *       metrics.</li>
 *   <li>Delegate the request to {@link LanceCoordinatorAction} via
 *       {@link Client#execute(org.opensearch.action.ActionType,
 *       org.opensearch.action.ActionRequest, ActionListener)} on the
 *       plugin's {@code lance_coordinator} thread pool. The
 *       coordinator has {@code TransportService} injected and can
 *       fan out the fragment-level work to every data node;
 *       single-node clusters take the same path with a fan-out of
 *       one local hop. When the pool refuses the request, the
 *       request fails with the pool's rejection (HTTP 429).</li>
 *   <li>Fall through to the standard shard fan-out via
 *       {@code chain.proceed} for anything else (non-Lance targets,
 *       unsupported query or aggregation shapes, cross-index
 *       requests with metrics). The shard path still exists as a
 *       safety net for shapes the fragment executor has not yet
 *       taken over; it is never used because of load.</li>
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
    private final ThreadPool threadPool;

    public LanceDispatchActionFilter(
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.client = client;
        this.threadPool = threadPool;
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
            // Cross-index aggregation needs a partial-reduce path that
            // merges partials across independent Lance datasets. Until
            // then multi-index aggregation requests route through the
            // shard path.
            chain.proceed(task, action, request, listener);
            return;
        }

        // Delegate to the coordinator transport action. It has
        // TransportService injected and can fan out fragment
        // queries to every data node. In single-node clusters
        // the fan-out reduces to a local executeLocally hop so
        // the same code path serves both.
        //
        // Fork onto the plugin's lance_coordinator pool before
        // entering the coordinator: this filter runs on the transport
        // worker that received the HTTP request, and
        // NodeClient.executeLocally invokes the coordinator's
        // doExecute inline (its request handler executor only fires
        // when the call arrives over the transport layer). Without
        // the fork, SQL translation, Lance dataset open and the
        // fan-out would all run on netty transport_worker threads,
        // stalling node I/O; core enforces this via
        // Transports.assertNotTransportThread on hot paths. The pool
        // is the plugin's own rather than `search` so the coordinator
        // side of a request never competes with the fragment
        // executors of a data node for the same queue.
        //
        // A rejected fork fails the request with the pool's
        // OpenSearchRejectedExecutionException (HTTP 429). It is not
        // retried on the shard path: that would run the whole table
        // through one node's shard under the very load that made the
        // fragment path refuse, and hide the overload from the client.
        @SuppressWarnings("unchecked")
        final ActionListener<SearchResponse> typedListener = (ActionListener<SearchResponse>) listener;
        threadPool.executor(LancePlugin.LANCE_COORDINATOR_THREAD_POOL).execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                client.execute(LanceCoordinatorAction.INSTANCE, searchRequest, typedListener);
            }

            @Override
            public void onRejection(Exception e) {
                LOGGER.warn("fragment dispatch rejected for {}; returning 429: {}", (Object) searchRequest.indices(), e.getMessage());
                listener.onFailure(e);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public String toString() {
                return "lance dispatch coordinator entry for " + Arrays.toString(searchRequest.indices());
            }
        });
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
        // collapse groups hits by a field and can materialise
        // inner_hits per group. The fragment executor does not
        // synthesise the CollapsingTopDocsCollector state, so hits
        // come back ungrouped and inner_hits disappear silently.
        // Send to the shard path instead of returning wrong hits.
        if (source.collapse() != null) {
            return false;
        }
        // rescore layers a second-pass query on top of the first
        // Sort/TopDocs window. The fragment executor drives a plain
        // IndexSearcher.search and never runs the rescorer, so
        // scores stay at their first-pass values. Send to the shard
        // path so users get either the rescored order or a proper
        // error, not silent scores.
        if (source.rescores() != null && !source.rescores().isEmpty()) {
            return false;
        }
        // Pipeline aggregations (sibling like avg_bucket / bucket_sort
        // and parent like cumulative_sum) hit an
        // "Already been replayed" IllegalStateException in the
        // coordinator merge because the fragment path replays the
        // InternalAggregations tree in a way the pipeline aggregators
        // don't expect. Route to the shard path where the standard
        // reduce loop handles them.
        if (source.aggregations() != null && hasPipelineAggregation(source.aggregations())) {
            return false;
        }
        // min_score filters hits by score threshold. The fragment
        // executor's hits + matched counting comes from Lance
        // metadata (or Lucene count), which sees every doc that
        // matches the query regardless of score. Passing the
        // request through would return hits above the threshold
        // but still report matched as the pre-filter total, so
        // send to the shard path where the built-in
        // MinScoreCollector actually clips.
        if (source.minScore() != null) {
            return false;
        }
        // terminate_after cuts the collector short after N docs on
        // each shard. Fragment path does not thread the terminate
        // count into its scan, so both hits.total.value and the
        // terminated_early flag would be silently wrong. Shard
        // path implements it directly via
        // EarlyTerminatingCollector.
        if (source.terminateAfter() > 0) {
            return false;
        }
        // track_total_hits (default 10,000 bound, `true`, `false`, or
        // an integer) is implemented by the fragment path: the
        // coordinator ships the bound to every executor, executors
        // stop counting past it, and the coordinator composes the
        // `eq` / `gte` relation or drops hits.total the way the shard
        // path's SearchPhaseController does. _count, which sends
        // track_total_hits: true with size 0, therefore takes this
        // path too.
        //
        // stored_fields projects a specific list of stored fields
        // per hit (or "_none_" to hide _source entirely). The
        // fragment executor materialises hits by copying the raw
        // _source bytes emitted by LanceFragmentLeafReader; the
        // stored_fields context is dropped, so a request that asks
        // for a stored field subset (or explicitly hides _source
        // with "_none_") gets the full _source back. Route to the
        // shard path where the fetch phase applies the projection.
        if (source.storedFields() != null) {
            return false;
        }
        // docvalue_fields loads named doc values into hits.fields.
        // The fragment executor does not populate hits.fields, so a
        // request that asks for docvalue_fields would come back
        // without them at all. Route to the shard path.
        if (source.docValueFields() != null && !source.docValueFields().isEmpty()) {
            return false;
        }
        // explain returns a per-hit scoring explanation. The
        // fragment executor drives IndexSearcher.search but never
        // calls searcher.explain, so a request with "explain":true
        // would come back without any _explanation field on the
        // hits. Route to the shard path.
        if (Boolean.TRUE.equals(source.explain())) {
            return false;
        }
        return true;
    }

    /**
     * Returns {@code true} if the aggregation tree contains any
     * pipeline aggregator, either at the top level (sibling pipelines
     * such as {@code avg_bucket}) or nested inside a bucket
     * aggregation (parent pipelines such as {@code cumulative_sum}
     * or {@code bucket_sort}).
     */
    private static boolean hasPipelineAggregation(AggregatorFactories.Builder aggs) {
        if (aggs == null) {
            return false;
        }
        if (!aggs.getPipelineAggregatorFactories().isEmpty()) {
            return true;
        }
        for (AggregationBuilder child : aggs.getAggregatorFactories()) {
            if (containsPipeline(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsPipeline(AggregationBuilder agg) {
        if (agg == null) {
            return false;
        }
        if (!agg.getPipelineAggregations().isEmpty()) {
            return true;
        }
        for (AggregationBuilder child : agg.getSubAggregations()) {
            if (containsPipeline(child)) {
                return true;
            }
        }
        return false;
    }
}
