/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Arrays;
import java.util.List;

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
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.engine.LanceEngineFactory;
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
 *   <li>Recognise whether every target is Lance backed
 *       ({@link #allLanceBacked}). Nothing about the request body
 *       takes part in that decision: the coordinator plans every body
 *       over a Lance backed target, and a body no plan answers
 *       ({@code suggest}, {@code highlight}) is refused there with 400
 *       naming the element
 *       ({@code SearchRequestToRel.checkEnvelopeSupported}), as an
 *       aggregation the executors cannot run is refused by the
 *       coordinator's translator ({@code AggregationToRel.checkExecutable}).</li>
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
 *       {@code chain.proceed} when a target is not Lance backed,
 *       alone or next to Lance backed ones. That is the only request
 *       over a Lance backed index the stock search action still
 *       serves; a Lance backed target alone never leaves the
 *       fragment path, whatever its body. Until every request shape
 *       ran on the fragment executors, a body they did not serve
 *       (a suggester, a highlighter, aggregation types off an allow
 *       list) proceeded here onto the stock action over the shard's
 *       whole table reader; that fallback route is gone.</li>
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

        String innerHitsPath = findNestedInnerHitsPath(searchRequest.source());
        if (innerHitsPath != null) {
            // Neither the fragment executor nor the shard fetch phase
            // materialises per-hit inner_hits for Lance-backed indices;
            // refuse loudly rather than return hits with the block
            // silently missing.
            listener.onFailure(
                new IllegalArgumentException(
                    "[inner_hits] on the nested query [path=" + innerHitsPath + "] is not supported for Lance-backed indices"
                )
            );
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
        // retried on the stock search action: that would run the whole
        // table through one node's shard under the very load that made
        // the fragment path refuse, and hide the overload from the
        // client.
        //
        // The coordinator's task is registered as a child of this
        // search task: cancelling the search task (a client that
        // closes its connection, _tasks/_cancel on
        // indices:data/read/search) then reaches the coordinator task,
        // and through it the per-node executor tasks, instead of
        // leaving them to run to the end.
        searchRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        @SuppressWarnings("unchecked")
        final ActionListener<SearchResponse> typedListener = (ActionListener<SearchResponse>) listener;
        AbstractRunnable entry = new AbstractRunnable() {
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
        };
        try {
            threadPool.executor(LancePlugin.LANCE_COORDINATOR_THREAD_POOL).execute(entry);
        } catch (Exception e) {
            // A rejection is delivered to entry.onRejection inside
            // execute and does not reach here. Anything execute throws
            // synchronously means the runnable was never accepted, so
            // the listener has not been completed yet; complete it
            // rather than leave the request open.
            listener.onFailure(e);
        }
    }

    /**
     * The path of the first {@link NestedQueryBuilder} in the request's
     * query or post_filter that carries an {@code inner_hits} block, or
     * {@code null} when there is none. Walks the compound builders a
     * nested clause can hide in (bool, boost, constant_score, dis_max).
     */
    private static String findNestedInnerHitsPath(SearchSourceBuilder source) {
        if (source == null) {
            return null;
        }
        String inQuery = findNestedInnerHitsPath(source.query());
        return inQuery != null ? inQuery : findNestedInnerHitsPath(source.postFilter());
    }

    private static String findNestedInnerHitsPath(QueryBuilder builder) {
        if (builder == null) {
            return null;
        }
        if (builder instanceof NestedQueryBuilder nested) {
            if (nested.innerHit() != null) {
                return nested.path();
            }
            return findNestedInnerHitsPath(nested.query());
        }
        if (builder instanceof BoolQueryBuilder bool) {
            for (List<QueryBuilder> clauses : List.of(bool.must(), bool.filter(), bool.should(), bool.mustNot())) {
                for (QueryBuilder clause : clauses) {
                    String path = findNestedInnerHitsPath(clause);
                    if (path != null) {
                        return path;
                    }
                }
            }
            return null;
        }
        if (builder instanceof BoostingQueryBuilder boosting) {
            String positive = findNestedInnerHitsPath(boosting.positiveQuery());
            return positive != null ? positive : findNestedInnerHitsPath(boosting.negativeQuery());
        }
        if (builder instanceof ConstantScoreQueryBuilder constantScore) {
            return findNestedInnerHitsPath(constantScore.innerQuery());
        }
        if (builder instanceof DisMaxQueryBuilder disMax) {
            for (QueryBuilder clause : disMax.innerQueries()) {
                String path = findNestedInnerHitsPath(clause);
                if (path != null) {
                    return path;
                }
            }
        }
        return null;
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
}
