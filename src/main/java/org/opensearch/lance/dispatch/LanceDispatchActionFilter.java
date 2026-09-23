/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
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
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.execute.PlanExecutor;
import org.opensearch.lance.plan.rel.physical.ShardPathFallbackExec;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
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
 *       {@link LanceAggregationSupport#isSupported}). The shape
 *       decision is planned: {@link SearchRequestToRel#translateDispatch}
 *       marks a body holding an element the fragment executor cannot
 *       answer correctly — suggester, highlighter, {@code collapse},
 *       {@code rescore}, pipeline aggregations — and the planner
 *       answers such a body with a {@link ShardPathFallbackExec}
 *       root, which routes the request to the shard path.</li>
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
 *       taken over; it is never used because of load. A Lance-backed
 *       target whose table has more rows than one Lucene reader may
 *       hold is not handed to it (the shard reader holds part of the
 *       table); such a request fails with 400 instead
 *       ({@link #proceedOnShardPath}).</li>
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
    private final PlanExecutor planExecutor;

    /**
     * The planner model the dispatch decision plans against. The
     * decision depends only on the request body's envelope, never on
     * the target's schema, so one synthetic single-column model serves
     * every request and no Lance dataset is opened on the transport
     * thread this filter runs on.
     */
    private static final LanceSchemas.IndexModel DISPATCH_MODEL = LanceSchemas.model(
        "dispatch",
        new Schema(List.of(new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null))),
        Map.of(),
        () -> 0L
    );

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
        long nativeBudgetBytes = NativeMemoryLimit.parse(
            LancePlugin.NATIVE_MEMORY_LIMIT_SETTING.get(clusterService.getSettings()),
            LancePlugin.NATIVE_MEMORY_LIMIT_SETTING.getKey()
        );
        this.planExecutor = new PlanExecutor(new LancePlannerFactory(nativeBudgetBytes, Runtime.getRuntime().maxMemory()));
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

        RelNode dispatchPlan = planDispatch(searchRequest);
        if (!isDispatchable(dispatchPlan)) {
            // The planner answered the body with a shard path fallback:
            // it carries an element (suggest, highlighter, collapse,
            // rescore, a pipeline aggregation) the fragment executor
            // does not serve. Fall through so the
            // standard path can still answer.
            planExecutor.executeShardPath(dispatchPlan, () -> proceedOnShardPath(task, action, request, listener, chain, concrete));
            return;
        }

        if (!LanceAggregationSupport.isSupported(searchRequest.source())) {
            // Aggregation shape the fragment executor has not taken
            // over yet (a script, a type off the allow list, a filter
            // bucket over a Lance query, a pipeline).
            proceedOnShardPath(task, action, request, listener, chain, concrete);
            return;
        }

        if (LanceAggregationSupport.hasAggregations(searchRequest.source()) && concrete.length > 1) {
            // Cross-index aggregation needs a partial-reduce path that
            // merges partials across independent Lance datasets. Until
            // then multi-index aggregation requests route through the
            // shard path.
            proceedOnShardPath(task, action, request, listener, chain, concrete);
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
     * Hand a request over Lance-backed indexes whose shape the fragment
     * path does not serve to the shard path, unless a target table has
     * more rows than one Lucene reader may hold. The shard engine's
     * reader of such a table holds the leading fragments that fit
     * ({@link org.opensearch.lance.engine.LanceDirectoryReader}), so the
     * shard path would answer from part of the table without saying so;
     * the request fails with 400 instead, naming the rows the table has
     * and the rows the shard reader holds.
     *
     * <p>Deciding that means reading each table's manifest, which is
     * Lance I/O and does not belong on the transport thread this filter
     * runs on, so the check and the {@code chain.proceed} it may end in
     * run on the {@code lance_coordinator} pool, the pool the fragment
     * path's entry runs on. The pool preserves the thread context, so
     * the shard path sees the caller's headers as it would from here.
     * A refused fork fails the request with the pool's rejection (HTTP
     * 429), as for the fragment path.
     */
    private <Request extends ActionRequest, Response extends ActionResponse> void proceedOnShardPath(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain,
        Index[] concrete
    ) {
        AbstractRunnable check = new AbstractRunnable() {
            @Override
            protected void doRun() {
                long maxDocs = clusterService.getClusterSettings().get(LancePlugin.MAX_DOCS_PER_READER_SETTING);
                Metadata metadata = clusterService.state().metadata();
                for (Index index : concrete) {
                    IndexMetadata indexMetadata = metadata.index(index);
                    if (indexMetadata == null) {
                        continue;
                    }
                    TransportLanceCoordinatorAction.ReaderBound bound = TransportLanceCoordinatorAction.readerBound(indexMetadata, maxDocs);
                    if (bound.exceeded()) {
                        throw new IllegalArgumentException(
                            "table of index ["
                                + index.getName()
                                + "] has "
                                + bound.tableRows()
                                + " rows, above the Lucene bound of "
                                + maxDocs
                                + " rows per reader; this request shape is served by the shard path and would see only "
                                + bound.readerRows()
                                + " rows. Use a shape the fragment path serves (see docs/limitations.md)"
                        );
                    }
                }
                chain.proceed(task, action, request, listener);
            }

            @Override
            public void onRejection(Exception e) {
                LOGGER.warn("shard path entry rejected for {}; returning 429: {}", Arrays.toString(concrete), e.getMessage());
                listener.onFailure(e);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public String toString() {
                return "lance dispatch shard path entry for " + Arrays.toString(concrete);
            }
        };
        try {
            threadPool.executor(LancePlugin.LANCE_COORDINATOR_THREAD_POOL).execute(check);
        } catch (Exception e) {
            listener.onFailure(e);
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
     * Plan the request's dispatch decision. The body translates
     * through {@link SearchRequestToRel#translateDispatch}, which marks
     * a body holding an element only the shard path serves with the
     * shard path shape node, and the Volcano run lowers that shape to
     * the {@link ShardPathFallbackExec} operator carrying the fallback
     * reasons; a body without such an element plans to the Lance scan.
     * Planning is pure computation over the already-parsed body — no
     * Lance dataset is opened and no I/O runs — so it is safe on the
     * transport thread this filter runs on.
     *
     * <p>Accepted shapes (fragment path answers end-to-end) include
     * any top-level query the local {@link
     * org.opensearch.index.query.QueryShardContext} can translate
     * (match on {@code lance_text}, knn on {@code lance_knn}, term /
     * terms / range / exists / bool combinations, ...): the receiving
     * node ships the {@link QueryBuilder} across the wire and
     * re-parses it via {@code QueryShardContext.toQuery}, so the
     * dispatch decision never depends on the planner spelling the
     * query itself; sort clauses; and aggregations that pass
     * {@link LanceAggregationSupport#isSupported}. The rejected
     * elements and their rationale live on
     * {@link org.opensearch.lance.plan.rel.ShardPathReason}.
     */
    private RelNode planDispatch(SearchRequest searchRequest) {
        LancePlannerFactory plannerFactory = planExecutor.plannerFactory();
        return plannerFactory.plan(SearchRequestToRel.translateDispatch(searchRequest.source(), DISPATCH_MODEL, plannerFactory));
    }

    /**
     * Whether the fragment executor can answer the planned request: a
     * {@link ShardPathFallbackExec} root is the planner's decision
     * that only the standard shard search path serves it.
     */
    private static boolean isDispatchable(RelNode dispatchPlan) {
        return !(dispatchPlan instanceof ShardPathFallbackExec);
    }
}
