/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.dispatch.LanceAggregationSupport;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.cost.CostInputs;
import org.opensearch.lance.plan.execute.PlanExecutor;
import org.opensearch.lance.plan.execute.RequestPlanner;
import org.opensearch.lance.plan.rel.ShardPathReason;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Serves {@link LanceExplainAction} on the coordinating node: resolves
 * the index from cluster state (404 unknown, 400 not Lance backed),
 * builds the planner's model of the index through
 * {@link LanceSchemas#build}, and plans the search body exactly as the
 * fragment coordinator would, so the answer is the plan a search with
 * the same body executes. No Lance scan is issued and no request
 * executes through the planner.
 *
 * <p>The route comes first. A body holding an element only the shard
 * path serves ({@link SearchRequestToRel#shardPathReasons}) is planned
 * through {@link SearchRequestToRel#translateDispatch}, the same tree
 * the dispatch filter reads its routing decision from, and answers with
 * the {@code ShardPathFallbackExec} root and the reasons; nothing else
 * is planned for it. Every other body takes the fragment route: the
 * query is rewritten with the shard free {@link QueryRewriteContext}
 * the coordinator applies ({@link RequestPlanner#rewriteAtCoordinator}),
 * the {@link ExecutionShape} is built the way
 * the coordinator builds it (the pushdown setting and the structural
 * allow list decide whether the aggregation tree may plan into the
 * scan), and {@link RequestPlanner#plan} runs with the same
 * {@link CostInputs} the coordinator would plan with (the cluster's
 * data node count, the table URI's storage kind, this node's CPUs and
 * the two parallelism settings). The physical text is the coordinator
 * tree over a fan out of one request per data node; at execution the
 * width can differ when the table has fewer fragments than nodes or a
 * node's share exceeds the Lucene reader bound. The request accepts
 * every envelope the runtime accepts; the only refusal left is the one
 * the runtime answers with the same 400, a filtered {@code lance_knn}
 * whose filter has no Lance SQL form. The aggregation allow list and
 * the multi index checks the dispatch filter applies outside the plan
 * are not reflected here.
 *
 * <p>Threading: the cluster state lookup runs wherever the request
 * arrives; the model build and the planning are handed to the plugin's
 * {@code lance_coordinator} pool because building the model on a cold
 * node opens the Lance table (metadata I/O) and the planning is CPU work
 * that does not belong on a transport thread. The transport handler
 * registers on {@code SAME} so a remote request is not bounced through
 * the pool once for the handler and again for the explicit fork; the
 * fork inside {@link #doExecute} is the single hop for local and remote
 * callers alike.
 *
 * <p>The cost budgets handed to {@link LancePlannerFactory} (the node's
 * {@code lance.native_memory.limit} and the JVM's max heap) feed the
 * cost ordering the Volcano run compares candidates with; nothing
 * predicts real byte usage yet, so they act as placeholders.
 */
public final class TransportLanceExplainAction extends HandledTransportAction<LanceExplainRequest, LanceExplainResponse> {

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final LanceWarmCache warmCache;
    private final LancePlannerFactory plannerFactory;

    @Inject
    public TransportLanceExplainAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        ClusterService clusterService,
        IndicesService indicesService,
        LanceWarmCache warmCache,
        Settings settings
    ) {
        super(LanceExplainAction.NAME, transportService, actionFilters, LanceExplainRequest::new, ThreadPool.Names.SAME);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.warmCache = warmCache;
        long nativeBudgetBytes = NativeMemoryLimit.parse(
            LancePlugin.NATIVE_MEMORY_LIMIT_SETTING.get(settings),
            LancePlugin.NATIVE_MEMORY_LIMIT_SETTING.getKey()
        );
        this.plannerFactory = new LancePlannerFactory(nativeBudgetBytes, Runtime.getRuntime().maxMemory());
    }

    @Override
    protected void doExecute(Task task, LanceExplainRequest request, ActionListener<LanceExplainResponse> listener) {
        String indexName = request.index();
        IndexMetadata metadata = clusterService.state().metadata().index(indexName);
        if (metadata == null) {
            listener.onFailure(new IndexNotFoundException(indexName));
            return;
        }
        String tableUri = metadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
        if (tableUri == null || tableUri.isEmpty()) {
            listener.onFailure(new IllegalArgumentException("index " + indexName + " is not a Lance index"));
            return;
        }
        threadPool.executor(LancePlugin.LANCE_COORDINATOR_THREAD_POOL)
            .execute(ActionRunnable.supply(listener, () -> explain(metadata, request.source())));
    }

    private LanceExplainResponse explain(IndexMetadata metadata, SearchSourceBuilder source) throws IOException {
        String indexName = metadata.getIndex().getName();
        LanceSchemas.IndexModel model = LanceSchemas.build(metadata, warmCache);
        String tableUri = metadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
        CostInputs inputs = RequestPlanner.clusterInputs(dataNodes(), tableUri, clusterService.getClusterSettings());

        List<ShardPathReason> reasons = SearchRequestToRel.shardPathReasons(source);
        if (!reasons.isEmpty()) {
            RelNode logical = SearchRequestToRel.translateDispatch(source, model, plannerFactory);
            String logicalText = RelOptUtil.toString(logical);
            RelNode physical = plannerFactory.plan(logical, inputs);
            return LanceExplainResponse.shardPath(indexName, reasons, logicalText, RelOptUtil.toString(physical));
        }

        LanceOverrides overrides = LanceOverrides.of(metadata.getSettings());
        QueryBuilder query = RequestPlanner.rewriteAtCoordinator(
            indicesService,
            source == null ? null : source.query(),
            System.currentTimeMillis()
        );
        boolean planAggregations = source != null
            && source.aggregations() != null
            && clusterService.getClusterSettings().get(LancePlugin.AGGREGATION_PUSHDOWN_SETTING)
            && LanceAggregationSupport.isPushdownCandidate(source.aggregations());
        ExecutionShape shape = ExecutionShape.of(source, query, planAggregations);
        RequestPlanner.Planned planned = RequestPlanner.plan(
            shape,
            model,
            PlanExecutor.sqlExcludedColumns(overrides),
            plannerFactory,
            inputs
        );
        String logicalText = RelOptUtil.toString(planned.logical());
        String physicalText = RelOptUtil.toString(planned.coordinatorPlan(shape, inputs.nodes()));
        boolean readerWrapper = ReaderWrapperProbe.installed(indicesService, metadata.getIndex());
        return LanceExplainResponse.fragment(
            indexName,
            logicalText,
            physicalText,
            planned.plan(),
            planned.unplanned(),
            ExplainRefinements.predict(planned.plan(), readerWrapper, overrides.ipColumns())
        );
    }

    /** The data nodes a search would fan out to, at least one so a cluster without them still explains. */
    private int dataNodes() {
        return Math.max(1, clusterService.state().nodes().getDataNodes().size());
    }
}
