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
import org.opensearch.lance.LanceMappingMeta;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.cost.CostInputs;
import org.opensearch.lance.plan.execute.PlanExecutor;
import org.opensearch.lance.plan.execute.RequestPlanner;
import org.opensearch.lance.plan.traits.UnmetPlanRequirementException;
import org.opensearch.lance.plan.translate.QueryToRex;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.lance.plan.translate.StockTextQueryRewriter;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Serves {@link LanceExplainAction} on the coordinating node: resolves
 * the index from cluster state (404 unknown, 400 not Lance backed),
 * builds the planner's model of the index through
 * {@link LanceSchemas#build}, and plans the search body exactly as the
 * fragment coordinator would, so the answer is the plan a search with
 * the same body executes. No Lance scan is issued and no request
 * executes through the planner.
 *
 * <p>The route comes first. A body carrying an element no plan answers
 * ({@link SearchRequestToRel#unsupportedElement}: {@code suggest},
 * {@code highlight}) answers {@code route: unsupported} with the message
 * the search endpoint refuses the same body with under
 * {@code unplanned}; nothing else is planned for it, and the endpoint
 * answers 200 because it reports rather than executes. Every other body
 * takes the fragment route: the
 * query is rewritten with the shard free {@link QueryRewriteContext}
 * the coordinator applies ({@link RequestPlanner#rewriteAtCoordinator})
 * and its stock full text clauses on {@code lance_text} fields become
 * Lance FTS clauses as on the coordinator ({@link StockTextQueryRewriter}),
 * the {@link ExecutionShape} is built the way
 * the coordinator builds it, and {@link RequestPlanner#plan} runs with
 * the same {@link CostInputs} the coordinator would plan with (the
 * cluster's data node count, the table URI's storage kind, this node's
 * CPUs, the two parallelism settings, and the aggregation routing
 * settings {@code lance.aggregation.pushdown} and
 * {@code lance.aggregation.pushdown_max_groups}, which the cost model
 * turns into an infinite pushed cost). The physical text is the coordinator
 * tree over a fan out of one request per data node; at execution the
 * width can differ when the table has fewer fragments than nodes or a
 * node's share exceeds the Lucene reader bound. The request accepts
 * every envelope the runtime accepts; the refusals left are the ones
 * the runtime answers with the same 400, a filtered {@code lance_knn}
 * whose filter has no Lance SQL form and an aggregation the executors
 * cannot run. A request whose trait demand no
 * plan meets, which the runtime answers 400 with a {@code plan_failed}
 * message, is described rather than refused: the answer carries the
 * cheapest plan the demand refused as {@code physical}, the message
 * under {@code unplanned}, no {@code fragment_plan}, and the refusal
 * under {@code traits.enforcer}. The multi index check the dispatch
 * filter applies outside the plan (a target that is not Lance backed
 * sends the whole request to the stock search action) is not reflected
 * here.
 *
 * <p>Both plan texts come from {@code PlanText}: every physical
 * operator line carries the {@code Accuracy} and {@code TieStability}
 * it declares and the cost the planner charged it, the root the total.
 * The {@code traits} object summarises the same for the plan: what the
 * request demanded, what the root declares and whether the enforcer
 * (the second Volcano pass with the demand on the root) fired.
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
        String unsupported = SearchRequestToRel.unsupportedElement(source);
        if (unsupported != null) {
            return LanceExplainResponse.unsupported(indexName, unsupported);
        }
        LanceOverrides overrides = LanceOverrides.of(metadata.getSettings());
        QueryBuilder query = StockTextQueryRewriter.rewrite(
            RequestPlanner.rewriteAtCoordinator(indicesService, source == null ? null : source.query(), System.currentTimeMillis()),
            LanceMappingMeta.lanceTextFields(metadata.mapping())
        );
        // The model reads the zone maps of the query's columns while it
        // holds the table, so the plan below prunes the same fragments
        // the coordinator's plan for this body would.
        LanceSchemas.IndexModel model = LanceSchemas.build(metadata, warmCache, QueryToRex.referencedFields(query));
        String tableUri = metadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
        CostInputs inputs = RequestPlanner.clusterInputs(dataNodes(), tableUri, clusterService.getClusterSettings());
        ExecutionShape shape = ExecutionShape.of(source, query);
        RequestPlanner.Planned planned;
        try {
            planned = RequestPlanner.plan(shape, model, PlanExecutor.sqlExcludedColumns(overrides), plannerFactory, inputs);
        } catch (UnmetPlanRequirementException unmet) {
            // The search endpoint answers this 400; explain describes it
            // instead: the cheapest plan the demand refused, with its
            // traits, and the refusal under traits.enforcer.
            RelNode refused = RequestPlanner.coordinatorPlan(unmet.offered(), shape, inputs.nodes());
            return LanceExplainResponse.planFailed(
                indexName,
                RelOptUtil.toString(unmet.logical()),
                PlanText.render(refused),
                unmet.getMessage(),
                LanceExplainResponse.Traits.of(unmet.enforcement(), refused.getTraitSet())
            );
        }
        String logicalText = RelOptUtil.toString(planned.logical());
        RelNode coordinatorPlan = planned.coordinatorPlan(shape, inputs.nodes());
        boolean readerWrapper = ReaderWrapperProbe.installed(indicesService, metadata.getIndex());
        return LanceExplainResponse.fragment(
            indexName,
            logicalText,
            PlanText.render(coordinatorPlan),
            planned.plan(),
            planned.unplanned(),
            ExplainRefinements.predict(planned.plan(), readerWrapper, overrides.ipColumns()),
            LanceExplainResponse.Traits.of(planned.enforcement(), coordinatorPlan.getTraitSet())
        );
    }

    /** The data nodes a search would fan out to, at least one so a cluster without them still explains. */
    private int dataNodes() {
        return Math.max(1, clusterService.state().nodes().getDataNodes().size());
    }
}
