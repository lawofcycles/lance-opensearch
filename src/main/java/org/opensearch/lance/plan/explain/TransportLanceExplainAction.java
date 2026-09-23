/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
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
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.cost.CostInputs;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Serves {@link LanceExplainAction} on the coordinating node: resolves
 * the index from cluster state (404 unknown, 400 not Lance backed),
 * builds the planner's model of the index through
 * {@link LanceSchemas#build}, translates the search body with
 * {@link SearchRequestToRel}, runs the Hep planner over its still empty
 * program and the Volcano planner with the pushdown rules, and returns
 * the logical and physical plan texts. No Lance scan is issued and no
 * request executes through the planner.
 *
 * <p>A body outside the supported shape surfaces as
 * {@link UnsupportedOperationException} from the translator and is
 * rewrapped as {@link IllegalArgumentException}, so the caller sees a
 * 400 {@code illegal_argument_exception} carrying the translator's
 * message instead of a 500.
 *
 * <p>Threading: the cluster state lookup runs wherever the request
 * arrives; the model build and the translation are handed to the
 * plugin's {@code lance_coordinator} pool because building the model on
 * a cold node opens the Lance table (metadata I/O) and the translation
 * is CPU work that does not belong on a transport thread. The transport
 * handler registers on {@code SAME} so a remote request is not bounced
 * through the pool once for the handler and again for the explicit
 * fork; the fork inside {@link #doExecute} is the single hop for local
 * and remote callers alike.
 *
 * <p>The cost budgets handed to {@link LancePlannerFactory} (the node's
 * {@code lance.native_memory.limit} and the JVM's max heap) feed the
 * cost ordering the Volcano run compares candidates with; nothing
 * predicts real byte usage yet, so they act as placeholders. The
 * latency side is costed under the {@link CostInputs} the coordinator
 * would plan with: the cluster's data node count, the table URI's
 * storage kind, this node's CPUs and the two parallelism settings, so
 * the explain output shows the choice a search on this cluster makes
 * rather than the single node default.
 */
public final class TransportLanceExplainAction extends HandledTransportAction<LanceExplainRequest, LanceExplainResponse> {

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final LanceWarmCache warmCache;
    private final LancePlannerFactory plannerFactory;

    @Inject
    public TransportLanceExplainAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        ClusterService clusterService,
        LanceWarmCache warmCache,
        Settings settings
    ) {
        super(LanceExplainAction.NAME, transportService, actionFilters, LanceExplainRequest::new, ThreadPool.Names.SAME);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
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
        LanceSchemas.IndexModel model = LanceSchemas.build(metadata, warmCache);
        RelNode logical;
        try {
            logical = SearchRequestToRel.translate(source, model, plannerFactory);
        } catch (UnsupportedOperationException unsupported) {
            throw new IllegalArgumentException(unsupported.getMessage(), unsupported);
        }
        HepPlanner hepPlanner = plannerFactory.newHepPlanner();
        hepPlanner.setRoot(logical);
        RelNode planned = hepPlanner.findBestExp();
        // The logical text is rendered before the Volcano run: the
        // planner registers the tree and the physical string comes from
        // its own best expression.
        String logicalText = RelOptUtil.toString(planned);
        RelNode physical = plannerFactory.plan(planned, costInputs(metadata));
        return new LanceExplainResponse(metadata.getIndex().getName(), logicalText, RelOptUtil.toString(physical));
    }

    /**
     * The inputs a search over {@code metadata}'s index would be
     * planned with on this cluster: every data node is a fan out
     * target, the storage kind follows the table URI, and the
     * parallelism settings are read at their current values.
     */
    private CostInputs costInputs(IndexMetadata metadata) {
        int dataNodes = Math.max(1, clusterService.state().nodes().getDataNodes().size());
        return CostInputs.forCluster(
            dataNodes,
            metadata.getSettings().get(LanceEngineFactory.TABLE_SETTING),
            NativeMemoryLimit.availableCpus(),
            clusterService.getClusterSettings().get(LancePlugin.AGGREGATION_PUSHDOWN_PARALLELISM_SETTING),
            clusterService.getClusterSettings().get(LancePlugin.FRAGMENT_PATH_SLICES_SETTING)
        );
    }
}
