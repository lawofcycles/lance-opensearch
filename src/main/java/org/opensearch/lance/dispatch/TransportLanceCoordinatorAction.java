/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.search.TotalHits;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.LanceMappingMeta;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.execute.FragmentFanOut;
import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.lance.plan.execute.MergeReducer;
import org.opensearch.lance.plan.execute.PlanExecutor;
import org.opensearch.lance.plan.execute.RequestPlanner;
import org.opensearch.lance.plan.metadata.TableStatistics;
import org.opensearch.lance.plan.metadata.TableStatisticsCache;
import org.opensearch.lance.plan.translate.SearchRequestToRel.ExecutionShape;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Coordinator handler for shard-free dispatch. Receives a
 * {@link SearchRequest} the {@link LanceDispatchActionFilter}
 * already decided is fragment-dispatchable, resolves the target
 * indexes and query metadata, enumerates fragments through the
 * shared {@link LanceRegistry}, groups them round-robin across every
 * data node in cluster state (sorted by node id), and fans
 * requests out via {@link LanceFragmentQueryAction}. A node whose
 * fragments hold more rows than one Lucene reader may
 * ({@code IndexWriter.MAX_DOCS}) receives one request per group of
 * fragments that fits, so a table of any size is searchable; the merge
 * treats a group's response like a node's. The executor
 * builds its query context from cluster state alone, so a node needs
 * no shard copy of the index to take a share; the plugin has to be
 * installed on every data node, which OpenSearch expects of plugins
 * anyway. Once every
 * per-node response arrives, it merges the per-node hit lists by the
 * request's sort (or by score) and the per-node
 * {@link InternalAggregations} through the stock reduce into a
 * single {@link SearchResponse}.
 *
 * <p>Single-node clusters take this same path with a data-node list
 * of length one, so the transport hop reduces to a local
 * {@code sendRequest} against the loopback pool. Multi-node
 * behaviour is covered by {@code LanceMultiNodeIT}.
 *
 * <p>Threading: the entry (resolve, enumerate, send) and the merge
 * run on the plugin's {@code lance_coordinator} pool; the per-node
 * responses are received on the transport thread that read them and
 * only stored and counted there (see {@link FragmentFanOut}).
 *
 * <p>Timeout and cancellation: the request's {@code timeout} (or the
 * cluster's {@code search.default_search_timeout}) is the transport
 * timeout of every per-node request. A node that has not answered by
 * then is reported as incomplete, its executor task is cancelled, and
 * the request either completes from the nodes that did answer with
 * {@code timed_out: true} ({@code allow_partial_search_results},
 * default true) or fails with HTTP 504. The per-node requests are child
 * requests of the coordinator task, so cancelling that task (through
 * {@code _tasks/_cancel}, or the client closing its connection, which
 * cancels the search task the coordinator task is a child of) cancels
 * the executors through the task manager and ends the request with
 * {@code TaskCancelledException} instead of a merge.
 */
public final class TransportLanceCoordinatorAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportLanceCoordinatorAction.class);

    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final BigArrays bigArrays;
    private final ScriptService scriptService;
    private final NodeClient client;
    /**
     * Supplies the coordinator rewrite context: the same shard-free
     * {@code QueryRewriteContext} {@code TransportSearchAction} rewrites
     * a search body with before the shard fan-out.
     */
    private final IndicesService indicesService;
    /**
     * Plans every target once: the translator, the Volcano run and the
     * per node plan the fragment requests carry; the budgets mirror the
     * explain action's.
     */
    private final LancePlannerFactory plannerFactory;
    /**
     * This node's planner table statistics, keyed on (table URI,
     * manifest version); the fan-out fills the entry of the version it
     * enumerates fragments from, collecting from the dataset it has
     * open, and every later request on the same version reads it.
     */
    private final TableStatisticsCache tableStatistics;
    /**
     * Runs the coordinator plan: the {@code MergeExec (FanOutExec
     * (per node plan))} tree built per target executes as the per-node
     * fan-out and the reduce of the gathered responses.
     */
    private final PlanExecutor planExecutor;

    /**
     * Requests that reach this action over the transport layer (a
     * coordinating node other than the one that received the HTTP
     * request) are handled on the plugin's {@code lance_coordinator}
     * pool, the same pool {@link LanceDispatchActionFilter} forks the
     * local case onto, so no coordinator work runs on a transport
     * thread or on the {@code search} pool of a data node.
     */
    @Inject
    public TransportLanceCoordinatorAction(
        TransportService transportService,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ActionFilters actionFilters,
        BigArrays bigArrays,
        ScriptService scriptService,
        NodeClient client,
        LanceWarmCache warmCache,
        IndicesService indicesService
    ) {
        super(LanceCoordinatorAction.NAME, transportService, actionFilters, SearchRequest::new, LancePlugin.LANCE_COORDINATOR_THREAD_POOL);
        this.transportService = transportService;
        this.threadPool = transportService.getThreadPool();
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
        this.client = client;
        this.indicesService = indicesService;
        long nativeBudgetBytes = NativeMemoryLimit.parse(
            LancePlugin.NATIVE_MEMORY_LIMIT_SETTING.get(clusterService.getSettings()),
            LancePlugin.NATIVE_MEMORY_LIMIT_SETTING.getKey()
        );
        this.plannerFactory = new LancePlannerFactory(nativeBudgetBytes, Runtime.getRuntime().maxMemory());
        this.tableStatistics = warmCache.tableStatistics();
        this.planExecutor = new PlanExecutor(plannerFactory);
    }

    /**
     * How the per-node requests of one coordinator request leave the
     * node: as child requests of {@code task} (so the task manager
     * cancels the executors when the coordinator task is cancelled)
     * with {@code timeout} as the transport timeout of each. A null
     * {@code task} (no task registered for the request) sends plain
     * requests; a null {@code timeout} means none.
     */
    private FragmentFanOut.Sender sender(CancellableTask task, TimeValue timeout) {
        TransportRequestOptions options = timeout == null
            ? TransportRequestOptions.EMPTY
            : TransportRequestOptions.builder().withTimeout(timeout).build();
        return (node, request, handler) -> {
            if (task != null) {
                transportService.sendChildRequest(node, LanceFragmentQueryAction.NAME, request, task, options, handler);
            } else {
                transportService.sendRequest(node, LanceFragmentQueryAction.NAME, request, options, handler);
            }
        };
    }

    /**
     * Cancel the executor task of a per-node request whose answer will
     * not come: the request timed out, so the transport layer has
     * dropped its handler and unregistered the node as a child of the
     * coordinator task, and a later cancellation of the coordinator task
     * would not reach it. The cancel names the executor's action and the
     * coordinator task as parent, so on that node it matches the one
     * task this request started there. It runs under a stashed thread
     * context so a caller without the tasks privilege can still stop
     * its own executor, and its outcome only goes to the log: the
     * request has already been answered or failed by then.
     */
    private void cancelExecutorTask(CancellableTask task, DiscoveryNode node, LanceFragmentQueryRequest request) {
        if (task == null || node == null) {
            return;
        }
        CancelTasksRequest cancel = new CancelTasksRequest().setNodes(node.getId())
            .setActions(LanceFragmentQueryAction.NAME)
            .setParentTaskId(new TaskId(clusterService.localNode().getId(), task.getId()))
            .setReason("lance fragment request timed out at the coordinator");
        try (ThreadContext.StoredContext ignored = threadPool.getThreadContext().stashContext()) {
            client.admin().cluster().cancelTasks(cancel, ActionListener.wrap(response -> {
                if (response.getTasks().isEmpty()) {
                    LOGGER.debug(
                        "lance.dispatch: no fragment query task left to cancel on node [{}] for index [{}]",
                        node.getId(),
                        request == null ? "?" : request.indexName()
                    );
                } else {
                    LOGGER.debug(
                        "lance.dispatch: cancelled {} fragment query task(s) on node [{}] for index [{}]",
                        response.getTasks().size(),
                        node.getId(),
                        request == null ? "?" : request.indexName()
                    );
                }
            }, e -> LOGGER.warn("lance.dispatch: could not cancel the fragment query task on node [{}]: {}", node.getId(), e.toString())));
        }
    }

    @Override
    protected void doExecute(Task task, SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        try {
            executeCoordinated(task instanceof CancellableTask cancellable ? cancellable : null, searchRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Resolve the request, enumerate fragments per Lance-backed
     * index, and issue one per-node {@link LanceFragmentQueryAction}
     * fan-out per index. Multi-index requests without aggregations
     * are handled by running one fan-out per index sequentially and
     * merging the partial results at the end; multi-index requests
     * that carry metrics are filtered out earlier by the filter
     * because per-index metric merge is not yet implemented.
     */
    private void executeCoordinated(CancellableTask task, SearchRequest searchRequest, ActionListener<SearchResponse> listener)
        throws Exception {
        long start = System.currentTimeMillis();
        SearchSourceBuilder source = searchRequest.source();
        FanOutPolicy policy = new FanOutPolicy(task, resolveTimeout(source), resolveAllowPartialSearchResults(searchRequest));

        QueryBuilder query = rewriteAtCoordinator(source == null ? null : source.query(), start);
        QueryBuilder postFilter = source == null ? null : source.postFilter();
        List<SortBuilder<?>> sorts = source == null || source.sorts() == null ? Collections.emptyList() : source.sorts();
        Object[] searchAfter = source == null ? null : source.searchAfter();
        AggregatorFactories.Builder aggregations = source == null ? null : source.aggregations();
        int size = resolveSize(source);
        int from = resolveFrom(source);
        // Each per-node executor needs from + size docs so the coordinator
        // has enough hits after skipping `from`. Deep pagination costs
        // linear memory per node just like the shard path — no additional
        // fragment-level penalty.
        int perNodeSize = from + size;
        int trackTotalHitsUpTo = resolveTrackTotalHitsUpTo(source);

        Index[] concrete = indexNameExpressionResolver.concreteIndices(clusterService.state(), searchRequest);
        List<IndexTarget> targets = resolveTargets(concrete);
        if (targets.isEmpty()) {
            // Coordinator was invoked with no Lance-backed target.
            // Filter should not have delegated in that case; return
            // an empty response instead of failing so the client
            // still gets a well-formed shape.
            listener.onResponse(emptyResponse(0));
            return;
        }

        Collection<DiscoveryNode> dataNodes = clusterService.state().nodes().getDataNodes().values();
        if (dataNodes.isEmpty()) {
            listener.onFailure(new IllegalStateException("no data nodes available for lance fragment dispatch"));
            return;
        }
        List<DiscoveryNode> nodeList = new ArrayList<>(dataNodes);
        nodeList.sort(Comparator.comparing(DiscoveryNode::getId));

        // Per-index fan-out results, collected sequentially. The plan
        // is derived per target inside runIndexLoop, against the
        // target's own schema; the spec built here carries no plan yet.
        boolean planAggregations = aggregations != null
            && clusterService.getClusterSettings().get(LancePlugin.AGGREGATION_PUSHDOWN_SETTING)
            && LanceAggregationSupport.isPushdownCandidate(aggregations);
        FragmentQuerySpec spec = new FragmentQuerySpec(
            null,
            query,
            postFilter,
            sorts,
            searchAfter,
            from,
            perNodeSize,
            aggregations,
            planAggregations,
            source != null && source.trackScores(),
            trackTotalHitsUpTo
        );
        boolean versionRequested = source != null && Boolean.TRUE.equals(source.version());
        boolean seqNoAndPrimaryTermRequested = source != null && Boolean.TRUE.equals(source.seqNoAndPrimaryTerm());
        MergeReducer merged = new MergeReducer(
            clusterService,
            bigArrays,
            scriptService,
            aggregations,
            sorts,
            from,
            size,
            versionRequested,
            seqNoAndPrimaryTermRequested,
            trackTotalHitsUpTo
        );
        runIndexLoop(targets, 0, nodeList, spec, policy, merged, start, listener);
    }

    /**
     * The coordinator rewrite of the top level query
     * ({@link RequestPlanner#rewriteAtCoordinator}), so a query that
     * folds itself away without a mapping does so before it is planned
     * and the plan and the query the executors receive agree.
     */
    QueryBuilder rewriteAtCoordinator(QueryBuilder query, long nowInMillis) throws IOException {
        return RequestPlanner.rewriteAtCoordinator(indicesService, query, nowInMillis);
    }

    /**
     * The timeout of the per-node requests: the request's
     * {@code timeout}, else the cluster's
     * {@code search.default_search_timeout}
     * ({@link SearchService#DEFAULT_SEARCH_TIMEOUT_SETTING}), else none
     * ({@code null}). The same resolution order the shard path applies
     * to its query phase; the plugin adds no default of its own.
     */
    private TimeValue resolveTimeout(SearchSourceBuilder source) {
        TimeValue timeout = source == null ? null : source.timeout();
        if (timeout == null) {
            timeout = clusterService.getClusterSettings().get(SearchService.DEFAULT_SEARCH_TIMEOUT_SETTING);
        }
        if (timeout == null || timeout.equals(SearchService.NO_TIMEOUT) || timeout.millis() <= 0) {
            return null;
        }
        return timeout;
    }

    /**
     * Whether a request answers from the nodes that did answer when
     * one timed out: the request's {@code allow_partial_search_results},
     * else the cluster's {@code search.default_allow_partial_results}
     * (default true). The filter intercepts the search before
     * {@code TransportSearchAction} fills the request's default in, so
     * the cluster setting is read here.
     */
    private boolean resolveAllowPartialSearchResults(SearchRequest searchRequest) {
        Boolean requested = searchRequest.allowPartialSearchResults();
        if (requested != null) {
            return requested;
        }
        return clusterService.getClusterSettings().get(SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS);
    }

    /**
     * The {@code track_total_hits} bound in the encoding
     * {@link SearchSourceBuilder#trackTotalHitsUpTo()} uses. A request
     * that leaves the flag out counts up to
     * {@link SearchContext#DEFAULT_TRACK_TOTAL_HITS_UP_TO} (10,000),
     * the same default the shard path applies; {@code true} maps to
     * {@link SearchContext#TRACK_TOTAL_HITS_ACCURATE} and {@code false}
     * to {@link SearchContext#TRACK_TOTAL_HITS_DISABLED}.
     */
    private static int resolveTrackTotalHitsUpTo(SearchSourceBuilder source) {
        if (source == null || source.trackTotalHitsUpTo() == null) {
            return SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;
        }
        return source.trackTotalHitsUpTo();
    }

    /**
     * Recursive per-index fan-out. Uses continuation-passing so the
     * outer listener sees the final merged response only after every
     * index's fan-out completes.
     */
    private void runIndexLoop(
        List<IndexTarget> targets,
        int index,
        List<DiscoveryNode> nodeList,
        FragmentQuerySpec spec,
        FanOutPolicy policy,
        MergeReducer merged,
        long startMillis,
        ActionListener<SearchResponse> listener
    ) {
        if (index >= targets.size()) {
            listener.onResponse(merged.buildResponse(startMillis));
            return;
        }
        IndexTarget target = targets.get(index);
        try {
            fanOutForTarget(
                target,
                nodeList,
                spec,
                policy,
                merged,
                ActionListener.wrap(
                    v -> runIndexLoop(targets, index + 1, nodeList, spec, policy, merged, startMillis, listener),
                    listener::onFailure
                )
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Enumerate fragments for one Lance-backed index, plan the request
     * against the target once, group the fragments across the data-node
     * list, and issue one {@link LanceFragmentQueryAction} per node, or
     * several per node when the node's fragments hold more rows than one
     * Lucene reader may (see {@link PlanExecutor#splitByRows}). The plan
     * is derived here, once per target while its dataset is open,
     * because the planner's model needs the Arrow schema and the SQL
     * literal encoding depends on the target's own columns: two indices
     * in the same request may map the same field name to different
     * types. Completion signals through {@code done} once every response
     * is merged into {@code merged}.
     */
    private void fanOutForTarget(
        IndexTarget target,
        List<DiscoveryNode> nodeList,
        FragmentQuerySpec baseSpec,
        FanOutPolicy policy,
        MergeReducer merged,
        ActionListener<Void> done
    ) throws Exception {
        List<Integer> allFragmentIds;
        // Physical rows of every fragment, in the same order, for the cut
        // into groups a Lucene reader can hold.
        List<Long> allFragmentRows;
        // The manifest version this fan-out enumerates fragments from.
        // Every per-node request carries it, for a pinned or tag
        // following index as well as for one that follows the table,
        // so all executors of this request read the same version and
        // key their LanceWarmCache snapshot on it without asking Lance
        // for the latest version themselves.
        long observedVersion;
        // The Arrow schema and the table statistics the planner model
        // reads for the plan below, captured while the dataset is open.
        // The statistics come from this node's cache under the observed
        // version and are collected from the open dataset only on the
        // first request of that version.
        Schema arrowSchema;
        TableStatistics statistics = null;
        long tableRows = 0L;
        try (Dataset dataset = LanceRegistry.openDataset(target.tableUri(), target.storageOptions(), target.pinnedVersionOrEmpty())) {
            observedVersion = dataset.version();
            arrowSchema = dataset.getSchema();
            allFragmentIds = new ArrayList<>(dataset.getFragments().size());
            allFragmentRows = new ArrayList<>(dataset.getFragments().size());
            dataset.getFragments().forEach(fragment -> {
                allFragmentIds.add(fragment.getId());
                allFragmentRows.add(fragment.metadata().getPhysicalRows());
            });
            for (Long rows : allFragmentRows) {
                tableRows += rows;
            }
            try {
                statistics = tableStatistics.forDataset(dataset);
            } catch (RuntimeException e) {
                // Statistics are an input to plan quality, not to
                // correctness: the model falls back to the physical
                // row count rather than failing the search.
                LOGGER.warn("lance.dispatch: table statistics of [{}] unavailable, planning without them", target.indexName(), e);
            }
        }
        final long totalRows = tableRows;
        LanceSchemas.IndexModel model = statistics != null
            ? LanceSchemas.model(
                target.indexName(),
                arrowSchema,
                target.multiFields(),
                target.renamedFields(),
                target.primaryKeyField(),
                target.dateOverrideColumns(),
                statistics
            )
            : LanceSchemas.model(
                target.indexName(),
                arrowSchema,
                target.multiFields(),
                target.renamedFields(),
                target.primaryKeyField(),
                target.dateOverrideColumns(),
                () -> totalRows
            );
        RequestPlanner.Planned planned = RequestPlanner.plan(
            baseSpec.executionShape(),
            model,
            target.sqlExcludedColumns(),
            plannerFactory,
            RequestPlanner.clusterInputs(nodeList.size(), target.tableUri(), clusterService.getClusterSettings())
        );
        FragmentQuerySpec spec = baseSpec.withPlan(planned.plan());
        if (allFragmentIds.isEmpty()) {
            if (spec.aggregations() == null) {
                // Empty table with no aggregations requested: no
                // partials to merge, and hits.total.value = 0 is
                // already the coordinator's default when no
                // response is absorbed. Skip the fan-out entirely.
                done.onResponse(null);
                return;
            }
            // Empty table + aggregations requested: the coordinator
            // still needs an aggregations block in the response
            // (matching shard path behaviour for an empty index).
            // Send a single fan-out to the first data node with an
            // empty fragment set. The per-node executor opens a
            // LanceDirectoryReader with zero leaves, runs the
            // aggregators over zero docs, and returns an empty
            // InternalAggregations tree that the coordinator merges
            // via topLevelReduce. Same wire format as any other
            // fan-out; the only novel case is the reader being
            // shaped to maxDoc=0.
            dispatchEmptyAggregationRun(planned, target, observedVersion, nodeList.get(0), spec, policy, merged, done);
            return;
        }

        Map<DiscoveryNode, List<Integer>> perNode = PlanExecutor.groupFragmentsByNode(allFragmentIds, nodeList);
        // A node's fragments go out in one request unless their rows
        // would not fit one Lucene reader on the executor; then the node
        // gets one request per group of fragments that fits. Every table
        // under the bound (the usual case) keeps one request per node.
        long maxDocs = clusterService.getClusterSettings().get(LancePlugin.MAX_DOCS_PER_READER_SETTING);
        List<PlanExecutor.FragmentGroup> groups = PlanExecutor.splitByRows(perNode, allFragmentIds, allFragmentRows, maxDocs);

        // Responses land in the slot of the request they answer, so the
        // merge sees them in fan-out (node id, then group) order rather
        // than arrival order. The merge itself orders equal hits by row
        // address and does not depend on this order; keeping it fixed
        // keeps the per-node lists, and with them the logs and the
        // aggregation partials, in the same order on every request.
        // A per-node failure fails the whole request: the fan-out
        // forwards the first failure to `done` once every node has
        // answered and drops the remaining responses. A missing node
        // means missing fragments, and a silently short result set is
        // worse than an error. The one exception is a node that ran out
        // of time (or whose executor was cancelled): under
        // allow_partial_search_results the request answers from the
        // other nodes and says so with timed_out: true, the contract
        // of the shard path's timeout.
        //
        // Dispatch goes through TransportService so remote data nodes
        // receive the requests. For the local node this still executes
        // in-process because TransportService's request handler
        // dispatch is loopback aware, but any other node in the
        // cluster picks up its slice through the network.
        planExecutor.execute(
            planned.coordinatorPlan(spec.executionShape(), groups.size()),
            fanOutContext(groups, target, observedVersion, spec, policy),
            merged,
            target.indexName(),
            done
        );
    }

    /**
     * Everything the fan-out execution needs beyond the plan: the
     * groups, the transport payload of each (built and logged as the
     * request leaves), the sender under the request's task and
     * timeout, the pools, and the listener a node that will not
     * answer is reported through.
     */
    private PlanExecutor.FanOutContext fanOutContext(
        List<PlanExecutor.FragmentGroup> groups,
        IndexTarget target,
        long observedVersion,
        FragmentQuerySpec spec,
        FanOutPolicy policy
    ) {
        return new PlanExecutor.FanOutContext(groups, group -> {
            List<Integer> fragmentsForNode = group.fragmentIds();
            if (group.groupCount() == 1) {
                LOGGER.info(
                    "lance.dispatch: fan-out index [{}] table [{}] to node [{}] with fragments {}",
                    target.indexName(),
                    target.tableUri(),
                    group.node().getId(),
                    fragmentsForNode
                );
            } else {
                LOGGER.info(
                    "lance.dispatch: fan-out index [{}] table [{}] to node [{}] with fragments {} (group {} of {}, {} rows)",
                    target.indexName(),
                    target.tableUri(),
                    group.node().getId(),
                    fragmentsForNode,
                    group.groupIndex() + 1,
                    group.groupCount(),
                    group.rows()
                );
            }
            return fragmentRequest(target, observedVersion, spec, fragmentsForNode);
        },
            sender(policy.task(), policy.timeout()),
            threadPool.executor(LancePlugin.LANCE_COORDINATOR_THREAD_POOL),
            // The generic pool, not lance_coordinator: its queue is
            // unbounded, so the log line and the cancel of a node that
            // timed out never take the coordinator pool's queue slot the
            // merge of the same request needs (three nodes timing out
            // would otherwise refuse the merge on a small pool and turn
            // a timeout into a 429).
            threadPool.generic(),
            incompleteNodeListener(target, policy),
            policy.allowPartialSearchResults(),
            policy.task()
        );
    }

    /** The transport payload of one per-node request. */
    private LanceFragmentQueryRequest fragmentRequest(
        IndexTarget target,
        long observedVersion,
        FragmentQuerySpec spec,
        List<Integer> fragmentsForNode
    ) {
        return new LanceFragmentQueryRequest(
            target.tableUri(),
            target.indexName(),
            target.storageOptions(),
            observedVersion,
            spec.plan(),
            spec.query(),
            spec.postFilter(),
            spec.sorts(),
            spec.searchAfter(),
            spec.effectiveSize(),
            spec.aggregations(),
            fragmentsForNode,
            spec.trackScores(),
            spec.trackTotalHitsUpTo()
        );
    }

    /**
     * How a node that did not answer is reported: a timeout is logged
     * and its executor task cancelled; a cancelled executor only needs
     * the debug line, nothing is left to stop there.
     */
    private FragmentFanOut.IncompleteNodeListener incompleteNodeListener(IndexTarget target, FanOutPolicy policy) {
        return (node, request, cause) -> {
            String nodeId = node == null ? "?" : node.getId();
            String fragments = request == null
                ? "?"
                : (request.fragmentIds().isEmpty() ? "all" : String.valueOf(request.fragmentIds().size()));
            if (cause instanceof ReceiveTimeoutTransportException) {
                LOGGER.warn(
                    "lance.dispatch: node [{}] did not answer the fragment request for index [{}] ({} fragments) within [{}]; "
                        + "cancelling its executor task",
                    nodeId,
                    target.indexName(),
                    fragments,
                    policy.timeout()
                );
                cancelExecutorTask(policy.task(), node, request);
            } else {
                // The executor's task was cancelled on that node,
                // through _tasks/_cancel or because this request's
                // task was cancelled; nothing left to stop there.
                LOGGER.debug(
                    "lance.dispatch: the fragment query task on node [{}] for index [{}] ({} fragments) was cancelled: {}",
                    nodeId,
                    target.indexName(),
                    fragments,
                    cause.getMessage()
                );
            }
        };
    }

    /**
     * Send a single fan-out to one shard host with an empty
     * fragment set so the per-node aggregator machinery still runs
     * over zero docs and returns an empty
     * {@link InternalAggregations} tree. Reserved for the
     * empty-table case where the request asked for aggregations;
     * without this the response would drop the aggregations block
     * entirely rather than returning the standard "aggregators
     * ran, no data" shape.
     */
    private void dispatchEmptyAggregationRun(
        RequestPlanner.Planned planned,
        IndexTarget target,
        long observedVersion,
        DiscoveryNode host,
        FragmentQuerySpec spec,
        FanOutPolicy policy,
        MergeReducer merged,
        ActionListener<Void> done
    ) {
        LOGGER.info(
            "lance.dispatch: fan-out index [{}] table [{}] empty aggregation run on node [{}]",
            target.indexName(),
            target.tableUri(),
            host.getId()
        );
        List<PlanExecutor.FragmentGroup> groups = List.of(new PlanExecutor.FragmentGroup(host, Collections.emptyList(), 0L, 0, 1));
        planExecutor.execute(
            planned.coordinatorPlan(spec.executionShape(), 1),
            new PlanExecutor.FanOutContext(
                groups,
                group -> fragmentRequest(target, observedVersion, spec, group.fragmentIds()),
                sender(policy.task(), policy.timeout()),
                threadPool.executor(LancePlugin.LANCE_COORDINATOR_THREAD_POOL),
                threadPool.generic(),
                incompleteNodeListener(target, policy),
                policy.allowPartialSearchResults(),
                policy.task()
            ),
            merged,
            target.indexName(),
            done
        );
    }

    private static int resolveSize(SearchSourceBuilder source) {
        if (source == null || source.size() < 0) {
            return 10;
        }
        return source.size();
    }

    private static int resolveFrom(SearchSourceBuilder source) {
        if (source == null || source.from() < 0) {
            return 0;
        }
        return source.from();
    }

    private List<IndexTarget> resolveTargets(Index[] concrete) {
        Metadata metadata = clusterService.state().metadata();
        List<IndexTarget> targets = new ArrayList<>(concrete.length);
        for (Index index : concrete) {
            IndexMetadata indexMetadata = metadata.index(index);
            if (indexMetadata == null) {
                continue;
            }
            String tableUri = indexMetadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
            if (tableUri == null || tableUri.isEmpty()) {
                continue;
            }
            StorageOptions storageOptions = StorageOptions.fromIndexSettings(indexMetadata.getSettings());
            long pinnedVersion = resolvePinnedVersion(indexMetadata, tableUri, storageOptions);
            LanceOverrides overrides = LanceOverrides.of(indexMetadata.getSettings());
            Map<String, String> renamedFields = new LinkedHashMap<>();
            for (LanceMappingMeta.RenamedField renamed : LanceMappingMeta.renamedFields(indexMetadata.mapping())) {
                renamedFields.put(renamed.from(), renamed.to());
            }
            String primaryKeyField = indexMetadata.getSettings().get("index.lance.primary_key_field", "");
            targets.add(
                new IndexTarget(
                    index.getName(),
                    tableUri,
                    storageOptions,
                    pinnedVersion,
                    overrides.subFields(),
                    renamedFields,
                    primaryKeyField,
                    PlanExecutor.sqlExcludedColumns(overrides),
                    overrides.dateColumns().keySet()
                )
            );
        }
        return targets;
    }

    /**
     * The manifest version a Lance-backed index reads right now, or
     * {@code -1} when it follows the latest. Same reading of
     * {@code index.lance.version} as the shard engine
     * ({@link LanceEngineFactory#newReadWriteEngine}): -1 follows the
     * latest manifest, anything else pins. A tag-following index pins to
     * whatever version the tag points at right now; resolving it costs
     * one extra Dataset.open of the latest manifest (the tag lives in
     * the table's refs, not in any manifest), which the shared Lance
     * Session keeps cheap.
     */
    private static long resolvePinnedVersion(IndexMetadata indexMetadata, String tableUri, StorageOptions storageOptions) {
        long pinnedVersion = indexMetadata.getSettings().getAsLong(LanceEngineFactory.VERSION_SETTING, -1L);
        String tag = indexMetadata.getSettings().get(LanceEngineFactory.TAG_SETTING, "");
        if (pinnedVersion < 0 && !tag.isEmpty()) {
            pinnedVersion = LanceRegistry.resolveTagVersion(tableUri, storageOptions, tag);
        }
        return pinnedVersion;
    }

    /**
     * How much of the table behind a Lance-backed index one Lucene
     * reader holds under {@code maxDocs}: the table's physical rows at
     * the version the index reads, and the rows of the leading fragments
     * that fit ({@link LanceDirectoryReader#leadingFragmentsWithinBound}),
     * which is what the shard engine's reader serves. Opens the table
     * (metadata only) and must not run on a transport thread.
     */
    static ReaderBound readerBound(IndexMetadata indexMetadata, long maxDocs) {
        String tableUri = indexMetadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
        StorageOptions storageOptions = StorageOptions.fromIndexSettings(indexMetadata.getSettings());
        long pinnedVersion = resolvePinnedVersion(indexMetadata, tableUri, storageOptions);
        Optional<Long> version = pinnedVersion >= 0 ? Optional.of(pinnedVersion) : Optional.empty();
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, storageOptions, version)) {
            List<Fragment> fragments = dataset.getFragments();
            long[] rows = new long[fragments.size()];
            for (int i = 0; i < rows.length; i++) {
                rows[i] = fragments.get(i).metadata().getPhysicalRows();
            }
            int held = LanceDirectoryReader.leadingFragmentsWithinBound(rows, maxDocs);
            long tableRows = 0L;
            long readerRows = 0L;
            for (int i = 0; i < rows.length; i++) {
                tableRows += rows[i];
                if (i < held) {
                    readerRows += rows[i];
                }
            }
            return new ReaderBound(tableRows, readerRows);
        }
    }

    /** Physical rows of a table and the rows of it one shard reader holds. */
    record ReaderBound(long tableRows, long readerRows) {
        boolean exceeded() {
            return readerRows < tableRows;
        }
    }

    private SearchResponse emptyResponse(long took) {
        SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, false, null, 1);
        // Same rationale as MergeReducer.buildResponse: report a
        // single logical unit rather than 0 shards so clients that
        // check {@code _shards.total >= 1} keep parsing correctly.
        return new SearchResponse(sections, null, 1, 1, 0, took, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    /**
     * One Lance-backed index in the request. {@code pinnedVersion}
     * is the {@code index.lance.version} setting or the resolved tag
     * version ({@code -1} when the index follows the latest
     * manifest); it drives the coordinator's own fragment
     * enumeration, whose observed version then travels with every
     * per-node request so the executors read the same manifest.
     * {@code multiFields}, {@code renamedFields},
     * {@code primaryKeyField}, {@code sqlExcludedColumns} and
     * {@code dateOverrideColumns} feed the planner model the per-target
     * filter SQL derivation builds once the target's Arrow schema is
     * known.
     */
    private record IndexTarget(String indexName, String tableUri, StorageOptions storageOptions, long pinnedVersion, Map<
        String,
        LinkedHashMap<String, String>> multiFields, Map<String, String> renamedFields, String primaryKeyField, Set<
            String> sqlExcludedColumns, Set<String> dateOverrideColumns) {

        Optional<Long> pinnedVersionOrEmpty() {
            return pinnedVersion >= 0 ? Optional.of(pinnedVersion) : Optional.empty();
        }
    }

    /**
     * How the per-node requests of one coordinator request are sent and
     * how a node that does not answer is treated: the coordinator task
     * they are children of (null when the request runs under none), the
     * transport timeout of each (null for none), and whether a node that
     * did not answer leaves the request with partial results or fails it.
     */
    private record FanOutPolicy(CancellableTask task, TimeValue timeout, boolean allowPartialSearchResults) {
    }

    /**
     * Immutable bundle of the query-time settings the coordinator
     * resolves once and threads through the per-index fan-out, plus the
     * per node plan derived for the current target ({@code plan}, null
     * until {@link #fanOutForTarget} planned it). Keeps the recursive
     * {@link #runIndexLoop} / {@link #fanOutForTarget} signatures short
     * even as new wire-format fields are added.
     */
    private record FragmentQuerySpec(FragmentPlan plan, QueryBuilder query, QueryBuilder postFilter, List<SortBuilder<?>> sorts,
        Object[] searchAfter, int from, int effectiveSize, AggregatorFactories.Builder aggregations, boolean planAggregations,
        boolean trackScores, int trackTotalHitsUpTo) {

        /** The same spec carrying the plan derived for one target. */
        FragmentQuerySpec withPlan(FragmentPlan targetPlan) {
            return new FragmentQuerySpec(
                targetPlan,
                query,
                postFilter,
                sorts,
                searchAfter,
                from,
                effectiveSize,
                aggregations,
                planAggregations,
                trackScores,
                trackTotalHitsUpTo
            );
        }

        /** What the planner reads of the request. */
        ExecutionShape executionShape() {
            return new ExecutionShape(query, postFilter, sorts, searchAfter, from, effectiveSize, aggregations, planAggregations);
        }
    }
}
