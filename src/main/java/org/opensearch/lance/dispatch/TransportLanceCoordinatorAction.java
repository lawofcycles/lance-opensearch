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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.GroupedActionListener;
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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.query.LanceKnnFilterTranslator;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchService;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
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
        NodeClient client
    ) {
        super(LanceCoordinatorAction.NAME, transportService, actionFilters, SearchRequest::new, LancePlugin.LANCE_COORDINATOR_THREAD_POOL);
        this.transportService = transportService;
        this.threadPool = transportService.getThreadPool();
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
        this.client = client;
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

        org.opensearch.index.query.QueryBuilder query = source == null ? null : source.query();
        org.opensearch.index.query.QueryBuilder postFilter = source == null ? null : source.postFilter();
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

        // Per-index fan-out results, collected sequentially.
        // filterSql is recomputed per target inside runIndexLoop so
        // each target encodes literals against its own mapping; the
        // value set here only survives when the mapping does not
        // affect the emitted SQL.
        FragmentQuerySpec spec = new FragmentQuerySpec(
            null,
            query,
            postFilter,
            sorts,
            searchAfter,
            perNodeSize,
            aggregations,
            source != null && source.trackScores(),
            trackTotalHitsUpTo
        );
        boolean versionRequested = source != null && Boolean.TRUE.equals(source.version());
        boolean seqNoAndPrimaryTermRequested = source != null && Boolean.TRUE.equals(source.seqNoAndPrimaryTerm());
        MergeState merged = new MergeState(
            aggregations,
            sorts,
            from,
            size,
            versionRequested,
            seqNoAndPrimaryTermRequested,
            trackTotalHitsUpTo
        );
        runIndexLoop(targets, 0, nodeList, spec, policy, source, merged, start, listener);
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
        SearchSourceBuilder source,
        MergeState merged,
        long startMillis,
        ActionListener<SearchResponse> listener
    ) {
        if (index >= targets.size()) {
            listener.onResponse(merged.buildResponse(startMillis));
            return;
        }
        IndexTarget target = targets.get(index);
        // filterSql is re-derived per target because Lance SQL literal
        // encoding depends on the target's mapping: two indices in the
        // same request may map the same field name to different types
        // (say `ts` as `date` on one and `long` on the other).
        FragmentQuerySpec perTargetSpec = new FragmentQuerySpec(
            resolveFilterSql(source, target.fieldTypeLookup()),
            spec.query(),
            spec.postFilter(),
            spec.sorts(),
            spec.searchAfter(),
            spec.effectiveSize(),
            spec.aggregations(),
            spec.trackScores(),
            spec.trackTotalHitsUpTo()
        );
        try {
            fanOutForTarget(
                target,
                nodeList,
                perTargetSpec,
                policy,
                merged,
                ActionListener.wrap(
                    v -> runIndexLoop(targets, index + 1, nodeList, spec, policy, source, merged, startMillis, listener),
                    listener::onFailure
                )
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Enumerate fragments for one Lance-backed index, group them
     * across the data-node list, and issue one
     * {@link LanceFragmentQueryAction} per node, or several per node
     * when the node's fragments hold more rows than one Lucene reader
     * may (see {@link #splitByRows}). Completion signals
     * through {@code done} once every response is merged into
     * {@code merged}.
     */
    private void fanOutForTarget(
        IndexTarget target,
        List<DiscoveryNode> nodeList,
        FragmentQuerySpec spec,
        FanOutPolicy policy,
        MergeState merged,
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
        try (Dataset dataset = LanceRegistry.openDataset(target.tableUri(), target.storageOptions(), target.pinnedVersionOrEmpty())) {
            observedVersion = dataset.version();
            allFragmentIds = new ArrayList<>(dataset.getFragments().size());
            allFragmentRows = new ArrayList<>(dataset.getFragments().size());
            dataset.getFragments().forEach(fragment -> {
                allFragmentIds.add(fragment.getId());
                allFragmentRows.add(fragment.metadata().getPhysicalRows());
            });
        }
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
            dispatchEmptyAggregationRun(target, observedVersion, nodeList.get(0), spec, policy, merged, done);
            return;
        }

        Map<DiscoveryNode, List<Integer>> perNode = groupFragmentsByNode(allFragmentIds, nodeList);
        // A node's fragments go out in one request unless their rows
        // would not fit one Lucene reader on the executor; then the node
        // gets one request per group of fragments that fits. Every table
        // under the bound (the usual case) keeps one request per node.
        long maxDocs = clusterService.getClusterSettings().get(LancePlugin.MAX_DOCS_PER_READER_SETTING);
        List<FragmentGroup> groups = splitByRows(perNode, allFragmentIds, allFragmentRows, maxDocs);

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
        FragmentFanOut fanOut = newFanOut(groups.size(), target, policy, merged, done);
        FragmentFanOut.Sender sender = sender(policy.task(), policy.timeout());

        int slot = 0;
        for (FragmentGroup group : groups) {
            final int slotIndex = slot++;
            DiscoveryNode nodeTarget = group.node();
            List<Integer> fragmentsForNode = group.fragmentIds();
            LanceFragmentQueryRequest fragmentRequest = new LanceFragmentQueryRequest(
                target.tableUri(),
                target.indexName(),
                target.storageOptions(),
                observedVersion,
                spec.filterSql(),
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
            if (group.groupCount() == 1) {
                LOGGER.info(
                    "lance.dispatch: fan-out index [{}] table [{}] to node [{}] with fragments {}",
                    target.indexName(),
                    target.tableUri(),
                    nodeTarget.getId(),
                    fragmentsForNode
                );
            } else {
                LOGGER.info(
                    "lance.dispatch: fan-out index [{}] table [{}] to node [{}] with fragments {} (group {} of {}, {} rows)",
                    target.indexName(),
                    target.tableUri(),
                    nodeTarget.getId(),
                    fragmentsForNode,
                    group.groupIndex() + 1,
                    group.groupCount(),
                    group.rows()
                );
            }
            // Dispatch through TransportService so remote data nodes
            // receive the request. For the local node this still
            // executes in-process because TransportService's request
            // handler dispatch is loopback aware, but any other node
            // in the cluster picks up its slice through the network.
            fanOut.send(sender, slotIndex, nodeTarget, fragmentRequest);
        }
    }

    /**
     * The fragments of one per-node request: a node's fragments, or the
     * {@code groupIndex}th of {@code groupCount} contiguous groups of them
     * when they do not fit one Lucene reader together, with the physical
     * rows of the group.
     */
    record FragmentGroup(DiscoveryNode node, List<Integer> fragmentIds, long rows, int groupIndex, int groupCount) {
    }

    /**
     * Cut every node's fragment list of {@code perNode} into contiguous
     * groups whose physical rows fit in {@code maxDocs}, in the order of
     * the map and of each list ({@link LanceDirectoryReader#groupEnds}).
     * {@code fragmentIds} and {@code fragmentRows} are the table's
     * fragments and their physical rows in the same order; a fragment id
     * the rows are not known for counts as zero rows. A node whose
     * fragments fit yields one group, so a table under the bound fans
     * out exactly as before: one request per node.
     */
    static List<FragmentGroup> splitByRows(
        Map<DiscoveryNode, List<Integer>> perNode,
        List<Integer> fragmentIds,
        List<Long> fragmentRows,
        long maxDocs
    ) {
        Map<Integer, Long> rowsById = new HashMap<>(fragmentIds.size());
        for (int i = 0; i < fragmentIds.size(); i++) {
            rowsById.put(fragmentIds.get(i), fragmentRows.get(i));
        }
        List<FragmentGroup> groups = new ArrayList<>(perNode.size());
        for (Map.Entry<DiscoveryNode, List<Integer>> assignment : perNode.entrySet()) {
            List<Integer> nodeFragments = assignment.getValue();
            long[] rows = new long[nodeFragments.size()];
            for (int i = 0; i < rows.length; i++) {
                rows[i] = rowsById.getOrDefault(nodeFragments.get(i), 0L);
            }
            int[] ends = LanceDirectoryReader.groupEnds(rows, maxDocs);
            int start = 0;
            for (int g = 0; g < ends.length; g++) {
                long groupRows = 0L;
                for (int i = start; i < ends[g]; i++) {
                    groupRows += rows[i];
                }
                groups.add(
                    new FragmentGroup(assignment.getKey(), List.copyOf(nodeFragments.subList(start, ends[g])), groupRows, g, ends.length)
                );
                start = ends[g];
            }
        }
        return groups;
    }

    /**
     * The fan-out of one target under {@code policy}: the merge absorbs
     * the responses that arrived into {@code merged} and marks the
     * response timed out when a node did not answer; a node that did
     * not answer is logged and its executor task cancelled.
     */
    private FragmentFanOut newFanOut(int size, IndexTarget target, FanOutPolicy policy, MergeState merged, ActionListener<Void> done) {
        return new FragmentFanOut(
            size,
            threadPool.executor(LancePlugin.LANCE_COORDINATOR_THREAD_POOL),
            outcome -> merged.absorbTargetResponses(target, outcome.responses(), outcome.incompleteNodes() > 0),
            done,
            policy.allowPartialSearchResults(),
            policy.task(),
            // The generic pool, not lance_coordinator: its queue is
            // unbounded, so the log line and the cancel of a node that
            // timed out never take the coordinator pool's queue slot the
            // merge of the same request needs (three nodes timing out
            // would otherwise refuse the merge on a small pool and turn
            // a timeout into a 429).
            threadPool.generic(),
            (node, request, cause) -> {
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
            }
        );
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
        IndexTarget target,
        long observedVersion,
        DiscoveryNode host,
        FragmentQuerySpec spec,
        FanOutPolicy policy,
        MergeState merged,
        ActionListener<Void> done
    ) {
        LanceFragmentQueryRequest fragmentRequest = new LanceFragmentQueryRequest(
            target.tableUri(),
            target.indexName(),
            target.storageOptions(),
            observedVersion,
            spec.filterSql(),
            spec.query(),
            spec.postFilter(),
            spec.sorts(),
            spec.searchAfter(),
            spec.effectiveSize(),
            spec.aggregations(),
            Collections.emptyList(),
            spec.trackScores(),
            spec.trackTotalHitsUpTo()
        );
        LOGGER.info(
            "lance.dispatch: fan-out index [{}] table [{}] empty aggregation run on node [{}]",
            target.indexName(),
            target.tableUri(),
            host.getId()
        );
        FragmentFanOut fanOut = newFanOut(1, target, policy, merged, done);
        fanOut.send(sender(policy.task(), policy.timeout()), 0, host, fragmentRequest);
    }

    /**
     * The per-node requests of one fan-out and the delivery of their
     * responses to the merge.
     *
     * <p>Responses arrive on the transport thread that read them
     * ({@link TransportResponseHandler#executor()} is
     * {@link ThreadPool.Names#SAME}), so the transport layer never has
     * to queue a response on a thread pool and can never reject one.
     * A rejected response would close the channel it came on and fail
     * every other request in flight on that channel, and the request
     * whose response was dropped would never complete. On that thread
     * a response is only stored in its slot and counted; nothing of
     * Lance, Lucene or the reduce runs there.
     *
     * <p>When the last node has answered, the merge of the ordered
     * responses is submitted to the coordinator pool. Every way the
     * merge can fail to run or to finish reaches {@code done} exactly
     * once: the pool rejecting the merge, the merge throwing, a node
     * failing, and {@link #send} throwing before a request left the
     * node all end in {@link ActionListener#onFailure}; the last
     * response's merge completing ends in
     * {@link ActionListener#onResponse}. A pool rejection is passed
     * through as the pool's {@code OpenSearchRejectedExecutionException}
     * so the client sees HTTP 429.
     *
     * <p>A node whose answer will not come counts as answered with an
     * empty slot: its request timed out
     * ({@link ReceiveTimeoutTransportException}, the transport layer
     * has dropped the handler and a late answer is discarded there), or
     * its executor task was cancelled and it answered
     * {@link TaskCancelledException}. The {@link IncompleteNodeListener}
     * is told once per such node. With partial results allowed the
     * merge then runs over the responses that did arrive and the
     * {@link Outcome} carries the count of missing nodes; otherwise the
     * fan-out fails with {@link OpenSearchTimeoutException} (HTTP 504)
     * carrying the transport exception as its cause, once every node
     * has answered or timed out.
     *
     * <p>A cancelled coordinator task ends the fan-out with
     * {@link TaskCancelledException} in place of the merge (and in
     * place of any other failure), whatever the nodes answered. The
     * task's state is read at the moment {@code done} is completed, so
     * a cancellation that lands between a node's failure and the
     * completion still wins.
     */
    static final class FragmentFanOut {

        /** How a per-node request leaves the coordinator; {@code TransportService::sendChildRequest} outside tests. */
        interface Sender {
            void send(DiscoveryNode node, LanceFragmentQueryRequest request, TransportResponseHandler<LanceFragmentQueryResponse> handler);
        }

        /**
         * Told about a node whose answer will not come, with the
         * exception that said so. {@code node} and {@code request} are
         * those {@link #send} was called with for the slot, null when
         * the slot was never sent.
         */
        interface IncompleteNodeListener {
            void onIncomplete(DiscoveryNode node, LanceFragmentQueryRequest request, TransportException cause);
        }

        /**
         * What the merge receives: the responses that arrived, in slot
         * order, and how many nodes did not answer (0 when every node
         * did).
         */
        record Outcome(List<LanceFragmentQueryResponse> responses, int incompleteNodes) {
        }

        private final int size;
        private final Executor notifyExecutor;
        private final AtomicReferenceArray<LanceFragmentQueryResponse> slots;
        private final AtomicReferenceArray<DiscoveryNode> nodes;
        private final AtomicReferenceArray<LanceFragmentQueryRequest> requests;
        private final AtomicInteger incompleteNodes = new AtomicInteger();
        private final boolean allowPartialResults;
        private final CancellableTask task;
        private final IncompleteNodeListener incompleteListener;
        private final GroupedActionListener<LanceFragmentQueryResponse> gathered;

        /**
         * A fan-out that allows partial results, runs under no task and
         * tells nobody about a node that did not answer.
         */
        FragmentFanOut(int size, Executor mergeExecutor, Consumer<Outcome> merge, ActionListener<Void> done) {
            this(size, mergeExecutor, merge, done, true, null, Runnable::run, (node, request, cause) -> {});
        }

        /**
         * @param size                number of per-node requests
         * @param mergeExecutor       pool the merge runs on once every response is in
         * @param merge               consumes the responses in slot order
         * @param done                completed once, after the merge or on the first failure
         * @param allowPartialResults whether a node that did not answer leaves its slot empty (true) or fails the fan-out (false)
         * @param task                the coordinator task, or null; a cancelled task ends the fan-out with TaskCancelledException
         * @param notifyExecutor      pool the incomplete listener runs on, off the transport thread; a pool with an
         *                            unbounded queue, so a notification never takes a queue slot from the merge
         * @param incompleteListener  told once about every node that did not answer
         */
        FragmentFanOut(
            int size,
            Executor mergeExecutor,
            Consumer<Outcome> merge,
            ActionListener<Void> done,
            boolean allowPartialResults,
            CancellableTask task,
            Executor notifyExecutor,
            IncompleteNodeListener incompleteListener
        ) {
            this.size = size;
            this.notifyExecutor = notifyExecutor;
            this.slots = new AtomicReferenceArray<>(size);
            this.nodes = new AtomicReferenceArray<>(size);
            this.requests = new AtomicReferenceArray<>(size);
            this.allowPartialResults = allowPartialResults;
            this.task = task;
            this.incompleteListener = incompleteListener;
            // The task's state is read here, at the completion of done,
            // and not where the failure or the merge result was produced:
            // a cancellation that lands in between still ends the request
            // as cancelled.
            ActionListener<Void> once = ActionListener.notifyOnce(ActionListener.wrap(v -> {
                if (isCancelled()) {
                    done.onFailure(cancelled());
                } else {
                    done.onResponse(v);
                }
            }, e -> done.onFailure(cancelledOr(e))));
            this.gathered = new GroupedActionListener<>(ActionListener.wrap(responses -> {
                // ActionRunnable routes a throwing merge and a
                // rejected submit (AbstractRunnable.onRejection
                // defaults to onFailure) to once.onFailure, and a
                // completed merge to once.onResponse.
                mergeExecutor.execute(ActionRunnable.run(once, () -> {
                    ensureNotCancelled();
                    List<LanceFragmentQueryResponse> ordered = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        LanceFragmentQueryResponse response = slots.get(i);
                        if (response != null) {
                            ordered.add(response);
                        }
                    }
                    merge.accept(new Outcome(ordered, incompleteNodes.get()));
                }));
            }, once::onFailure), size);
        }

        /**
         * Send the request for {@code slot}. A synchronous failure of
         * the sender (the node is gone, the request does not serialise,
         * the coordinator task has been cancelled and refuses new
         * children) counts as that node's failure so the fan-out still
         * completes once the other nodes have answered.
         */
        void send(Sender sender, int slot, DiscoveryNode node, LanceFragmentQueryRequest request) {
            nodes.set(slot, node);
            requests.set(slot, request);
            try {
                sender.send(node, request, handler(slot));
            } catch (Exception e) {
                gathered.onFailure(e);
            }
        }

        /** The response handler for {@code slot}. */
        TransportResponseHandler<LanceFragmentQueryResponse> handler(int slot) {
            return new TransportResponseHandler<>() {
                @Override
                public LanceFragmentQueryResponse read(StreamInput in) throws IOException {
                    return new LanceFragmentQueryResponse(in);
                }

                @Override
                public void handleResponse(LanceFragmentQueryResponse response) {
                    slots.set(slot, response);
                    gathered.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    if (!isIncomplete(exp)) {
                        gathered.onFailure(exp);
                        return;
                    }
                    incompleteNodes.incrementAndGet();
                    notifyIncomplete(nodes.get(slot), requests.get(slot), exp);
                    if (allowPartialResults) {
                        gathered.onResponse(null);
                    } else {
                        gathered.onFailure(timedOut(nodes.get(slot), exp));
                    }
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            };
        }

        /**
         * Tell the listener about a node that will not answer, on the
         * notify pool: this runs from the transport thread that delivered
         * the exception (or the timeout handler's thread), and the
         * listener formats a log line and sends a cancel request, neither
         * of which belongs on a transport thread. The node is counted as
         * incomplete before this is called, so a pool that refuses the
         * runnable only costs the log line and the cancel of that
         * executor, which the timeout has already detached from the
         * request; the fan-out itself completes as before.
         */
        private void notifyIncomplete(DiscoveryNode node, LanceFragmentQueryRequest request, TransportException cause) {
            try {
                notifyExecutor.execute(() -> {
                    try {
                        incompleteListener.onIncomplete(node, request, cause);
                    } catch (Exception e) {
                        LOGGER.warn(
                            "lance.dispatch: the incomplete node listener failed for node [{}]",
                            node == null ? "?" : node.getId(),
                            e
                        );
                    }
                });
            } catch (Exception rejected) {
                LOGGER.warn(
                    "lance.dispatch: could not report node [{}] as incomplete on the coordinator pool: {}",
                    node == null ? "?" : node.getId(),
                    rejected.toString()
                );
            }
        }

        /**
         * Whether {@code exp} says the node's answer will not come: the
         * request timed out, or the executor's task was cancelled (the
         * exception then arrives wrapped in the transport layer's
         * {@code RemoteTransportException}).
         */
        static boolean isIncomplete(TransportException exp) {
            return exp instanceof ReceiveTimeoutTransportException || TransportLanceFragmentQueryAction.findCancelled(exp) != null;
        }

        private static OpenSearchTimeoutException timedOut(DiscoveryNode node, TransportException cause) {
            return new OpenSearchTimeoutException(
                "lance fragment request to node [" + (node == null ? "?" : node.getId()) + "] did not complete in time",
                cause
            );
        }

        private boolean isCancelled() {
            return task != null && task.isCancelled();
        }

        private TaskCancelledException cancelled() {
            return new TaskCancelledException("cancelled task with reason: " + task.getReasonCancelled());
        }

        private void ensureNotCancelled() {
            if (isCancelled()) {
                throw cancelled();
            }
        }

        /** {@code e}, or a {@link TaskCancelledException} carrying it when the coordinator task has been cancelled. */
        private Exception cancelledOr(Exception e) {
            if (isCancelled()) {
                TaskCancelledException cancelled = cancelled();
                cancelled.addSuppressed(e);
                return cancelled;
            }
            return e;
        }
    }

    /**
     * Round-robin fragment ids across the sorted data-node list.
     * Empty per-node bucket entries are omitted so downstream
     * dispatch code only sees nodes that actually own work. The map
     * iterates in {@code nodeList} order so the fan-out is
     * deterministic.
     */
    private static Map<DiscoveryNode, List<Integer>> groupFragmentsByNode(List<Integer> fragmentIds, List<DiscoveryNode> nodeList) {
        Map<DiscoveryNode, List<Integer>> result = new LinkedHashMap<>();
        for (int i = 0; i < fragmentIds.size(); i++) {
            DiscoveryNode node = nodeList.get(i % nodeList.size());
            result.computeIfAbsent(node, k -> new ArrayList<>()).add(fragmentIds.get(i));
        }
        return result;
    }

    /**
     * Try to translate the top-level query to a Lance SQL filter for
     * metadata-only row counting. Returns {@code null} when the
     * query is match_all, absent, or cannot be expressed in Lance
     * SQL (e.g. match, knn, or anything requiring a Lucene scoring
     * pass). In those cases the per-node executor falls back to
     * {@link org.apache.lucene.search.IndexSearcher#count}.
     */
    private static String resolveFilterSql(SearchSourceBuilder source, java.util.function.Function<String, String> fieldTypeLookup) {
        if (source == null) {
            return null;
        }
        Object query = source.query();
        if (query == null || query instanceof org.opensearch.index.query.MatchAllQueryBuilder) {
            return null;
        }
        org.opensearch.index.query.QueryBuilder qb = (org.opensearch.index.query.QueryBuilder) query;
        // Refuse to emit SQL when any leaf in the tree names a
        // field this target does not map. The translator itself
        // would happily produce "unmapped >= 1" here, but Lance's
        // Dataset.countRows(sql) evaluates that and rejects the
        // scan with SchemaError. Falling back to filterSql=null
        // lets the per-node executor use the rewritten Lucene
        // query (RangeQueryBuilder.doRewrite folds an unmapped
        // range to MatchNone) and count through the same
        // IndexSearcher.count path shard search uses.
        if (LanceKnnFilterTranslator.hasUnmappedField(qb, fieldTypeLookup)) {
            return null;
        }
        try {
            return LanceKnnFilterTranslator.toLanceSql(qb, fieldTypeLookup);
        } catch (IllegalArgumentException ignored) {
            // Query shape outside the translator's whitelist (match,
            // knn, ...). No filter push-down; the per-node hits path
            // and computeMatched fall back to Lucene.
            return null;
        }
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
            targets.add(new IndexTarget(index.getName(), tableUri, storageOptions, pinnedVersion, buildFieldTypeLookup(indexMetadata)));
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

    /**
     * Build a field-type lookup for {@code indexMetadata} that reads
     * the mapping straight out of cluster state so
     * {@link LanceKnnFilterTranslator} can consult it while emitting
     * SQL literals. The lookup returns the mapping's {@code type}
     * string ({@code "date"}, {@code "long"}, {@code "keyword"}, ...)
     * for a top-level field name and {@code null} for unmapped or
     * sub-field names — the translator's
     * {@link LanceKnnFilterTranslator#rejectMultiFieldPath} guard
     * already refuses dotted paths, so multi-field paths never reach
     * the literal encoder in the first place.
     *
     * <p>Cluster-state metadata carries the mapping as the same JSON
     * the operator sent to {@code POST /_lance/attach}, deserialised
     * into a {@code Map<String, Object>} tree. The top-level
     * {@code properties} entry holds one entry per column, keyed by
     * the OpenSearch field name; the {@code type} field on each
     * entry is what we need. A dotted name walks nested
     * {@code properties} maps (a Struct column mapped as
     * {@code object}), so {@code meta.region} resolves to its child
     * type while a multi-field sub-field ({@code body.raw}, declared
     * under {@code fields}) stays unresolved.
     *
     * <p>Package-private so the per-node executor can build the same
     * lookup from its own copy of the index metadata when it decides
     * whether a bool query's scalar clauses can travel to Lance as an
     * FTS prefilter.
     */
    @SuppressWarnings("unchecked")
    static java.util.function.Function<String, String> buildFieldTypeLookup(IndexMetadata indexMetadata) {
        org.opensearch.cluster.metadata.MappingMetadata mapping = indexMetadata.mapping();
        if (mapping == null) {
            return LanceKnnFilterTranslator.NO_MAPPING;
        }
        java.util.Map<String, Object> source = mapping.getSourceAsMap();
        Object properties = source == null ? null : source.get("properties");
        if (!(properties instanceof java.util.Map)) {
            return LanceKnnFilterTranslator.NO_MAPPING;
        }
        final java.util.Map<String, Object> propertyMap = (java.util.Map<String, Object>) properties;
        return name -> {
            Object field = propertyMap.get(name);
            if (field == null && name != null && name.indexOf('.') >= 0) {
                field = resolveObjectPath(propertyMap, name);
            }
            if (!(field instanceof java.util.Map)) {
                return null;
            }
            Map<String, Object> fieldMap = (Map<String, Object>) field;
            Object type = fieldMap.get("type");
            if (!(type instanceof String typeName)) {
                return null;
            }
            return "date".equals(typeName) && isIntegerArrowType(fieldMap) ? LanceKnnFilterTranslator.DATE_ON_INTEGER : typeName;
        };
    }

    /**
     * Whether the mapping entry's {@code meta.lance_arrow_type} names an
     * integer Arrow column. True for a {@code date} field the attach
     * body overrode onto an epoch-millis integer column, whose SQL
     * literals must stay numeric (see
     * {@link LanceKnnFilterTranslator#DATE_ON_INTEGER}); a real Date /
     * Timestamp column keeps the plain {@code date} answer.
     */
    @SuppressWarnings("unchecked")
    static boolean isIntegerArrowType(Map<String, Object> fieldMap) {
        Object meta = fieldMap.get("meta");
        if (!(meta instanceof Map)) {
            return false;
        }
        Object arrowType = ((Map<String, Object>) meta).get("lance_arrow_type");
        return arrowType instanceof String s && s.startsWith("Int(");
    }

    /**
     * Resolve a dotted field name through nested {@code properties}
     * maps: each segment but the last must name an entry that itself
     * carries a {@code properties} object (a struct child mapped as an
     * {@code object}). A multi-field sub-field does not resolve here —
     * its sub-entries live under {@code fields}, not {@code properties}
     * — so the filter translator can tell the two dotted shapes apart:
     * struct children print as Lance nested field accesses, sub-fields
     * stay on the Lucene doc value path. A path crossing an entry of
     * type {@code nested} (a {@code List<Struct>} column) does not
     * resolve either: DataFusion has no {@code UNNEST} in a filter, so a
     * nested child predicate cannot travel to Lance SQL and belongs on
     * the Lucene side.
     */
    @SuppressWarnings("unchecked")
    private static Object resolveObjectPath(Map<String, Object> propertyMap, String name) {
        Map<String, Object> current = propertyMap;
        String[] segments = name.split("\\.");
        for (int s = 0; s < segments.length - 1; s++) {
            Object entry = current.get(segments[s]);
            if (!(entry instanceof Map)) {
                return null;
            }
            if ("nested".equals(((Map<String, Object>) entry).get("type"))) {
                return null;
            }
            Object nested = ((Map<String, Object>) entry).get("properties");
            if (!(nested instanceof Map)) {
                return null;
            }
            current = (Map<String, Object>) nested;
        }
        return current.get(segments[segments.length - 1]);
    }

    /**
     * Merge the per-node hit lists into one list ordered the way a
     * single executor would have ordered the union.
     *
     * <p>Each inner list is one node's response, already sorted by
     * that node's executor and cut to {@code from + size}, with the
     * Lance row address of every hit. The merge re-sorts the union
     * with a comparator built from {@code sorts}:
     * <ul>
     *   <li>no sort clause, or a single {@code _score} clause: score
     *       descending ({@link SearchHit#getScore()});</li>
     *   <li>a {@code _score} clause among others: the raw sort value
     *       at that position (the executor stores the score there;
     *       {@link SearchHit#getScore()} is NaN when the request did
     *       not set {@code track_scores}), in the clause's order;</li>
     *   <li>a {@code _doc} clause: row address in the clause's order.
     *       On a single reader over the whole table doc id order is
     *       fragment order then offset, which is row address order,
     *       so this is what the clause means there; the per-node doc
     *       ids the executors report as sort values only order docs
     *       within one node;</li>
     *   <li>any other clause: {@link SearchHit#getRawSortValues()} at
     *       that position, compared as {@link Comparable} in the
     *       clause's order. The executor already substituted the
     *       {@code missing} sentinel for numeric fields, so a null
     *       only arrives for keyword fields; it sorts last unless the
     *       clause says {@code "missing": "_first"}, the same default
     *       OpenSearch's comparator sources apply.</li>
     * </ul>
     * Hits that compare equal are ordered by index (the order of the
     * request's targets) and then by row address ascending. Lucene's
     * collectors break ties by doc id ascending, so every executor
     * returns the tied rows of its fragments in row address order and
     * the lowest addresses of the whole table are always among the
     * per-node pages; sorting the union by the same key therefore
     * yields the page one reader over the whole table would produce,
     * whatever the number of nodes.
     *
     * <p>{@code search_after} needs no handling here: each executor
     * already applied the cursor to its own hits, so every hit in
     * every inner list is past the cursor and the merged order is the
     * correct continuation.
     */
    static List<SearchHit> mergeHits(List<List<RankedHit>> perNodeHits, List<SortBuilder<?>> sorts) {
        List<RankedHit> ranked = new ArrayList<>();
        for (List<RankedHit> nodeHits : perNodeHits) {
            ranked.addAll(nodeHits);
        }
        if (ranked.size() > 1) {
            ranked.sort(hitComparator(sorts));
        }
        List<SearchHit> out = new ArrayList<>(ranked.size());
        for (RankedHit r : ranked) {
            out.add(r.hit());
        }
        return out;
    }

    /**
     * A per-node hit with what the merge needs to place it: the
     * ordinal of the index it came from in the request's target list
     * and its Lance row address ({@code fragmentId << 32 | offset}).
     * Row addresses are unique within one table, so the pair is a
     * total order over every hit of the request.
     */
    record RankedHit(SearchHit hit, int target, long rowAddr) {
    }

    private static Comparator<RankedHit> hitComparator(List<SortBuilder<?>> sorts) {
        Comparator<RankedHit> tieBreak = Comparator.comparingInt(RankedHit::target).thenComparingLong(RankedHit::rowAddr);
        if (sorts == null || sorts.isEmpty()) {
            return Comparator.<RankedHit>comparingDouble(r -> -scoreOf(r.hit())).thenComparing(tieBreak);
        }
        Comparator<RankedHit> comparator = null;
        for (int i = 0; i < sorts.size(); i++) {
            SortBuilder<?> sort = sorts.get(i);
            Comparator<RankedHit> clause = clauseComparator(sort, i);
            comparator = comparator == null ? clause : comparator.thenComparing(clause);
        }
        return comparator.thenComparing(tieBreak);
    }

    private static Comparator<RankedHit> clauseComparator(SortBuilder<?> sort, int index) {
        boolean descending = sort.order() == SortOrder.DESC;
        if (sort instanceof ScoreSortBuilder) {
            Comparator<RankedHit> byScore = (a, b) -> Float.compare(scoreAt(a.hit(), index), scoreAt(b.hit(), index));
            return descending ? byScore.reversed() : byScore;
        }
        boolean nullsFirst = false;
        if (sort instanceof FieldSortBuilder field) {
            if (FieldSortBuilder.DOC_FIELD_NAME.equals(field.getFieldName())) {
                Comparator<RankedHit> byRowAddr = Comparator.comparingLong(RankedHit::rowAddr);
                return descending ? byRowAddr.reversed() : byRowAddr;
            }
            nullsFirst = "_first".equals(field.missing());
        }
        final boolean nullsFirstFinal = nullsFirst;
        return (a, b) -> {
            Object left = rawSortValue(a.hit(), index);
            Object right = rawSortValue(b.hit(), index);
            if (left == null || right == null) {
                if (left == null && right == null) {
                    return 0;
                }
                // Missing placement is absolute (first or last in
                // the response), not relative to the clause
                // direction, so it is decided before the
                // direction flip below.
                return (left == null) == nullsFirstFinal ? -1 : 1;
            }
            int cmp = compareValues(left, right);
            return descending ? -cmp : cmp;
        };
    }

    private static float scoreOf(SearchHit hit) {
        float score = hit.getScore();
        // NaN would sort above every real score under Float.compare;
        // treat "no score" as the lowest score instead.
        return Float.isNaN(score) ? Float.NEGATIVE_INFINITY : score;
    }

    /**
     * Score for a {@code _score} sort clause: the raw sort value at
     * the clause position when the executor recorded one, else
     * {@link SearchHit#getScore()}.
     */
    private static float scoreAt(SearchHit hit, int index) {
        Object raw = rawSortValue(hit, index);
        if (raw instanceof Number number) {
            return number.floatValue();
        }
        return scoreOf(hit);
    }

    private static Object rawSortValue(SearchHit hit, int index) {
        Object[] raw = hit.getRawSortValues();
        if (raw == null || index >= raw.length) {
            return null;
        }
        return raw[index];
    }

    /**
     * Compare two non-null raw sort values. The executors type a
     * given clause identically on every node (the type comes from
     * the field mapping), so the common case is two values of the
     * same {@link Comparable} class. Mixed numeric widths, which can
     * only happen when two indexes in one request map a field
     * differently, are compared by value. Any other pair has no
     * defined order and would silently scramble the page, so it is
     * refused.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static int compareValues(Object left, Object right) {
        if (left.getClass() == right.getClass() && left instanceof Comparable) {
            return ((Comparable) left).compareTo(right);
        }
        if (left instanceof Number l && right instanceof Number r) {
            if (isIntegral(l) && isIntegral(r)) {
                return Long.compare(l.longValue(), r.longValue());
            }
            return Double.compare(l.doubleValue(), r.doubleValue());
        }
        throw new IllegalStateException(
            "cannot merge sort values of types [" + left.getClass().getName() + "] and [" + right.getClass().getName() + "]"
        );
    }

    private static boolean isIntegral(Number n) {
        return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
    }

    private SearchResponse emptyResponse(long took) {
        SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, false, null, 1);
        // Same rationale as MergeState.buildResponse: report a
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
     */
    private record IndexTarget(String indexName, String tableUri, StorageOptions storageOptions, long pinnedVersion, Function<
        String,
        String> fieldTypeLookup) {

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
     * resolves once and threads through the per-index fan-out. Keeps
     * the recursive {@link #runIndexLoop} / {@link #fanOutForTarget}
     * signatures short even as new wire-format fields are added.
     */
    private record FragmentQuerySpec(String filterSql, org.opensearch.index.query.QueryBuilder query,
        org.opensearch.index.query.QueryBuilder postFilter, List<org.opensearch.search.sort.SortBuilder<?>> sorts, Object[] searchAfter,
        int effectiveSize, AggregatorFactories.Builder aggregations, boolean trackScores, int trackTotalHitsUpTo) {
    }

    /**
     * Mutable accumulator that folds every fan-out result into the
     * final response state. Not thread-safe: guarded by the
     * sequential index loop.
     */
    private final class MergeState {

        private final AggregatorFactories.Builder aggregationsRequested;
        // Sort clauses of the request; drive the cross-node hit merge
        // in buildResponse. Empty means score order.
        private final List<SortBuilder<?>> sorts;
        // Requested pagination window. `perNodeSize` on the wire is
        // `from + size` so every per-node executor already returned
        // enough hits for us to skip the first `from` and keep `size`.
        private final int from;
        private final int size;
        // Whether the request asked for _version / _seq_no /
        // _primary_term envelope fields on hits. Fragment path has
        // no per-doc version accounting (Lance datasets are
        // append/rewrite, not per-doc versioned), so populated
        // values are constant: version=1, seqNo=0, primaryTerm=1.
        // The flags exist so the coordinator only stamps hits when
        // the caller explicitly asked, matching shard path
        // behaviour where these fields default off.
        private final boolean versionRequested;
        private final boolean seqNoAndPrimaryTermRequested;
        // track_total_hits bound the executors counted up to; decides
        // the hits.total relation in buildResponse.
        private final int trackTotalHitsUpTo;
        private long totalMatched = 0L;
        // Set when any executor stopped counting at the bound, so the
        // summed total is a lower bound even if it did not exceed
        // trackTotalHitsUpTo itself.
        private boolean matchedIsLowerBound = false;
        // Set when a node of any target did not answer in time: the
        // response is built from the nodes that did, says timed_out,
        // and reports hits.total as a lower bound.
        private boolean timedOut = false;
        // One entry per per-node response, in fan-out order (target
        // order, then node id order within a target). Each inner list
        // is already sorted by the executor and cut to from + size,
        // and carries the target ordinal and row address the merge
        // breaks ties on.
        private final List<List<RankedHit>> perNodeHits = new ArrayList<>();
        // Ordinal of the target whose responses absorbTargetResponses
        // is absorbing; targets arrive one after another in request
        // order, so this is the position of the target in the
        // request's index list.
        private int targetOrdinal = -1;
        private final List<InternalAggregations> perNodeAggregations = new ArrayList<>();

        MergeState(
            AggregatorFactories.Builder aggregationsRequested,
            List<SortBuilder<?>> sorts,
            int from,
            int size,
            boolean versionRequested,
            boolean seqNoAndPrimaryTermRequested,
            int trackTotalHitsUpTo
        ) {
            this.aggregationsRequested = aggregationsRequested;
            this.sorts = sorts;
            this.from = from;
            this.size = size;
            this.versionRequested = versionRequested;
            this.seqNoAndPrimaryTermRequested = seqNoAndPrimaryTermRequested;
            this.trackTotalHitsUpTo = trackTotalHitsUpTo;
        }

        void absorbTargetResponses(IndexTarget target, List<LanceFragmentQueryResponse> responses, boolean incomplete) {
            timedOut |= incomplete;
            // Every hit needs a SearchShardTarget so the response
            // envelope carries the {@code _index} key that clients
            // expect. Fragment path has no shard concept, so we
            // synthesise one entry keyed on the resolved index
            // metadata; the shard id is always 0 (single-shard).
            IndexMetadata indexMetadata = clusterService.state().metadata().index(target.indexName());
            SearchShardTarget shardTarget = indexMetadata == null
                ? null
                : new SearchShardTarget(
                    clusterService.localNode().getId(),
                    new ShardId(indexMetadata.getIndex(), 0),
                    /* clusterAlias */ null,
                    org.opensearch.action.OriginalIndices.NONE
                );
            targetOrdinal++;
            for (LanceFragmentQueryResponse response : responses) {
                totalMatched += response.matched();
                matchedIsLowerBound |= response.matchedIsLowerBound();
                // Keep each node's list intact; the sort merge and
                // the from/size cut run in buildResponse once every
                // node of every target has answered.
                List<SearchHit> hits = response.hits();
                long[] rowAddrs = response.rowAddrs();
                List<RankedHit> nodeHits = new ArrayList<>(hits.size());
                for (int i = 0; i < hits.size(); i++) {
                    SearchHit hit = hits.get(i);
                    stampEnvelope(hit, shardTarget);
                    nodeHits.add(new RankedHit(hit, targetOrdinal, rowAddrs[i]));
                }
                perNodeHits.add(nodeHits);
                if (response.aggregations() != null) {
                    perNodeAggregations.add(response.aggregations());
                }
            }
        }

        /**
         * Attach the shard target and, if requested, the constant
         * version / seq_no / primary_term envelope values to a
         * per-node hit. Runs on the coordinator because per-node
         * responses do not know the index name and because the
         * request-level flags live on {@link SearchSourceBuilder}
         * which is not shipped over the wire in full.
         */
        private void stampEnvelope(SearchHit hit, SearchShardTarget shardTarget) {
            if (shardTarget != null) {
                hit.shard(shardTarget);
            }
            if (versionRequested) {
                hit.version(1L);
            }
            if (seqNoAndPrimaryTermRequested) {
                hit.setSeqNo(0L);
                hit.setPrimaryTerm(1L);
            }
        }

        /**
         * {@code hits.total} of the merged response; see
         * {@link TransportLanceCoordinatorAction#totalHits(long, boolean, int)}.
         * When a node did not answer, the count of the nodes that did is
         * a lower bound of the true count whatever the tracking mode, so
         * the relation is {@code gte}; the value stays as composed (the
         * sum, or the bound when the sum passed it).
         */
        private TotalHits totalHits() {
            TotalHits total = TransportLanceCoordinatorAction.totalHits(totalMatched, matchedIsLowerBound, trackTotalHitsUpTo);
            if (total == null || !timedOut) {
                return total;
            }
            return new TotalHits(total.value(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        }

        SearchResponse buildResponse(long startMillis) {
            long took = System.currentTimeMillis() - startMillis;
            // Merge the per-node sorted lists into one ordered list,
            // then apply from/size so the response reflects the
            // requested pagination window. Every node returned up to
            // from + size hits, so the merged list always holds the
            // global top from + size.
            List<SearchHit> hits = mergeHits(perNodeHits, sorts);
            SearchHit[] paged;
            if (hits.size() <= from) {
                paged = new SearchHit[0];
            } else {
                int end = Math.min(hits.size(), from + size);
                paged = hits.subList(from, end).toArray(new SearchHit[0]);
            }
            // max_score is the largest per-hit score in the paged
            // window, matching shard path behaviour. The old
            // hard-coded 1.0f flattened the response for
            // function_score / script_score / FTS queries where
            // real scores can range far above 1.0. NaN when no
            // hits survive paging, or when every hit carries NaN
            // (e.g. sort without track_scores).
            float maxScore = Float.NaN;
            for (SearchHit hit : paged) {
                float score = hit.getScore();
                if (Float.isNaN(score)) {
                    continue;
                }
                if (Float.isNaN(maxScore) || score > maxScore) {
                    maxScore = score;
                }
            }
            SearchHits searchHits = new SearchHits(paged, totalHits(), maxScore);
            InternalAggregations aggregations = null;
            if (aggregationsRequested != null && !perNodeAggregations.isEmpty()) {
                // Feed every per-node InternalAggregations tree into the
                // stock reduce path so cross-node reduction lives in
                // OpenSearch's aggregator code rather than in the Lance
                // plugin: nodes ship InternalAggregations, the
                // coordinator calls topLevelReduce.
                InternalAggregation.ReduceContext ctx = InternalAggregation.ReduceContext.forFinalReduction(
                    bigArrays,
                    scriptService,
                    /* multiBucketConsumer */ n -> {},
                    org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree.EMPTY
                );
                aggregations = InternalAggregations.topLevelReduce(perNodeAggregations, ctx);
            }
            // timed_out is the only trace of a node that did not answer:
            // the fragment path reports one logical unit under _shards,
            // so there is no failed shard to count; the coordinator's
            // WARN log names the node, the fragment count and the
            // timeout. Aggregations are the reduce of the nodes that
            // answered, as on the shard path.
            SearchResponseSections sections = new SearchResponseSections(searchHits, aggregations, null, timedOut, false, null, 1);
            // Hide the Lance fragment fan-out from the response
            // shape. The user's mental model is one logical dataset,
            // not N shards; reporting fragmentCount here would leak
            // the Lucene-shard concept back into the API surface
            // that shard-free dispatch is meant to remove. total /
            // successful stay at 1 (single logical unit) so clients
            // scripts that expect at least one successful shard
            // continue to parse cleanly. The physical distribution
            // is still observable through the coordinator's INFO
            // logs and, in the future, dedicated telemetry.
            return new SearchResponse(
                sections,
                null,
                /* totalShards */ 1,
                /* successfulShards */ 1,
                /* skippedShards */ 0,
                took,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
        }
    }

    /**
     * {@code hits.total} under the request's {@code track_total_hits}
     * contract, composed the way
     * {@code SearchPhaseController.TopDocsStats#getTotalHits} does it
     * for shard results. {@code null} (no {@code total} block in the
     * response) when tracking is disabled. For {@code track_total_hits:
     * true} the summed per-node count is exact. With an integer bound
     * the relation is {@code gte} when the sum exceeds the bound or any
     * executor stopped counting at it, and the value is then the bound
     * itself, never the sum: each executor counts its own fragments'
     * share of one scan limited to {@code bound + 1} rows, and because
     * Lance picks among tied rows differently on every executor, the
     * shares can add up to less than the bound even though every scan
     * filled. Clients read {@code gte} with the bound as "more than the
     * bound" (the shard path never reports a smaller value with
     * {@code gte}), so the sum is only reported when it is exact.
     *
     * @param totalMatched sum of the per-node matched counts
     * @param matchedIsLowerBound whether any executor stopped counting
     *        at the bound
     * @param trackTotalHitsUpTo the bound the request asked for, or one
     *        of the {@link SearchContext} tracking constants
     */
    static TotalHits totalHits(long totalMatched, boolean matchedIsLowerBound, int trackTotalHitsUpTo) {
        if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
            return null;
        }
        if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
            if (matchedIsLowerBound) {
                // An executor may only stop counting under an integer
                // bound; a lower bound with an accurate request is an
                // executor contract bug. The value is still reported as
                // gte so the response does not claim an exactness it
                // does not have.
                LOGGER.warn(
                    "lance.dispatch: executor reported hits.total as a lower bound [{}] although track_total_hits requested "
                        + "an accurate count; reporting gte",
                    totalMatched
                );
                return new TotalHits(totalMatched, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
            }
            return new TotalHits(totalMatched, TotalHits.Relation.EQUAL_TO);
        }
        if (matchedIsLowerBound || totalMatched > trackTotalHitsUpTo) {
            return new TotalHits(trackTotalHitsUpTo, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        }
        return new TotalHits(totalMatched, TotalHits.Relation.EQUAL_TO);
    }
}
