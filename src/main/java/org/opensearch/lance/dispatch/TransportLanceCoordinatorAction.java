/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.lance.Dataset;
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
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.query.LanceKnnFilterTranslator;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Coordinator handler for shard-free dispatch. Receives a
 * {@link SearchRequest} the {@link LanceDispatchActionFilter}
 * already decided is fragment-dispatchable, resolves the target
 * indexes and query metadata, enumerates fragments through the
 * shared {@link LanceRegistry}, groups them by data node (currently
 * round-robin), and fans requests out via
 * {@link LanceFragmentQueryAction}. Once every per-node response
 * arrives, it merges the partial hits + partial metric state into
 * a single {@link SearchResponse}.
 *
 * <p>Single-node clusters take this same path with a data-node list
 * of length one, so the transport hop reduces to a local
 * {@code sendRequest} against the loopback pool. Multi-node
 * behaviour is verified separately in Milestone 5-C4's integration
 * cluster.
 */
public final class TransportLanceCoordinatorAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportLanceCoordinatorAction.class);

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final BigArrays bigArrays;
    private final ScriptService scriptService;

    @Inject
    public TransportLanceCoordinatorAction(
        TransportService transportService,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ActionFilters actionFilters,
        BigArrays bigArrays,
        ScriptService scriptService
    ) {
        super(LanceCoordinatorAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
    }

    @Override
    protected void doExecute(Task task, SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        try {
            executeCoordinated(searchRequest, listener);
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
    private void executeCoordinated(SearchRequest searchRequest, ActionListener<SearchResponse> listener) throws Exception {
        long start = System.currentTimeMillis();
        SearchSourceBuilder source = searchRequest.source();

        String filterSql = resolveFilterSql(source);
        AggregatorFactories.Builder aggregations = source == null ? null : source.aggregations();
        int effectiveSize = resolveSize(source);

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
        // Sequential loop keeps the merge trivial; parallel per-index
        // fan-out is future work if it becomes a hot spot.
        MergeState merged = new MergeState(aggregations, effectiveSize);
        runIndexLoop(targets, 0, nodeList, filterSql, effectiveSize, aggregations, merged, start, listener);
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
        String filterSql,
        int effectiveSize,
        AggregatorFactories.Builder aggregations,
        MergeState merged,
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
                filterSql,
                effectiveSize,
                aggregations,
                merged,
                ActionListener.wrap(
                    v -> runIndexLoop(targets, index + 1, nodeList, filterSql, effectiveSize, aggregations, merged, startMillis, listener),
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
     * {@link LanceFragmentQueryAction} per node. Completion signals
     * through {@code done} once every response is merged into
     * {@code merged}.
     */
    private void fanOutForTarget(
        IndexTarget target,
        List<DiscoveryNode> nodeList,
        String filterSql,
        int effectiveSize,
        AggregatorFactories.Builder aggregations,
        MergeState merged,
        ActionListener<Void> done
    ) throws Exception {
        List<Integer> allFragmentIds;
        try (Dataset dataset = LanceRegistry.openDataset(target.tableUri(), target.storageOptions())) {
            allFragmentIds = new ArrayList<>(dataset.getFragments().size());
            dataset.getFragments().forEach(fragment -> allFragmentIds.add(fragment.getId()));
        }
        if (allFragmentIds.isEmpty()) {
            // Empty table: nothing to fan out, no partials to merge.
            done.onResponse(null);
            return;
        }

        Map<DiscoveryNode, List<Integer>> perNode = groupFragmentsByNode(allFragmentIds, nodeListForTarget(target, nodeList));
        int fanOutSize = perNode.size();

        GroupedActionListener<LanceFragmentQueryResponse> gathered = new GroupedActionListener<>(ActionListener.wrap(responses -> {
            merged.absorbTargetResponses(responses);
            done.onResponse(null);
        }, done::onFailure), fanOutSize);

        for (Map.Entry<DiscoveryNode, List<Integer>> assignment : perNode.entrySet()) {
            DiscoveryNode nodeTarget = assignment.getKey();
            List<Integer> fragmentsForNode = assignment.getValue();
            LanceFragmentQueryRequest fragmentRequest = new LanceFragmentQueryRequest(
                target.tableUri(),
                target.indexName(),
                target.storageOptions(),
                filterSql,
                effectiveSize,
                aggregations,
                fragmentsForNode
            );
            LOGGER.info(
                "lance.dispatch: fan-out index [{}] table [{}] to node [{}] with fragments {}",
                target.indexName(),
                target.tableUri(),
                nodeTarget.getId(),
                fragmentsForNode
            );
            // Milestone 5-C4: dispatch through TransportService so
            // remote data nodes actually receive the request. For
            // the local node this still executes in-process because
            // TransportService's request handler dispatch is loopback
            // aware, but any other node in the cluster picks up its
            // slice through the network. The handler wraps the
            // GroupedActionListener so per-node failures propagate
            // through GroupedActionListener.onFailure and abort the
            // fan-out cleanly.
            transportService.sendRequest(
                nodeTarget,
                LanceFragmentQueryAction.NAME,
                fragmentRequest,
                new org.opensearch.transport.TransportResponseHandler<LanceFragmentQueryResponse>() {
                    @Override
                    public LanceFragmentQueryResponse read(org.opensearch.core.common.io.stream.StreamInput in) throws java.io.IOException {
                        return new LanceFragmentQueryResponse(in);
                    }

                    @Override
                    public void handleResponse(LanceFragmentQueryResponse response) {
                        gathered.onResponse(response);
                    }

                    @Override
                    public void handleException(org.opensearch.transport.TransportException exp) {
                        gathered.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return org.opensearch.threadpool.ThreadPool.Names.SEARCH;
                    }
                }
            );
        }
    }

    /**
     * Round-robin fragment ids across the sorted data-node list.
     * Empty per-node bucket entries are omitted so downstream
     * dispatch code only sees nodes that actually own work.
     */
    private static Map<DiscoveryNode, List<Integer>> groupFragmentsByNode(List<Integer> fragmentIds, List<DiscoveryNode> nodeList) {
        Map<DiscoveryNode, List<Integer>> result = new HashMap<>();
        for (int i = 0; i < fragmentIds.size(); i++) {
            DiscoveryNode node = nodeList.get(i % nodeList.size());
            result.computeIfAbsent(node, k -> new ArrayList<>()).add(fragmentIds.get(i));
        }
        return result;
    }

    /**
     * Filter the data-node list down to nodes that hold at least one
     * started shard copy of the target index.
     *
     * <p>The fragment path drives OpenSearch's aggregator machinery
     * through {@link org.opensearch.index.shard.IndexShard} so the
     * receiving node must have the index initialised in its
     * {@link org.opensearch.indices.IndicesService}. A node without a
     * started shard has no IndexService yet
     * ({@code IndicesService.indexServiceSafe} throws
     * {@code IndexNotFoundException}), so the fragment query would
     * fail there.
     *
     * <p>Lance-backed indices are created with {@code
     * number_of_replicas=0} so, in a multi-node cluster, only one
     * data node holds the shard at steady state. Fragment fan-out
     * collapses to that node when this method runs. Every fragment
     * still executes because Lance fragments live in external
     * storage: the shard-holding node can open any fragment through
     * {@link org.opensearch.lance.LanceRegistry#openDataset}. To
     * reintroduce cross-node parallelism, operators can request more
     * shards (or replicas) — each additional shard copy widens the
     * set of nodes this filter accepts.
     *
     * <p>If cluster state has no {@link IndexRoutingTable} for the
     * index yet (very early in create-index handling) or no shard is
     * started anywhere, fall back to the caller's full node list.
     * The receiving node then surfaces a clear
     * {@code IndexNotFoundException} in that rare case.
     */
    private List<DiscoveryNode> nodeListForTarget(IndexTarget target, List<DiscoveryNode> fullList) {
        IndexMetadata indexMetadata = clusterService.state().metadata().index(target.indexName());
        if (indexMetadata == null) {
            return fullList;
        }
        IndexRoutingTable routingTable = clusterService.state().routingTable().index(indexMetadata.getIndex());
        if (routingTable == null) {
            return fullList;
        }
        java.util.Set<String> nodesWithShard = new java.util.HashSet<>();
        for (IndexShardRoutingTable shardTable : routingTable) {
            for (ShardRouting shardRouting : shardTable) {
                if (shardRouting.started()) {
                    nodesWithShard.add(shardRouting.currentNodeId());
                }
            }
        }
        List<DiscoveryNode> filtered = new ArrayList<>(fullList.size());
        for (DiscoveryNode node : fullList) {
            if (nodesWithShard.contains(node.getId())) {
                filtered.add(node);
            }
        }
        return filtered.isEmpty() ? fullList : filtered;
    }

    private static String resolveFilterSql(SearchSourceBuilder source) {
        if (source == null) {
            return null;
        }
        Object query = source.query();
        if (query == null || query instanceof org.opensearch.index.query.MatchAllQueryBuilder) {
            return null;
        }
        return LanceKnnFilterTranslator.toLanceSql((org.opensearch.index.query.QueryBuilder) query);
    }

    private static int resolveSize(SearchSourceBuilder source) {
        if (source == null || source.size() < 0) {
            return 10;
        }
        return source.size();
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
            targets.add(new IndexTarget(index.getName(), tableUri, storageOptions));
        }
        return targets;
    }

    private SearchResponse emptyResponse(long took) {
        SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, false, null, 1);
        // Same rationale as MergeState.buildResponse: report a
        // single logical unit rather than 0 shards so clients that
        // check {@code _shards.total >= 1} keep parsing correctly.
        return new SearchResponse(sections, null, 1, 1, 0, took, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    private record IndexTarget(String indexName, String tableUri, StorageOptions storageOptions) {
    }

    /**
     * Mutable accumulator that folds every fan-out result into the
     * final response state. Not thread-safe: guarded by the
     * sequential index loop.
     */
    private final class MergeState {

        private final AggregatorFactories.Builder aggregationsRequested;
        private final int effectiveSize;
        private long totalMatched = 0L;
        private final List<SearchHit> hits = new ArrayList<>();
        private final List<InternalAggregations> perNodeAggregations = new ArrayList<>();

        MergeState(AggregatorFactories.Builder aggregationsRequested, int effectiveSize) {
            this.aggregationsRequested = aggregationsRequested;
            this.effectiveSize = effectiveSize;
        }

        void absorbTargetResponses(Collection<LanceFragmentQueryResponse> responses) {
            for (LanceFragmentQueryResponse response : responses) {
                totalMatched += response.matched();
                for (SearchHit hit : response.hits()) {
                    if (hits.size() < effectiveSize) {
                        hits.add(hit);
                    }
                }
                if (response.aggregations() != null) {
                    perNodeAggregations.add(response.aggregations());
                }
            }
        }

        SearchResponse buildResponse(long startMillis) {
            long took = System.currentTimeMillis() - startMillis;
            SearchHits searchHits = new SearchHits(
                hits.toArray(new SearchHit[0]),
                new TotalHits(totalMatched, TotalHits.Relation.EQUAL_TO),
                hits.isEmpty() ? Float.NaN : 1.0f
            );
            InternalAggregations aggregations = null;
            if (aggregationsRequested != null && !perNodeAggregations.isEmpty()) {
                // Feed every per-node InternalAggregations tree into the
                // stock reduce path so cross-node reduction lives in
                // OpenSearch's aggregator code rather than in the Lance
                // plugin. Direction 1 Stage 2 wire format: nodes ship
                // InternalAggregations, coordinator calls topLevelReduce,
                // no plugin-specific merge logic in the middle.
                InternalAggregation.ReduceContext ctx = InternalAggregation.ReduceContext.forFinalReduction(
                    bigArrays,
                    scriptService,
                    /* multiBucketConsumer */ n -> {},
                    org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree.EMPTY
                );
                aggregations = InternalAggregations.topLevelReduce(perNodeAggregations, ctx);
            }
            SearchResponseSections sections = new SearchResponseSections(searchHits, aggregations, null, false, false, null, 1);
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
}
