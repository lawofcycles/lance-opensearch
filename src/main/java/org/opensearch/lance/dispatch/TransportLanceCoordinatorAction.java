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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.dispatch.LanceMetricAggregator.MetricSpec;
import org.opensearch.lance.dispatch.LanceMetricAggregator.PartialState;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.query.LanceKnnFilterTranslator;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
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

    @Inject
    public TransportLanceCoordinatorAction(
        TransportService transportService,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ActionFilters actionFilters
    ) {
        super(LanceCoordinatorAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
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
        List<MetricSpec> metrics = resolveMetrics(source);
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
        MergeState merged = new MergeState(metrics, effectiveSize);
        runIndexLoop(targets, 0, nodeList, filterSql, effectiveSize, metrics, merged, start, listener);
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
        List<MetricSpec> metrics,
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
                metrics,
                merged,
                ActionListener.wrap(
                    v -> runIndexLoop(targets, index + 1, nodeList, filterSql, effectiveSize, metrics, merged, startMillis, listener),
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
        List<MetricSpec> metrics,
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

        Map<DiscoveryNode, List<Integer>> perNode = groupFragmentsByNode(allFragmentIds, nodeList);
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
                target.storageOptions(),
                filterSql,
                effectiveSize,
                metrics,
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

    private static List<MetricSpec> resolveMetrics(SearchSourceBuilder source) {
        Optional<List<MetricSpec>> parsed = LanceMetricAggregator.parseSupported(source);
        return parsed.orElse(java.util.Collections.emptyList());
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
        return new SearchResponse(sections, null, 0, 0, 0, took, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    private record IndexTarget(String indexName, String tableUri, StorageOptions storageOptions) {
    }

    /**
     * Mutable accumulator that folds every fan-out result into the
     * final response state. Not thread-safe: guarded by the
     * sequential index loop.
     */
    private static final class MergeState {

        private final List<MetricSpec> metrics;
        private final int effectiveSize;
        private long totalMatched = 0L;
        private int totalFragments = 0;
        private final List<SearchHit> hits = new ArrayList<>();
        private final List<List<PartialState>> perGroupPartials = new ArrayList<>();

        MergeState(List<MetricSpec> metrics, int effectiveSize) {
            this.metrics = metrics;
            this.effectiveSize = effectiveSize;
        }

        void absorbTargetResponses(Collection<LanceFragmentQueryResponse> responses) {
            for (LanceFragmentQueryResponse response : responses) {
                totalMatched += response.matched();
                totalFragments += response.fragmentCount();
                for (SearchHit hit : response.hits()) {
                    if (hits.size() < effectiveSize) {
                        hits.add(hit);
                    }
                }
                if (!response.partials().isEmpty()) {
                    perGroupPartials.add(response.partials());
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
            InternalAggregations aggregations = metrics.isEmpty() ? null : LanceMetricAggregator.mergePartials(metrics, perGroupPartials);
            SearchResponseSections sections = new SearchResponseSections(searchHits, aggregations, null, false, false, null, 1);
            return new SearchResponse(
                sections,
                null,
                totalFragments,
                totalFragments,
                0,
                took,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
        }
    }
}
