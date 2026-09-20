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
import java.util.function.Function;

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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.query.LanceKnnFilterTranslator;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
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
 * behaviour is covered by {@code LanceMultiNodeIT}.
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
        super(LanceCoordinatorAction.NAME, transportService, actionFilters, SearchRequest::new, ThreadPool.Names.SEARCH);
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

        org.opensearch.index.query.QueryBuilder query = source == null ? null : source.query();
        org.opensearch.index.query.QueryBuilder postFilter = source == null ? null : source.postFilter();
        List<org.opensearch.search.sort.SortBuilder<?>> sorts = source == null || source.sorts() == null
            ? java.util.Collections.emptyList()
            : source.sorts();
        Object[] searchAfter = source == null ? null : source.searchAfter();
        AggregatorFactories.Builder aggregations = source == null ? null : source.aggregations();
        int size = resolveSize(source);
        int from = resolveFrom(source);
        // Each per-node executor needs from + size docs so the coordinator
        // has enough hits after skipping `from`. Deep pagination costs
        // linear memory per node just like the shard path — no additional
        // fragment-level penalty.
        int perNodeSize = from + size;

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
            source != null && source.trackScores()
        );
        boolean versionRequested = source != null && Boolean.TRUE.equals(source.version());
        boolean seqNoAndPrimaryTermRequested = source != null && Boolean.TRUE.equals(source.seqNoAndPrimaryTerm());
        MergeState merged = new MergeState(aggregations, from, size, versionRequested, seqNoAndPrimaryTermRequested);
        runIndexLoop(targets, 0, nodeList, spec, source, merged, start, listener);
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
            spec.trackScores()
        );
        try {
            fanOutForTarget(
                target,
                nodeList,
                perTargetSpec,
                merged,
                ActionListener.wrap(
                    v -> runIndexLoop(targets, index + 1, nodeList, spec, source, merged, startMillis, listener),
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
        FragmentQuerySpec spec,
        MergeState merged,
        ActionListener<Void> done
    ) throws Exception {
        List<Integer> allFragmentIds;
        try (Dataset dataset = LanceRegistry.openDataset(target.tableUri(), target.storageOptions(), target.pinnedVersionOrEmpty())) {
            allFragmentIds = new ArrayList<>(dataset.getFragments().size());
            dataset.getFragments().forEach(fragment -> allFragmentIds.add(fragment.getId()));
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
            // Send a single fan-out to the primary node with an
            // empty fragment set. The per-node executor opens a
            // LanceDirectoryReader with zero leaves, runs the
            // aggregators over zero docs, and returns an empty
            // InternalAggregations tree that the coordinator merges
            // via topLevelReduce. Same wire format as any other
            // fan-out; the only novel case is the reader being
            // shaped to maxDoc=0.
            List<DiscoveryNode> primaries = nodeListForTarget(target, nodeList);
            if (primaries.isEmpty()) {
                done.onResponse(null);
                return;
            }
            dispatchEmptyAggregationRun(target, primaries.get(0), spec, merged, done);
            return;
        }

        Map<DiscoveryNode, List<Integer>> perNode = groupFragmentsByNode(allFragmentIds, nodeListForTarget(target, nodeList));
        int fanOutSize = perNode.size();

        GroupedActionListener<LanceFragmentQueryResponse> gathered = new GroupedActionListener<>(ActionListener.wrap(responses -> {
            merged.absorbTargetResponses(target, responses);
            done.onResponse(null);
        }, done::onFailure), fanOutSize);

        for (Map.Entry<DiscoveryNode, List<Integer>> assignment : perNode.entrySet()) {
            DiscoveryNode nodeTarget = assignment.getKey();
            List<Integer> fragmentsForNode = assignment.getValue();
            LanceFragmentQueryRequest fragmentRequest = new LanceFragmentQueryRequest(
                target.tableUri(),
                target.indexName(),
                target.storageOptions(),
                target.pinnedVersion(),
                spec.filterSql(),
                spec.query(),
                spec.postFilter(),
                spec.sorts(),
                spec.searchAfter(),
                spec.effectiveSize(),
                spec.aggregations(),
                fragmentsForNode,
                spec.trackScores()
            );
            LOGGER.info(
                "lance.dispatch: fan-out index [{}] table [{}] to node [{}] with fragments {}",
                target.indexName(),
                target.tableUri(),
                nodeTarget.getId(),
                fragmentsForNode
            );
            // Dispatch through TransportService so remote data nodes
            // receive the request. For
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
     * Send a single fan-out to the primary node with an empty
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
        DiscoveryNode primary,
        FragmentQuerySpec spec,
        MergeState merged,
        ActionListener<Void> done
    ) {
        LanceFragmentQueryRequest fragmentRequest = new LanceFragmentQueryRequest(
            target.tableUri(),
            target.indexName(),
            target.storageOptions(),
            target.pinnedVersion(),
            spec.filterSql(),
            spec.query(),
            spec.postFilter(),
            spec.sorts(),
            spec.searchAfter(),
            spec.effectiveSize(),
            spec.aggregations(),
            java.util.Collections.emptyList(),
            spec.trackScores()
        );
        LOGGER.info(
            "lance.dispatch: fan-out index [{}] table [{}] empty aggregation run on primary node [{}]",
            target.indexName(),
            target.tableUri(),
            primary.getId()
        );
        transportService.sendRequest(
            primary,
            LanceFragmentQueryAction.NAME,
            fragmentRequest,
            new org.opensearch.transport.TransportResponseHandler<LanceFragmentQueryResponse>() {
                @Override
                public LanceFragmentQueryResponse read(org.opensearch.core.common.io.stream.StreamInput in) throws java.io.IOException {
                    return new LanceFragmentQueryResponse(in);
                }

                @Override
                public void handleResponse(LanceFragmentQueryResponse response) {
                    merged.absorbTargetResponses(target, java.util.List.of(response));
                    done.onResponse(null);
                }

                @Override
                public void handleException(org.opensearch.transport.TransportException exp) {
                    done.onFailure(exp);
                }

                @Override
                public String executor() {
                    return org.opensearch.threadpool.ThreadPool.Names.SEARCH;
                }
            }
        );
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
     * Return the single data node that hosts the primary shard of
     * the target index.
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
     * <p>Lance-backed indices are single-shard. With the default
     * {@code number_of_replicas=0} exactly one data node holds the
     * shard, and the fragment fan-out collapses to that node. With
     * {@code auto_expand_replicas} or an explicit replica count the
     * routing table also carries replica copies; if the coordinator
     * merged partials from both primary and replica hosts it would
     * concatenate two disjoint sets of fragments and per-node sort
     * order would leak into the top-level {@code hits} sequence.
     * The fragment path has no per-node sort merge, so we pin
     * fan-out to the primary. Every fragment still executes because
     * Lance fragments live in external storage: the primary node can
     * open any fragment through
     * {@link org.opensearch.lance.LanceRegistry#openDataset}.
     *
     * <p>If cluster state has no {@link IndexRoutingTable} for the
     * index yet (very early in create-index handling) or the primary
     * is not yet {@link ShardRouting#started()}, fall back to the
     * caller's full node list. The receiving node then surfaces a
     * clear {@code IndexNotFoundException} in that rare case.
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
        String primaryNodeId = null;
        for (IndexShardRoutingTable shardTable : routingTable) {
            ShardRouting primary = shardTable.primaryShard();
            if (primary != null && primary.started()) {
                primaryNodeId = primary.currentNodeId();
                break;
            }
        }
        if (primaryNodeId == null) {
            return fullList;
        }
        for (DiscoveryNode node : fullList) {
            if (node.getId().equals(primaryNodeId)) {
                return java.util.Collections.singletonList(node);
            }
        }
        return fullList;
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
            // Same reading of index.lance.version as the shard engine
            // (LanceEngineFactory.newReadWriteEngine): -1 follows the
            // latest manifest, anything else pins. Resolving it here and
            // shipping it to the per-node executor keeps _search on the
            // manifest _count / _stats / GET already serve.
            long pinnedVersion = indexMetadata.getSettings().getAsLong(LanceEngineFactory.VERSION_SETTING, -1L);
            String tag = indexMetadata.getSettings().get(LanceEngineFactory.TAG_SETTING, "");
            if (pinnedVersion < 0 && !tag.isEmpty()) {
                // A tag-following index pins to whatever version the tag
                // points at right now, so resolve it here and ship the
                // version exactly like an explicit pin. This costs one
                // extra Dataset.open of the latest manifest per request
                // per tag-following index (the tag lives in the table's
                // refs, not in any manifest); the shared Lance Session
                // keeps the metadata cached so it is a small, fixed cost
                // rather than a table scan.
                pinnedVersion = LanceRegistry.resolveTagVersion(tableUri, storageOptions, tag);
            }
            targets.add(new IndexTarget(index.getName(), tableUri, storageOptions, pinnedVersion, buildFieldTypeLookup(indexMetadata)));
        }
        return targets;
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
     * entry is what we need. Nested objects are not surfaced yet,
     * so this shallow walk covers today's mappings.
     */
    @SuppressWarnings("unchecked")
    private static java.util.function.Function<String, String> buildFieldTypeLookup(IndexMetadata indexMetadata) {
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
            if (!(field instanceof java.util.Map)) {
                return null;
            }
            Object type = ((java.util.Map<String, Object>) field).get("type");
            return type instanceof String ? (String) type : null;
        };
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
     * is the {@code index.lance.version} setting ({@code -1} when
     * the index follows the latest manifest); it drives the
     * coordinator's own fragment enumeration and travels with every
     * per-node request so the executors open the same manifest.
     */
    private record IndexTarget(String indexName, String tableUri, StorageOptions storageOptions, long pinnedVersion, Function<
        String,
        String> fieldTypeLookup) {

        Optional<Long> pinnedVersionOrEmpty() {
            return pinnedVersion >= 0 ? Optional.of(pinnedVersion) : Optional.empty();
        }
    }

    /**
     * Immutable bundle of the query-time settings the coordinator
     * resolves once and threads through the per-index fan-out. Keeps
     * the recursive {@link #runIndexLoop} / {@link #fanOutForTarget}
     * signatures short even as new wire-format fields are added.
     */
    private record FragmentQuerySpec(String filterSql, org.opensearch.index.query.QueryBuilder query,
        org.opensearch.index.query.QueryBuilder postFilter, List<org.opensearch.search.sort.SortBuilder<?>> sorts, Object[] searchAfter,
        int effectiveSize, AggregatorFactories.Builder aggregations, boolean trackScores) {
    }

    /**
     * Mutable accumulator that folds every fan-out result into the
     * final response state. Not thread-safe: guarded by the
     * sequential index loop.
     */
    private final class MergeState {

        private final AggregatorFactories.Builder aggregationsRequested;
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
        private long totalMatched = 0L;
        private final List<SearchHit> hits = new ArrayList<>();
        private final List<InternalAggregations> perNodeAggregations = new ArrayList<>();

        MergeState(
            AggregatorFactories.Builder aggregationsRequested,
            int from,
            int size,
            boolean versionRequested,
            boolean seqNoAndPrimaryTermRequested
        ) {
            this.aggregationsRequested = aggregationsRequested;
            this.from = from;
            this.size = size;
            this.versionRequested = versionRequested;
            this.seqNoAndPrimaryTermRequested = seqNoAndPrimaryTermRequested;
        }

        void absorbTargetResponses(IndexTarget target, Collection<LanceFragmentQueryResponse> responses) {
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
            for (LanceFragmentQueryResponse response : responses) {
                totalMatched += response.matched();
                // Keep every hit until we finish collecting per-node
                // responses; the skip/limit runs in buildResponse
                // where we know the full merged set. Multi-node sort
                // merge is future work — for now the coordinator
                // concatenates in per-node arrival order and relies on
                // the per-node executor's own sort/topN cut.
                for (SearchHit hit : response.hits()) {
                    stampEnvelope(hit, shardTarget);
                    if (hits.size() < from + size) {
                        hits.add(hit);
                    }
                }
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

        SearchResponse buildResponse(long startMillis) {
            long took = System.currentTimeMillis() - startMillis;
            // Apply from/size to the collected hits so the response
            // reflects the requested pagination window.
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
            SearchHits searchHits = new SearchHits(paged, new TotalHits(totalMatched, TotalHits.Relation.EQUAL_TO), maxScore);
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
