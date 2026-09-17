/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.index.Index;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.IndexService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.dispatch.LanceMetricAggregator.MetricSpec;
import org.opensearch.lance.dispatch.LanceMetricAggregator.MetricType;
import org.opensearch.lance.dispatch.LanceMetricAggregator.PartialState;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.query.LanceScanFilterQuery;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketCollector;
import org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Per-node handler for {@link LanceFragmentQueryAction}. Opens the
 * Lance dataset through the shared {@link LanceRegistry} and runs the
 * requested scan on the node this instance lives on. The coordinator
 * groups fragments by node so each handler only scans its subset,
 * keeping the total work proportional to the fragments a node owns
 * instead of the entire dataset.
 *
 * <p>Two logical scans happen per invocation, at most:
 * <ul>
 *   <li>The <em>hits</em> scan honours {@link
 *       LanceFragmentQueryRequest#size()} and produces up to
 *       {@code size} {@link SearchHit} instances whose {@code _id} is
 *       synthesised from Lance's {@code _rowaddr} column. The filter
 *       is pushed into {@link ScanOptions.Builder#filter(String)} so
 *       the number of rows the scan iterates stays bounded by the
 *       filter selectivity.</li>
 *   <li>When the request carries metric specs, the <em>aggregate</em>
 *       step opens per-fragment {@link
 *       org.opensearch.lance.engine.LanceFragmentLeafReader}s through
 *       {@link LanceDirectoryReader#openForFragments}, wraps them in
 *       an {@link IndexSearcher}, and drives OpenSearch's stock
 *       {@link org.opensearch.search.aggregations.metrics.SumAggregator}
 *       / {@code AvgAggregator} / etc. against the reader through a
 *       {@link LanceFragmentSearchContext}. The filter is expressed
 *       as a {@link LanceScanFilterQuery} so the aggregator only sees
 *       matching rows, with the Lance native filter running per leaf.
 *       The per-metric {@link InternalSum} / {@link InternalAvg} /
 *       etc. is converted back to a {@link PartialState} so the wire
 *       format stays unchanged from earlier fragment-path milestones;
 *       the wire format switch to {@code InternalAggregations} is a
 *       later cleanup step.</li>
 * </ul>
 *
 * <p>For requests with no metric specs the {@code matched} row count
 * comes from a Lance metadata-only path
 * ({@link Fragment#countRows()} sums when no filter is set;
 * {@link Dataset#countRows(String)} otherwise). This keeps the
 * hits-only case cheap; it does not need the aggregator machinery at
 * all.
 */
public final class TransportLanceFragmentQueryAction extends HandledTransportAction<LanceFragmentQueryRequest, LanceFragmentQueryResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportLanceFragmentQueryAction.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final BigArrays bigArrays;
    private final CircuitBreakerService circuitBreakerService;

    @Inject
    public TransportLanceFragmentQueryAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndicesService indicesService,
        BigArrays bigArrays,
        CircuitBreakerService circuitBreakerService
    ) {
        super(LanceFragmentQueryAction.NAME, transportService, actionFilters, LanceFragmentQueryRequest::new);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.bigArrays = bigArrays;
        this.circuitBreakerService = circuitBreakerService;
    }

    @Override
    protected void doExecute(Task task, LanceFragmentQueryRequest request, ActionListener<LanceFragmentQueryResponse> listener) {
        try {
            LanceFragmentQueryResponse response = execute(request);
            listener.onResponse(response);
        } catch (Exception e) {
            LOGGER.warn(
                "fragment query failed on this node for [{}] filter [{}] fragments [{}]",
                request.tableUri(),
                request.filterSql() == null ? "<match_all>" : request.filterSql(),
                request.fragmentIds().isEmpty() ? "<all>" : request.fragmentIds(),
                e
            );
            listener.onFailure(e);
        }
    }

    /**
     * Package-private helper that does the actual scan work. Split
     * out so unit tests can call it without going through the
     * transport layer.
     */
    LanceFragmentQueryResponse execute(LanceFragmentQueryRequest request) throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(request.tableUri(), request.storageOptions())) {
            int fragmentCount = request.fragmentIds().isEmpty() ? dataset.getFragments().size() : request.fragmentIds().size();

            List<SearchHit> hits = scanHits(dataset, request.size(), request.filterSql(), request.fragmentIdsOrNull());

            List<Integer> allFragmentIds = new ArrayList<>();
            for (Fragment fragment : dataset.getFragments()) {
                allFragmentIds.add(fragment.getId());
            }
            List<PartialState> partials = aggregateViaIndexSearcher(request, allFragmentIds);

            long matched = computeMatched(dataset, request, partials);
            return new LanceFragmentQueryResponse(matched, fragmentCount, hits, partials);
        }
    }

    /**
     * Read up to {@code size} rows from the given fragment subset
     * and synthesise a {@link SearchHit} per row with an {@code _id}
     * of {@code "<fragmentId>-<offsetInFragment>"} and a JSON
     * {@code _source} rendered from the Arrow batch.
     */
    private List<SearchHit> scanHits(Dataset dataset, int size, String filterSql, List<Integer> fragmentIds) throws Exception {
        if (size <= 0) {
            return Collections.emptyList();
        }
        List<SearchHit> out = new ArrayList<>();
        ScanOptions.Builder builder = new ScanOptions.Builder().withRowAddress(true).limit((long) size);
        if (filterSql != null) {
            builder.filter(filterSql);
        }
        if (fragmentIds != null) {
            builder.fragmentIds(fragmentIds);
        }
        ScanOptions options = builder.build();
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            int hitIndex = 0;
            while (out.size() < size && reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                int rowCount = root.getRowCount();
                for (int i = 0; i < rowCount && out.size() < size; i++) {
                    long addr = rowAddr.get(i);
                    int fragmentId = (int) (addr >>> 32);
                    int offset = (int) (addr & 0xFFFFFFFFL);
                    String idString = fragmentId + "-" + offset;
                    SearchHit hit = new SearchHit(hitIndex, idString, Collections.emptyMap(), Collections.emptyMap());
                    hit.score(1.0f);
                    byte[] source = LanceRowSourceRenderer.renderJson(root, i);
                    hit.sourceRef(new BytesArray(source));
                    out.add(hit);
                    hitIndex++;
                }
            }
        }
        return out;
    }

    /**
     * Direction 1 aggregator path. Opens per-fragment
     * {@link org.opensearch.lance.engine.LanceFragmentLeafReader}s and
     * runs the metric specs through OpenSearch's stock
     * {@code SumAggregator} / {@code AvgAggregator} / {@code MinAggregator}
     * / {@code MaxAggregator} / {@code ValueCountAggregator} — the same
     * aggregator machinery the shard path uses — via a
     * {@link LanceFragmentSearchContext} substitute. The per-aggregator
     * result is converted back to a {@link PartialState} so the wire
     * format between coordinator and per-node handler stays unchanged
     * from earlier milestones.
     */
    private List<PartialState> aggregateViaIndexSearcher(LanceFragmentQueryRequest request, List<Integer> allFragmentIds) throws Exception {
        List<MetricSpec> metrics = request.metrics();
        if (metrics.isEmpty()) {
            return Collections.emptyList();
        }
        Metadata metadata = clusterService.state().metadata();
        IndexMetadata indexMetadata = metadata.index(request.indexName());
        if (indexMetadata == null) {
            throw new IllegalStateException(
                "Fragment aggregator path cannot resolve OpenSearch index [" + request.indexName() + "] on this node"
            );
        }
        Index index = indexMetadata.getIndex();
        IndexService indexService = indicesService.indexServiceSafe(index);
        IndexShard indexShard = indexService.getShard(0);

        List<Integer> effectiveFragmentIds =
            (request.fragmentIdsOrNull() == null || request.fragmentIdsOrNull().isEmpty()) ? allFragmentIds : request.fragmentIdsOrNull();

        String pkField = indexMetadata.getSettings().get("index.lance.primary_key_field", "");
        // Open a fresh Dataset just for the aggregator reader.
        // LanceDirectoryReader takes ownership of the Dataset it wraps and
        // closes it on doClose, so the caller's Dataset (which
        // execute() also uses for scanHits / computeMatched) must stay
        // untouched. Both handles share the node-scoped Lance Session
        // cache installed by LancePlugin.createComponents, so the extra
        // open is cheap.
        try (
            Dataset aggregatorDataset = LanceRegistry.openDataset(request.tableUri(), request.storageOptions());
            DirectoryReader dr = LanceDirectoryReader.openForFragments(
                new ByteBuffersDirectory(),
                null,
                aggregatorDataset,
                pkField,
                effectiveFragmentIds
            )
        ) {
            MultiBucketConsumer bucketConsumer = new MultiBucketConsumer(
                Integer.MAX_VALUE,
                circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
            );
            SearchContextAggregations searchContextAggregations = new SearchContextAggregations(
                AggregatorFactories.EMPTY,
                bucketConsumer
            );

            Query query = request.filterSql() == null ? new MatchAllDocsQuery() : new LanceScanFilterQuery(request.filterSql());
            try (
                LanceFragmentSearchContext searchContext = new LanceFragmentSearchContext(
                    indexShard,
                    query,
                    searchContextAggregations,
                    bigArrays,
                    indexService.cache().bitsetFilterCache(),
                    clusterService.localNode().getId()
                )
            ) {
                ContextIndexSearcher searcher = buildSearcher(dr, indexShard, searchContext);
                searchContext.withSearcher(searcher);
                QueryShardContext qsc = indexService.newQueryShardContext(0, searcher, System::currentTimeMillis, null);
                searchContext.withQueryShardContext(qsc);

                // Build AggregatorFactories: per-metric-spec, map to one or
                // two stock aggregators. For Sum / Min / Max / ValueCount a
                // single stock aggregator carries all the state we need. For
                // Avg we run Sum + ValueCount side by side because
                // InternalAvg's raw count and sum are package-private, then
                // rejoin them into the PartialState after collection.
                AggregatorFactories.Builder factoriesBuilder = new AggregatorFactories.Builder();
                Map<String, List<String>> specToInternal = new LinkedHashMap<>();
                for (MetricSpec spec : metrics) {
                    List<String> internalNames = new ArrayList<>(2);
                    switch (spec.type()) {
                        case SUM -> {
                            factoriesBuilder.addAggregator(new SumAggregationBuilder(spec.name()).field(spec.field()));
                            internalNames.add(spec.name());
                        }
                        case AVG -> {
                            String sumName = spec.name() + "$sum";
                            String countName = spec.name() + "$count";
                            factoriesBuilder.addAggregator(new SumAggregationBuilder(sumName).field(spec.field()));
                            factoriesBuilder.addAggregator(new ValueCountAggregationBuilder(countName).field(spec.field()));
                            internalNames.add(sumName);
                            internalNames.add(countName);
                        }
                        case MIN -> {
                            factoriesBuilder.addAggregator(new MinAggregationBuilder(spec.name()).field(spec.field()));
                            internalNames.add(spec.name());
                        }
                        case MAX -> {
                            factoriesBuilder.addAggregator(new MaxAggregationBuilder(spec.name()).field(spec.field()));
                            internalNames.add(spec.name());
                        }
                        case VALUE_COUNT -> {
                            factoriesBuilder.addAggregator(new ValueCountAggregationBuilder(spec.name()).field(spec.field()));
                            internalNames.add(spec.name());
                        }
                    }
                    specToInternal.put(spec.name(), internalNames);
                }
                AggregatorFactories factories = factoriesBuilder.build(qsc, null);

                List<Aggregator> topLevelAggregators = factories.createTopLevelAggregators(searchContext);
                Aggregator[] aggregators = topLevelAggregators.toArray(new Aggregator[0]);
                for (Aggregator agg : aggregators) {
                    agg.preCollection();
                }
                BucketCollector wrapped = MultiBucketCollector.wrap(topLevelAggregators);
                searcher.search(query, wrapped);
                for (Aggregator agg : aggregators) {
                    agg.postCollection();
                }

                Map<String, InternalAggregation> byInternalName = new LinkedHashMap<>();
                for (Aggregator agg : aggregators) {
                    InternalAggregation[] results = agg.buildAggregations(new long[] { 0L });
                    byInternalName.put(agg.name(), results[0]);
                }

                List<PartialState> partials = new ArrayList<>(metrics.size());
                for (MetricSpec spec : metrics) {
                    partials.add(toPartialState(spec, byInternalName, specToInternal.get(spec.name())));
                }
                return partials;
            }
        }
    }

    /**
     * Wrap the {@link DirectoryReader} in a {@link ContextIndexSearcher}
     * bound to the caller's {@link LanceFragmentSearchContext} substitute.
     * The Lance-backed reader has its own freshness tracking (see the
     * LanceReaderManager comment in {@code LanceReadOnlyEngine}) so
     * Lucene's per-query cache is redundant.
     */
    private ContextIndexSearcher buildSearcher(DirectoryReader dr, IndexShard indexShard, LanceFragmentSearchContext searchContext)
        throws java.io.IOException {
        return new ContextIndexSearcher(
            dr,
            org.apache.lucene.search.IndexSearcher.getDefaultSimilarity(),
            new org.opensearch.index.cache.query.DisabledQueryCache(indexShard.indexSettings()),
            new org.apache.lucene.search.QueryCachingPolicy() {
                @Override
                public void onUse(Query query) {}

                @Override
                public boolean shouldCache(Query query) {
                    return false;
                }
            },
            false,
            null,
            searchContext
        );
    }

    /**
     * Fold stock {@link InternalAggregation} results back into the
     * {@link PartialState} shape the coordinator's merge phase
     * consumes. The wire format across coordinator and per-node
     * handlers stays unchanged from earlier milestones; switching
     * that format to a native {@link InternalAggregation} carrier is
     * a later cleanup step.
     *
     * <p>Unused {@code PartialState} fields for a given metric type
     * are set to the neutral {@link PartialState#EMPTY} values so the
     * associative {@link PartialState#merge} stays correct across
     * multiple per-node responses that mix metric types on the same
     * field.
     *
     * <p>AVG carries synthetic sum + count aggregators (see
     * {@link #aggregateViaIndexSearcher}), so its raw state is
     * reconstructed from the two internal names.
     */
    private PartialState toPartialState(MetricSpec spec, Map<String, InternalAggregation> byInternalName, List<String> internalNames) {
        return switch (spec.type()) {
            case SUM -> new PartialState(
                0L,
                ((InternalSum) byInternalName.get(internalNames.get(0))).getValue(),
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY
            );
            case AVG -> {
                double sum = ((InternalSum) byInternalName.get(internalNames.get(0))).getValue();
                long count = (long) ((InternalValueCount) byInternalName.get(internalNames.get(1))).getValue();
                yield new PartialState(count, sum, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
            }
            case MIN -> new PartialState(
                0L,
                0.0d,
                ((InternalMin) byInternalName.get(internalNames.get(0))).getValue(),
                Double.NEGATIVE_INFINITY
            );
            case MAX -> new PartialState(
                0L,
                0.0d,
                Double.POSITIVE_INFINITY,
                ((InternalMax) byInternalName.get(internalNames.get(0))).getValue()
            );
            case VALUE_COUNT -> new PartialState(
                (long) ((InternalValueCount) byInternalName.get(internalNames.get(0))).getValue(),
                0.0d,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY
            );
        };
    }

    /**
     * Determine the number of rows in this node's fragment subset
     * that satisfy the filter.
     *
     * <p>When at least one metric spec is present the aggregator scan
     * already visited every matching row; we can reuse its result
     * because every supported metric type increments {@code count}
     * per non-null row. VALUE_COUNT specs preserve the count directly,
     * so we look for one first. If the spec list has no VALUE_COUNT we
     * pick any spec and derive count from it if possible; when none
     * of the specs happens to carry a count (Sum / Min / Max return
     * only their own value), fall through to the metadata-only path.
     *
     * <p>Without metrics we take a shortcut appropriate to the
     * filter:
     * <ul>
     *   <li>No filter: sum {@link org.lance.Fragment#countRows()}
     *       across the assigned fragments (Lance metadata, no
     *       scan).</li>
     *   <li>Filter set, all fragments assigned: use
     *       {@link Dataset#countRows(String)}.</li>
     *   <li>Filter set, subset of fragments: run a bounded scan
     *       over the subset and count matching rows.</li>
     * </ul>
     */
    private long computeMatched(Dataset dataset, LanceFragmentQueryRequest request, List<PartialState> partials) throws Exception {
        if (!partials.isEmpty()) {
            for (int i = 0; i < request.metrics().size(); i++) {
                MetricSpec spec = request.metrics().get(i);
                if (spec.type() == MetricType.VALUE_COUNT || spec.type() == MetricType.AVG) {
                    // These fill PartialState.count exactly.
                    return partials.get(i).count();
                }
            }
            // Fall through: no count-carrying metric in the specs.
        }
        List<Integer> fragmentIds = request.fragmentIdsOrNull();
        String filterSql = request.filterSql();
        if (filterSql == null) {
            if (fragmentIds == null) {
                return dataset.countRows();
            }
            long total = 0L;
            List<Fragment> allFragments = dataset.getFragments();
            for (Fragment fragment : allFragments) {
                if (fragmentIds.contains(fragment.getId())) {
                    total += fragment.countRows();
                }
            }
            return total;
        }
        if (fragmentIds == null) {
            return dataset.countRows(filterSql);
        }
        // Filter + fragment subset: scan and count. Cheap for
        // typical query workloads because the filter narrows the
        // row set before the scan even starts.
        ScanOptions options = new ScanOptions.Builder().filter(filterSql).fragmentIds(fragmentIds).build();
        long count = 0L;
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                count += reader.getVectorSchemaRoot().getRowCount();
            }
        }
        return count;
    }
}
