/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.index.Index;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.IndexService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.query.LanceScanFilterQuery;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.MultiBucketCollector;
import org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Per-node handler for {@link LanceFragmentQueryAction}. Opens the
 * Lance dataset through the shared {@link LanceRegistry} and runs
 * the requested scan on the node this instance lives on. The
 * coordinator groups fragments by node so each handler only scans
 * its subset, keeping the total work proportional to the fragments
 * a node owns instead of the entire dataset.
 *
 * <p>Since Direction 1 Stage 3 both hits and aggregations flow through
 * OpenSearch's stock query / aggregator machinery driven against a
 * per-fragment {@link org.opensearch.lance.engine.LanceFragmentLeafReader}
 * bundle:
 * <ul>
 *   <li>{@link org.apache.lucene.search.IndexSearcher#search(Query, int)}
 *       returns the top-{@code size} docs with real scores (BM25 for
 *       Lance FTS, cosine for Lance knn) and the standard stored-fields
 *       path materialises {@code _source} and {@code _id} via
 *       {@link org.opensearch.lance.engine.LanceFragmentLeafReader#materialiseStoredFields}.
 *       The Lance-native scan + hand-rolled Arrow JSON path is retired.</li>
 *   <li>The aggregator branch drives OpenSearch's stock
 *       {@link org.opensearch.search.aggregations.metrics.SumAggregator}
 *       / {@code Avg} / {@code Min} / {@code Max} / {@code ValueCount}
 *       / bucket aggregators through the same
 *       {@link LanceFragmentSearchContext} substitute against the same
 *       reader. The wire format is native
 *       {@link InternalAggregations}, reduced at the coordinator via
 *       {@link InternalAggregations#topLevelReduce}.</li>
 * </ul>
 *
 * <p>The {@code matched} row count comes from Lance's metadata-only
 * path ({@link Fragment#countRows()} sums when no filter is set;
 * {@link Dataset#countRows(String)} otherwise). This keeps
 * hits.total.value cheap; the aggregator does not need to be
 * consulted for it.
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
     *
     * <p>Opens a single {@link LanceDirectoryReader} covering the
     * requested fragments and drives OpenSearch's stock hits + agg
     * pipelines through one {@link ContextIndexSearcher}. The
     * previous per-request separation (Lance-native hits scan +
     * separate Lucene aggregator scan) collapses to one Lucene scan
     * that answers both.
     */
    LanceFragmentQueryResponse execute(LanceFragmentQueryRequest request) throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(request.tableUri(), request.storageOptions())) {
            int fragmentCount = request.fragmentIds().isEmpty() ? dataset.getFragments().size() : request.fragmentIds().size();

            List<Integer> allFragmentIds = new ArrayList<>();
            for (Fragment fragment : dataset.getFragments()) {
                allFragmentIds.add(fragment.getId());
            }
            List<Integer> effectiveFragmentIds = (request.fragmentIdsOrNull() == null || request.fragmentIdsOrNull().isEmpty())
                ? allFragmentIds
                : request.fragmentIdsOrNull();

            IndexMetadata indexMetadata = clusterService.state().metadata().index(request.indexName());
            if (indexMetadata == null) {
                throw new IllegalStateException(
                    "Fragment path cannot resolve OpenSearch index [" + request.indexName() + "] on this node"
                );
            }
            Index index = indexMetadata.getIndex();
            IndexService indexService = indicesService.indexServiceSafe(index);
            IndexShard indexShard = indexService.getShard(0);
            String pkField = indexMetadata.getSettings().get("index.lance.primary_key_field", "");

            // Open a fresh Dataset for the reader: LanceDirectoryReader takes
            // ownership of the Dataset and closes it in doClose. The
            // node-scoped Lance Session cache makes the second open cheap.
            try (
                Dataset readerDataset = LanceRegistry.openDataset(request.tableUri(), request.storageOptions());
                DirectoryReader dr = LanceDirectoryReader.openForFragments(
                    new ByteBuffersDirectory(),
                    null,
                    readerDataset,
                    pkField,
                    effectiveFragmentIds
                )
            ) {
                Query query = request.filterSql() == null ? new MatchAllDocsQuery() : new LanceScanFilterQuery(request.filterSql());

                MultiBucketConsumer bucketConsumer = new MultiBucketConsumer(
                    Integer.MAX_VALUE,
                    circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
                );
                SearchContextAggregations searchContextAggregations = new SearchContextAggregations(
                    AggregatorFactories.EMPTY,
                    bucketConsumer
                );

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

                    List<SearchHit> hits = scanHitsViaIndexSearcher(searcher, query, request.size());
                    InternalAggregations aggregations = aggregateViaIndexSearcher(request, searchContext, searcher, qsc, query);
                    long matched = computeMatched(dataset, request);
                    return new LanceFragmentQueryResponse(matched, fragmentCount, hits, aggregations);
                }
            }
        }
    }

    /**
     * Run the top-{@code size} query on the shared
     * {@link ContextIndexSearcher} and materialise every hit through
     * OpenSearch's stock stored-fields path
     * ({@link LanceFragmentLeafReader#materialiseStoredFields}).
     * The reader is built by the caller so hits and aggregations
     * share one Lucene scan of the fragment subset.
     *
     * <p>Since Stage 3 the score is the real Lucene score
     * (BM25 for Lance FTS, cosine for Lance knn, 1.0 for
     * {@link MatchAllDocsQuery}) instead of the hard-coded 1.0 the
     * previous Lance-native scan wrote. The Lance-only Arrow-batch
     * hand-rolled JSON renderer is retired: the {@code _source}
     * bytes come from {@code LanceFragmentLeafReader.materialiseStoredFields}
     * which builds the same JSON through
     * {@link org.opensearch.core.xcontent.XContentBuilder}.
     */
    private List<SearchHit> scanHitsViaIndexSearcher(ContextIndexSearcher searcher, Query query, int size) throws java.io.IOException {
        if (size <= 0) {
            return Collections.emptyList();
        }
        TopDocs topDocs = searcher.search(query, size);
        List<SearchHit> out = new ArrayList<>(topDocs.scoreDocs.length);
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[i];
            HitVisitor visitor = new HitVisitor();
            searcher.storedFields().document(scoreDoc.doc, visitor);
            SearchHit hit = new SearchHit(i, visitor.idString(), Collections.emptyMap(), Collections.emptyMap());
            hit.score(scoreDoc.score);
            if (visitor.source != null) {
                hit.sourceRef(new org.opensearch.core.common.bytes.BytesArray(visitor.source));
            }
            out.add(hit);
        }
        return out;
    }

    /**
     * StoredFieldVisitor that captures the {@code _id} and
     * {@code _source} bytes {@link LanceFragmentLeafReader#materialiseStoredFields}
     * emits per hit. Reused for every doc in {@link
     * #scanHitsViaIndexSearcher} to avoid allocating a new visitor
     * per doc; the two capture fields are reset by the visitor
     * itself on each {@code document} call.
     */
    private static final class HitVisitor extends org.apache.lucene.index.StoredFieldVisitor {

        private byte[] source;
        private byte[] idBytes;

        @Override
        public Status needsField(org.apache.lucene.index.FieldInfo fieldInfo) {
            String name = fieldInfo.name;
            if ("_id".equals(name) || "_source".equals(name)) {
                return Status.YES;
            }
            return Status.NO;
        }

        @Override
        public void binaryField(org.apache.lucene.index.FieldInfo fieldInfo, byte[] value) {
            if ("_id".equals(fieldInfo.name)) {
                idBytes = value;
            } else if ("_source".equals(fieldInfo.name)) {
                source = value;
            }
        }

        String idString() {
            if (idBytes == null) {
                return "";
            }
            return org.opensearch.index.mapper.Uid.decodeId(idBytes);
        }
    }

    /**
     * Direction 1 aggregator path. Runs the request's
     * {@link org.opensearch.search.aggregations.AggregatorFactories.Builder}
     * against the shared per-fragment reader and returns the
     * per-node {@link InternalAggregations} for the coordinator to
     * reduce.
     *
     * <p>Returns {@code null} when the request carries no
     * aggregations; the response ships {@code aggregations == null}
     * in that case.
     */
    private InternalAggregations aggregateViaIndexSearcher(
        LanceFragmentQueryRequest request,
        LanceFragmentSearchContext searchContext,
        ContextIndexSearcher searcher,
        QueryShardContext qsc,
        Query query
    ) throws Exception {
        AggregatorFactories.Builder factoriesBuilder = request.aggregations();
        if (factoriesBuilder == null || factoriesBuilder.getAggregatorFactories().isEmpty()) {
            return null;
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

        List<InternalAggregation> results = new ArrayList<>(aggregators.length);
        for (Aggregator agg : aggregators) {
            InternalAggregation[] built = agg.buildAggregations(new long[] { 0L });
            results.add(built[0]);
        }
        return InternalAggregations.from(results);
    }

    /**
     * Wrap the {@link DirectoryReader} in a {@link ContextIndexSearcher}
     * bound to the caller's {@link LanceFragmentSearchContext} substitute.
     * The Lance-backed reader has its own freshness tracking (see the
     * LanceReaderManager comment in {@code LanceEngineFactory}) so
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
     * Determine the number of rows in this node's fragment subset
     * that satisfy the filter. Uses Lance's metadata-only counting
     * whenever possible:
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
    private long computeMatched(Dataset dataset, LanceFragmentQueryRequest request) throws Exception {
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
        org.lance.ipc.ScanOptions options = new org.lance.ipc.ScanOptions.Builder().filter(filterSql).fragmentIds(fragmentIds).build();
        long total = 0L;
        try (
            org.lance.ipc.LanceScanner scanner = dataset.newScan(options);
            org.apache.arrow.vector.ipc.ArrowReader reader = scanner.scanBatches()
        ) {
            while (reader.loadNextBatch()) {
                total += reader.getVectorSchemaRoot().getRowCount();
            }
        }
        return total;
    }

    /**
     * Suppress unused-import warnings from javadoc {@link ...}
     * references. Kept private to avoid altering the class's public
     * surface.
     */
    @SuppressWarnings("unused")
    private static void javadocReferences(org.opensearch.lance.engine.LanceFragmentLeafReader unused1) {}
}
