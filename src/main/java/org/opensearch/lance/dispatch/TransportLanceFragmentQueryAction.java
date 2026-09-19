/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.index.Index;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.IndexService;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.query.LanceFtsQuery;
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
import org.opensearch.threadpool.ThreadPool;
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

    /**
     * Reflective handle on {@code IndexService.getReaderWrapper()}.
     *
     * <p>OpenSearch declares the accessor package-private
     * (annotated with {@code // pkg private for testing}), so a
     * plugin cannot reach it through the normal type system. The
     * security plugin still needs the wrapper it installs on
     * IndexService to be applied to every reader that answers a
     * search request, otherwise DLS row filters and FLS field masks
     * never touch fragment path hits. The plugin-security.policy
     * already carries {@code ReflectPermission
     * "suppressAccessChecks"} for Arrow's C Data internals, so we
     * lean on that permission here to bridge into the package-private
     * method rather than adding a new grant.
     *
     * <p>Loaded once at class init: a {@code NoSuchMethodException}
     * would mean the plugin was built against an OpenSearch version
     * that removed or renamed the accessor, in which case failing
     * fast is more useful than silently bypassing DLS/FLS.
     */
    private static final Method INDEX_SERVICE_GET_READER_WRAPPER = resolveReaderWrapperAccessor();

    /**
     * Resolve the reflective handle for
     * {@code IndexService#getReaderWrapper()} once at class load.
     * The accessor is package-private in OpenSearch core, so we
     * have to go through {@code getDeclaredMethod} +
     * {@code setAccessible}; those are on the forbidden APIs list
     * so we scope the suppression to this single helper. The
     * plugin-security.policy already grants
     * {@code ReflectPermission "suppressAccessChecks"}, so runtime
     * policy is not affected by this suppression.
     *
     * <p>{@code NoSuchMethodException} means the plugin was built
     * against an OpenSearch version that removed or renamed the
     * accessor; refuse to load the class rather than silently
     * bypassing DLS/FLS.
     */
    @SuppressForbidden(reason = "IndexService#getReaderWrapper() is package-private in core; "
        + "reflection is required to apply DLS/FLS on the fragment path until upstream exposes it")
    private static Method resolveReaderWrapperAccessor() {
        try {
            Method m = IndexService.class.getDeclaredMethod("getReaderWrapper");
            m.setAccessible(true);
            return m;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                "IndexService#getReaderWrapper is not available on this OpenSearch build; refusing to serve Lance fragment queries because DLS/FLS would be bypassed silently",
                e
            );
        }
    }

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final BigArrays bigArrays;
    private final CircuitBreakerService circuitBreakerService;
    /**
     * Bound on how many fragment path queries this node runs in
     * parallel. Fragment path serves an entire index's fragments on
     * one node, so per-query heap (score arrays, aggregation
     * buffers) scales with concurrency rather than shard fan-out;
     * the semaphore keeps allocation from racing the
     * {@code lance_native} circuit breaker into an OOM. Backed by
     * {@link LancePlugin#FRAGMENT_DISPATCH_MAX_CONCURRENT_SETTING},
     * read once at construction so the permit count is fixed for
     * the life of the node. Threads waiting for a permit are
     * SEARCH threadpool threads, which is the same pool the
     * fan-out sender uses, so a queue never grows without bound —
     * once every search thread is either running or waiting here,
     * the transport layer applies its own queue limits.
     */
    private final java.util.concurrent.Semaphore concurrencyLimit;

    @Inject
    public TransportLanceFragmentQueryAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndicesService indicesService,
        BigArrays bigArrays,
        CircuitBreakerService circuitBreakerService
    ) {
        super(LanceFragmentQueryAction.NAME, transportService, actionFilters, LanceFragmentQueryRequest::new, ThreadPool.Names.SEARCH);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.bigArrays = bigArrays;
        this.circuitBreakerService = circuitBreakerService;
        int permits = org.opensearch.lance.LancePlugin.FRAGMENT_DISPATCH_MAX_CONCURRENT_SETTING.get(clusterService.getSettings());
        this.concurrencyLimit = new java.util.concurrent.Semaphore(permits, /*fair*/ false);
    }

    @Override
    protected void doExecute(Task task, LanceFragmentQueryRequest request, ActionListener<LanceFragmentQueryResponse> listener) {
        boolean acquired = false;
        try {
            // Bound the fragment path concurrency before we touch any
            // per-query buffers. Blocking here parks the SEARCH thread
            // that carried the request in, which pushes back on the
            // fan-out sender the same way any other slow shard would;
            // it is preferable to letting the JVM race the circuit
            // breaker into OOM.
            concurrencyLimit.acquire();
            acquired = true;
            LanceFragmentQueryResponse response = execute(request);
            listener.onResponse(response);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            listener.onFailure(interrupted);
        } catch (Exception e) {
            LOGGER.warn(
                "fragment query failed on this node for [{}] filter [{}] fragments [{}]",
                request.tableUri(),
                request.filterSql() == null ? "<match_all>" : request.filterSql(),
                request.fragmentIds().isEmpty() ? "<all>" : request.fragmentIds(),
                e
            );
            listener.onFailure(e);
        } finally {
            if (acquired) {
                concurrencyLimit.release();
            }
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
                throw new IllegalStateException("Fragment path cannot resolve OpenSearch index [" + request.indexName() + "] on this node");
            }
            Index index = indexMetadata.getIndex();
            IndexService indexService = indicesService.indexServiceSafe(index);
            IndexShard indexShard = indexService.getShard(0);
            String pkField = indexMetadata.getSettings().get("index.lance.primary_key_field", "");
            // Parse the type setting through the same fromSetting helper the
            // engine uses so unknown values fall back to LONG. Empty pkField
            // overrides whatever the type says (see the reader constructor
            // for the canonicalisation).
            org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType pkType = pkField.isEmpty()
                ? org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType.NONE
                : org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType.fromSetting(
                    indexMetadata.getSettings().get("index.lance.primary_key_type", "long")
                );
            // Multi-fields spec is persisted as JSON in a single setting.
            // Empty (no attach-body clause) leaves the reader with an empty
            // sub-field map. Malformed JSON falls through to
            // IllegalArgumentException, which the outer catch turns into a
            // 500 for the caller; that is loud enough to surface a bad
            // index setting without hiding the failure behind an empty map.
            java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields = org.opensearch.lance.rest.RestAttachAction
                .deserialiseMultiFields(indexMetadata.getSettings().get("index.lance.multi_fields", ""));

            // Open a fresh Dataset for the reader: LanceDirectoryReader
            // takes ownership of the Dataset and closes it in doClose.
            // The node-scoped Lance Session cache makes the second
            // open cheap. Any reader wrapper installed on IndexService
            // (most importantly the security plugin's DLS/FLS wrapper)
            // is applied before the searcher is built so document- and
            // field-level filtering apply to fragment path hits the
            // same way they apply to shard path hits.
            try (
                Dataset readerDataset = LanceRegistry.openDataset(request.tableUri(), request.storageOptions());
                DirectoryReader dr = openWrappedReader(
                    indexService,
                    indexShard,
                    readerDataset,
                    pkField,
                    pkType,
                    multiFields,
                    effectiveFragmentIds,
                    // Push the coordinator-translated Lance SQL down
                    // to the leaf reader. When the top-level query is
                    // a scalar filter LanceKnnFilterTranslator can
                    // express (bool / term / terms / range / exists /
                    // match_all), request.filterSql() carries the SQL
                    // and every per-column Lance scan the leaf reader
                    // issues inside ensureXxxLoaded is layered with
                    // that filter, so `filter + terms agg` and
                    // `filter + sum` no longer materialise every row
                    // of the aggregated column when only a fraction
                    // matches. FTS and knn queries do not have a
                    // SQL representation so filterSql is null there
                    // and the leaf reader falls back to unfiltered
                    // full-column scans, matching the pre-Phase-C
                    // behaviour for those shapes.
                    request.filterSql()
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

                // Placeholder query for the LanceFragmentSearchContext ctor;
                // resolveLuceneQuery() runs after we have the QueryShardContext.
                Query placeholderQuery = new MatchAllDocsQuery();

                try (
                    LanceFragmentSearchContext searchContext = new LanceFragmentSearchContext(
                        indexShard,
                        placeholderQuery,
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

                    Query query = resolveLuceneQuery(request, qsc);
                    Query hitsQuery = applyPostFilter(query, request, qsc);
                    // Match count runs through the same Weight as the
                    // hits phase when the query is a scoring Lucene
                    // query (FTS, knn), so strip any scan-limit hint
                    // from the query before handing it to
                    // computeMatched. Without this, an FTS Weight
                    // that was clipped to `size` rows during
                    // scanHitsViaIndexSearcher would also clip the
                    // count and hits.total.value would collapse to
                    // `size`.
                    Query countQuery = withoutScanLimit(hitsQuery);
                    org.opensearch.search.sort.SortAndFormats sortAndFormats = resolveSort(request, qsc);

                    // Aggregations run over the top-level query only —
                    // OpenSearch semantics for post_filter say the
                    // filter applies to hits (and hits.total.value)
                    // but not to aggregations. Hits and matched
                    // therefore use the AND-combined query.
                    List<SearchHit> hits = scanHitsViaIndexSearcher(
                        searcher,
                        hitsQuery,
                        sortAndFormats,
                        request.searchAfter(),
                        request.size(),
                        request.trackScores()
                    );
                    InternalAggregations aggregations = aggregateViaIndexSearcher(request, searchContext, searcher, qsc, query);
                    long matched = computeMatched(dataset, request, searcher, countQuery);
                    return new LanceFragmentQueryResponse(matched, fragmentCount, hits, aggregations);
                }
            }
        }
    }

    /**
     * Translate the request's OpenSearch-native query representation
     * into a Lucene {@link Query} the shared {@link ContextIndexSearcher}
     * can execute. Priority order:
     * <ol>
     *   <li>{@link LanceFragmentQueryRequest#query()} — the top-level
     *       {@link org.opensearch.index.query.QueryBuilder} the
     *       coordinator forwarded. Runs through the local
     *       {@link QueryShardContext#toQuery} so per-node mapping
     *       decisions (Lance FTS field types, knn field types,
     *       etc.) apply.</li>
     *   <li>{@link LanceFragmentQueryRequest#filterSql()} — a
     *       Lance SQL filter the coordinator translated ahead of
     *       time from a pure-filter query. Wrapped in
     *       {@link LanceScanFilterQuery} so Lance native evaluates
     *       the predicate per leaf.</li>
     *   <li>Otherwise: {@link MatchAllDocsQuery}.</li>
     * </ol>
     * The two shapes are mutually exclusive on the wire (the
     * coordinator sets one or the other), so precedence is only a
     * belt-and-braces guard against future double-set bugs.
     */
    /**
     * Strip any scan-limit hint from a Lance-backed Query so it can
     * be reused for match-count purposes.
     *
     * <p>{@link LanceFtsQuery} and {@link LanceScanFilterQuery} both
     * embed an optional top-k inside the {@link Query} instance
     * itself: the fragment scan uses that hint to stop early during
     * the hits phase. The same instance is also what
     * {@link #computeMatched} hands to {@code IndexSearcher.count}
     * when there is no cheaper counting path (scoring queries, or
     * scan-filter queries wrapped by post_filter). Counting the top
     * {@code size} rows instead of every matched row would collapse
     * {@code hits.total.value} to {@code size}, so build a fresh
     * unbounded copy before the count call.
     *
     * <p>Non-Lance queries fall through unchanged; nested queries
     * (bool / boost / dis_max wrapping a LanceFtsQuery) are the
     * same story — the resolver keeps the scan limit at
     * {@link LanceScanFilterQuery#SCAN_LIMIT_UNBOUNDED} for those
     * shapes, so nothing needs to be rewritten here.
     */
    private static Query withoutScanLimit(Query query) {
        if (query instanceof LanceFtsQuery fts && fts.scanLimit() != LanceFtsQuery.SCAN_LIMIT_UNBOUNDED) {
            return fts.withScanLimit(LanceFtsQuery.SCAN_LIMIT_UNBOUNDED);
        }
        if (query instanceof LanceScanFilterQuery scan && scan.scanLimit() != LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED) {
            return new LanceScanFilterQuery(scan.filterSql(), LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED);
        }
        return query;
    }

    private Query resolveLuceneQuery(LanceFragmentQueryRequest request, QueryShardContext qsc) throws java.io.IOException {
        if (request.query() != null) {
            Query base = request.query().toQuery(qsc);
            // If the request shape allows top-k pushdown and the
            // resulting Lucene tree is a bare LanceFtsQuery (single
            // lance_match / lance_match_phrase / etc. at the root),
            // ship the size hint into it so Lance's FTS scorer can
            // stop after k score-sorted rows. Callers wrapping the
            // FTS clause in a bool / boost / dis_max keep the
            // sentinel: mixing the top-k with other scorers would
            // clip the wrong side. Phase B follow-ups can extend the
            // rewrite deeper once we teach the scorer to negotiate
            // with siblings.
            int scanLimit = resolveScanFilterTopK(request);
            if (scanLimit != LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED && base instanceof LanceFtsQuery fts) {
                return fts.withScanLimit(scanLimit);
            }
            return base;
        }
        if (request.filterSql() != null) {
            int scanLimit = resolveScanFilterTopK(request);
            return new LanceScanFilterQuery(request.filterSql(), scanLimit);
        }
        return new MatchAllDocsQuery();
    }

    /**
     * Decide whether the pure scalar filter shape can push a
     * {@code limit(size)} into the per-fragment Lance scan.
     *
     * <p>Enabled only when every consumer of the matched set inside
     * this transport action is content with the top-k (or when there
     * are no consumers at all):
     * <ul>
     *   <li>No sort clause — Lance's row-address ordering matches
     *       what {@link org.apache.lucene.search.IndexSearcher#search(Query, int)}
     *       returns for a scalar query.</li>
     *   <li>No aggregations — aggregators need every matched doc to
     *       accumulate bucket counts and metric state.</li>
     *   <li>No post_filter — post_filter narrows below the scan and
     *       would leave the caller short of the requested rows.</li>
     * </ul>
     *
     * <p>The coordinator already folds the top-level {@code from} into
     * {@code size} before shipping the request (see
     * {@link TransportLanceCoordinatorAction#executeCoordinated}'s
     * {@code perNodeSize = from + size} calculation), so
     * {@link LanceFragmentQueryRequest#size()} is the per-node top-k
     * the coordinator is asking for. Using it directly here is safe.
     *
     * <p>{@code hits.total.value} is served by
     * {@link #computeMatched}, which for the scalar-filter shape
     * dispatches straight to {@link Dataset#countRows(String)} and
     * therefore is not affected by the scan clip.
     *
     * <p>Returns {@link LanceScanFilterQuery#SCAN_LIMIT_UNBOUNDED}
     * when top-k pushdown is not safe.
     */
    private int resolveScanFilterTopK(LanceFragmentQueryRequest request) {
        if (!request.sorts().isEmpty()) {
            return LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED;
        }
        if (request.aggregations() != null && !request.aggregations().getAggregatorFactories().isEmpty()) {
            return LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED;
        }
        if (request.postFilter() != null) {
            return LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED;
        }
        int size = request.size();
        if (size <= 0) {
            // size:0 count-only shape never enters
            // scanHitsViaIndexSearcher, and computeMatched serves the
            // total from Dataset.countRows without going through this
            // scorer, so we can leave the scan unbounded.
            return LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED;
        }
        return size;
    }

    /**
     * Translate the request's sort clauses (native
     * {@link org.opensearch.search.sort.SortBuilder} shape) into an
     * OpenSearch {@link org.opensearch.search.sort.SortAndFormats}
     * pair the shared searcher can pass to
     * {@link org.apache.lucene.search.IndexSearcher#search(Query, int, org.apache.lucene.search.Sort)}.
     * Returns {@code null} when the request has no sort — the
     * searcher then orders by score.
     */
    private org.opensearch.search.sort.SortAndFormats resolveSort(LanceFragmentQueryRequest request, QueryShardContext qsc)
        throws java.io.IOException {
        if (request.sorts().isEmpty()) {
            return null;
        }
        return org.opensearch.search.sort.SortBuilder.buildSort(request.sorts(), qsc).orElse(null);
    }

    /**
     * Combine the top-level query with {@code post_filter} into the
     * Lucene query used for hits and matched counting. Returns the
     * unmodified {@code base} when no post_filter is set.
     * Aggregations still run against {@code base} because
     * OpenSearch semantics say post_filter applies only to hits.
     */
    private static Query applyPostFilter(Query base, LanceFragmentQueryRequest request, QueryShardContext qsc) throws java.io.IOException {
        if (request.postFilter() == null) {
            return base;
        }
        Query pf = request.postFilter().toQuery(qsc);
        return new org.apache.lucene.search.BooleanQuery.Builder().add(base, org.apache.lucene.search.BooleanClause.Occur.MUST)
            .add(pf, org.apache.lucene.search.BooleanClause.Occur.FILTER)
            .build();
    }

    /**
     * Run the top-{@code size} query on the shared
     * {@link ContextIndexSearcher} and materialise every hit through
     * OpenSearch's stock stored-fields path
     * ({@link org.opensearch.lance.engine.LanceFragmentLeafReader#materialiseStoredFields}).
     * The reader is built by the caller so hits and aggregations
     * share one Lucene scan of the fragment subset.
     *
     * <p>Since Stage 3 the score is the real Lucene score
     * (BM25 for Lance FTS, cosine for Lance knn, 1.0 for
     * {@link MatchAllDocsQuery}) instead of the hard-coded 1.0 the
     * previous Lance-native scan wrote. Sort clauses go through the
     * standard {@link org.apache.lucene.search.IndexSearcher#search(Query, int, org.apache.lucene.search.Sort)}
     * call and per-hit sort values are captured for the coordinator's
     * merge phase.
     *
     * <p>{@code trackScores} follows the OpenSearch
     * {@code track_scores} request flag. Sort-based Lucene search
     * defaults to computing sort values only, leaving
     * {@link ScoreDoc#score} at {@link Float#NaN}. When the caller
     * asks for {@code track_scores:true} the 4 / 5 argument
     * {@link org.apache.lucene.search.IndexSearcher#search(Query, int, org.apache.lucene.search.Sort, boolean)}
     * / {@code searchAfter} overloads compute scores alongside the
     * sort, so hits come back with numeric {@code _score} values
     * and the coordinator's {@code max_score} sees real numbers.
     * The score-only path ({@code sortAndFormats == null}) already
     * collects scores through
     * {@link org.apache.lucene.search.IndexSearcher#search(Query, int)}
     * and ignores this flag.
     */
    private List<SearchHit> scanHitsViaIndexSearcher(
        ContextIndexSearcher searcher,
        Query query,
        org.opensearch.search.sort.SortAndFormats sortAndFormats,
        Object[] searchAfter,
        int size,
        boolean trackScores
    ) throws java.io.IOException {
        if (size <= 0) {
            return Collections.emptyList();
        }
        TopDocs topDocs;
        if (searchAfter != null && sortAndFormats != null) {
            // FieldDoc.doc is Lucene's tie-breaker for docs sharing
            // the sort value with the cursor. Setting it just past
            // the reader's last doc means "exclude the tied doc",
            // which matches OpenSearch's usual search_after
            // semantics. Integer.MAX_VALUE is rejected by Lucene's
            // pre-flight (`>= maxDoc`), so pin the value to
            // `maxDoc - 1` (or 0 when the reader is empty).
            int maxDoc = searcher.getIndexReader().maxDoc();
            int afterDoc = maxDoc > 0 ? maxDoc - 1 : 0;
            org.apache.lucene.search.FieldDoc after = new org.apache.lucene.search.FieldDoc(afterDoc, 0f, searchAfter);
            topDocs = searcher.searchAfter(after, query, size, sortAndFormats.sort, trackScores);
        } else if (sortAndFormats == null) {
            topDocs = searcher.search(query, size);
        } else {
            topDocs = searcher.search(query, size, sortAndFormats.sort, trackScores);
        }
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
            if (sortAndFormats != null && scoreDoc instanceof org.apache.lucene.search.FieldDoc fieldDoc) {
                hit.sortValues(fieldDoc.fields, sortAndFormats.formats);
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

        // ContextIndexSearcher.search() ends by calling
        // searchContext.bucketCollectorProcessor().processPostCollection(collector),
        // which already runs postCollection() and buildTopLevel() on
        // every top-level aggregator in the collector tree and stores
        // the result inside the aggregator (Aggregator#internalAggregation).
        // Read those stored results back out through
        // getPostCollectionAggregation(), matching the shard path
        // (BucketCollectorProcessor#toInternalAggregations).
        //
        // Calling postCollection() or buildAggregations() a second
        // time here would drive DeferableBucketAggregator (breadth_first
        // terms with metric sub-aggregations such as avg / sum / max /
        // terms) through BestBucketsDeferringCollector#prepareSelectedBuckets
        // twice; the second call throws "Already been replayed" (issue #40).
        List<InternalAggregation> results = new ArrayList<>(aggregators.length);
        for (Aggregator agg : aggregators) {
            results.add(agg.getPostCollectionAggregation());
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
     * Open the Lance-backed {@link DirectoryReader} for the caller's
     * fragment subset, wrap it as an
     * {@link OpenSearchDirectoryReader} so downstream code that
     * relies on {@code ShardUtils.extractShardId(reader)} (the
     * security plugin's DLS/FLS wrapper, most importantly) can find
     * the shard id, and run it through the IndexService reader
     * wrapper if one is installed.
     *
     * <p>Lance-backed indexes are single-shard fixed, so the shard
     * id is always {@code indexShard.shardId()} with shard number
     * 0. This matches the shard path (see {@code
     * LanceReadOnlyEngine.openLanceReader}) so wrapper behaviour is
     * consistent across the two paths.
     *
     * <p>{@code IndexService.getReaderWrapper()} is package-private
     * in OpenSearch core; the reflective handle is set up once at
     * class load ({@link #INDEX_SERVICE_GET_READER_WRAPPER}) and the
     * plugin-security.policy already grants {@code
     * ReflectPermission "suppressAccessChecks"}. If no wrapper is
     * installed (empty cluster, no security plugin), the accessor
     * returns {@code null} and the {@link OpenSearchDirectoryReader}
     * wrap is returned as-is (wrapping is still needed for shard id
     * extraction by other code paths, e.g. the search context).
     *
     * <p>The reader contract is that {@code close()} on the
     * returned reader also closes any nested reader, so the caller
     * only needs to close the return value of this method
     * (typically via try-with-resources). Errors during construction
     * clean up the partially-built chain here.
     */
    @SuppressWarnings("unchecked")
    private DirectoryReader openWrappedReader(
        IndexService indexService,
        IndexShard indexShard,
        Dataset readerDataset,
        String pkField,
        org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType pkType,
        java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields,
        List<Integer> effectiveFragmentIds,
        String filterSql
    ) throws IOException {
        DirectoryReader lanceReader = LanceDirectoryReader.openForFragments(
            new ByteBuffersDirectory(),
            null,
            readerDataset,
            pkField,
            pkType,
            multiFields,
            effectiveFragmentIds,
            filterSql
        );
        OpenSearchDirectoryReader wrapped = null;
        try {
            wrapped = OpenSearchDirectoryReader.wrap(lanceReader, indexShard.shardId());
            CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper;
            try {
                wrapper = (CheckedFunction<DirectoryReader, DirectoryReader, IOException>) INDEX_SERVICE_GET_READER_WRAPPER.invoke(
                    indexService
                );
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("cannot access IndexService#getReaderWrapper via reflection", e);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new IllegalStateException("failed to obtain IndexService reader wrapper", cause);
            }
            if (wrapper == null) {
                return wrapped;
            }
            return wrapper.apply(wrapped);
        } catch (Exception e) {
            // Close the outermost reader we successfully built.
            // OpenSearchDirectoryReader.close() closes the inner
            // Lance reader; if wrap itself failed before returning,
            // the inner reader is still ours to close directly.
            DirectoryReader toClose = wrapped != null ? wrapped : lanceReader;
            try {
                toClose.close();
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /**
     * Determine the number of rows in this node's fragment subset
     * that satisfy the query. Uses Lance's metadata-only counting
     * whenever the query is a pure filter shape the coordinator has
     * already translated to Lance SQL
     * ({@link LanceFragmentQueryRequest#filterSql()}):
     * <ul>
     *   <li>No filter: sum {@link org.lance.Fragment#countRows()}
     *       across the assigned fragments (Lance metadata, no
     *       scan).</li>
     *   <li>Filter set, all fragments assigned: use
     *       {@link Dataset#countRows(String)}.</li>
     *   <li>Filter set, subset of fragments: run a bounded scan
     *       over the subset and count matching rows.</li>
     * </ul>
     *
     * <p>For scoring queries (match / knn) the coordinator leaves
     * filterSql null and only ships the {@link QueryBuilder}. We
     * cannot express those in Lance SQL, so we ask Lucene through
     * {@link org.apache.lucene.search.IndexSearcher#count(Query)}.
     * The searcher iterates the same doc set the hits phase does,
     * so this is a second pass in exchange for the exact total
     * (versus underestimating when {@code size} clips).
     */
    private long computeMatched(Dataset dataset, LanceFragmentQueryRequest request, ContextIndexSearcher searcher, Query luceneQuery)
        throws Exception {
        List<Integer> fragmentIds = request.fragmentIdsOrNull();
        String filterSql = request.filterSql();
        boolean hasScoringQuery = request.query() != null && filterSql == null;
        boolean hasPostFilter = request.postFilter() != null;
        if (hasScoringQuery && !hasPostFilter && luceneQuery instanceof LanceFtsQuery fts) {
            // Pure FTS shape (no post_filter, no other scoring
            // clause): count via Lance's inverted-index scan
            // without materialising every match. Lance's FTS scan
            // walks the posting list once and can stream row counts
            // when we do not ask it for row addresses or scores;
            // pylance measures this at 1.5-1.9 ms independent of hit
            // count, versus 4.6 s for the Weight-based path on a
            // 20M-row table with 500k hits (issue #42 perf report).
            return countFtsHitsDirectly(dataset, fts, fragmentIds);
        }
        if (hasScoringQuery || hasPostFilter) {
            // post_filter narrows hits.total.value below what
            // filterSql / countRows would return, so ask Lucene
            // directly against the AND-combined query. knn also
            // lands here (its LanceKnnQuery is not the LanceFtsQuery
            // branch above); Lucene serves the count via the shared
            // shard-level nearest scan the Weight already cached.
            return searcher.count(luceneQuery);
        }
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
     * Count FTS hits without materialising row addresses or scores.
     *
     * <p>Lance's inverted-index scanner can walk the posting list
     * once and stream row counts when we ask for zero columns and
     * no row address / row id. This is the count-only counterpart
     * of {@code Dataset.countRows(sqlFilter)} for scalar filters,
     * and pylance measures it at low milliseconds independent of
     * the hit count. See issue #42 phase A / Step A-1 for the
     * background: without this path, an FTS count went through
     * {@code IndexSearcher.count(luceneQuery)}, which triggered
     * {@link LanceFtsQuery}'s Weight to materialise every match's
     * row address and score into a sparse array (see
     * {@code LanceFtsQuery.scorerSupplier}). The Weight is
     * necessary for the hits phase, but only wastes work for a
     * pure count.
     *
     * <p>The Lance SDK has no {@code Dataset.countRows(FullTextQuery)}
     * overload today, so this method assembles a scan that yields
     * zero payload columns; the batches carry only the row count
     * that the aggregator returns via {@code getRowCount()}. When
     * fragmentIds is null every fragment is included; otherwise
     * Lance filters the scan to the caller's subset (matching the
     * {@code Dataset.countRows(sql)} branch below).
     */
    private long countFtsHitsDirectly(Dataset dataset, LanceFtsQuery fts, List<Integer> fragmentIds) throws Exception {
        org.lance.ipc.ScanOptions.Builder builder = new org.lance.ipc.ScanOptions.Builder().fullTextQuery(fts.fullTextQuery())
            .columns(Collections.emptyList())
            .withRowAddress(false)
            .withRowId(false);
        if (fragmentIds != null) {
            builder = builder.fragmentIds(fragmentIds);
        }
        long total = 0L;
        try (
            org.lance.ipc.LanceScanner scanner = dataset.newScan(builder.build());
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
