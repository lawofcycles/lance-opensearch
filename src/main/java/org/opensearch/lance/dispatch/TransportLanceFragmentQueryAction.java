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
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
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
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.engine.LanceFragmentLeafReader;
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
 * <p>Both hits and aggregations flow through
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
        // Both Dataset opens below use the manifest version the
        // coordinator resolved from index.lance.version so a pinned
        // index serves the same rows through _search as through
        // _count / _stats / GET on the shard engine.
        Optional<Long> pinnedVersion = request.pinnedVersionOrEmpty();
        try (Dataset dataset = LanceRegistry.openDataset(request.tableUri(), request.storageOptions(), pinnedVersion)) {
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

            // Fetch the IndexService reader wrapper once so both
            // openWrappedReader and computeMatched see the same
            // wrapper reference. A non-null wrapper here is the
            // signal that DLS/FLS or a similar reader-level
            // transform may filter documents; computeMatched uses
            // that signal to route counts through the searcher
            // instead of Lance-side metadata paths, which would
            // bypass the wrapper and return the pre-DLS count.
            CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper = resolveReaderWrapper(indexService);
            boolean hasSecurityWrapper = readerWrapper != null;

            // Open a fresh Dataset for the reader: LanceDirectoryReader
            // takes ownership of the Dataset and closes it in doClose.
            // The node-scoped Lance Session cache makes the second
            // open cheap. Any reader wrapper installed on IndexService
            // (most importantly the security plugin's DLS/FLS wrapper)
            // is applied before the searcher is built so document- and
            // field-level filtering apply to fragment path hits the
            // same way they apply to shard path hits.
            try (
                Dataset readerDataset = LanceRegistry.openDataset(request.tableUri(), request.storageOptions(), pinnedVersion);
                DirectoryReader dr = openWrappedReader(
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
                    // `filter + sum` materialise only the matching
                    // rows of the aggregated column. FTS and knn
                    // queries have no SQL representation so filterSql
                    // is null there and the leaf reader runs
                    // unfiltered full-column scans.
                    request.filterSql(),
                    readerWrapper
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
                Query placeholderQuery = MatchAllDocsQuery.INSTANCE;

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
                    //
                    // Sorted scalar-filter pages take the Lance sort
                    // pushdown when every clause translates to a
                    // ColumnOrdering (see resolvePushdownOrderings):
                    // Lance returns the top `size` rows already
                    // ordered, so the hits phase never materialises
                    // the sort column for every matching row. Every
                    // other shape goes through the Lucene collector.
                    List<org.lance.ipc.ColumnOrdering> pushdownOrderings = hasSecurityWrapper || sortAndFormats == null
                        ? null
                        : resolvePushdownOrderings(request, readerDataset.getSchema(), multiFields, sortAndFormats);
                    List<SearchHit> hits;
                    if (pushdownOrderings != null) {
                        hits = scanSortedHitsViaLance(
                            readerDataset,
                            request,
                            pushdownOrderings,
                            sortAndFormats,
                            searcher.getIndexReader(),
                            effectiveFragmentIds
                        );
                    } else {
                        hits = scanHitsViaIndexSearcher(
                            searcher,
                            hitsQuery,
                            sortAndFormats,
                            request.searchAfter(),
                            request.size(),
                            request.trackScores()
                        );
                    }
                    InternalAggregations aggregations = aggregateViaIndexSearcher(request, searchContext, searcher, qsc, query);
                    long matched = computeMatched(dataset, request, searcher, countQuery, hasSecurityWrapper);
                    return new LanceFragmentQueryResponse(matched, fragmentCount, hits, aggregations);
                }
            }
        }
    }

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

    /**
     * Translate the request's OpenSearch-native query representation
     * into a Lucene {@link Query} the shared {@link ContextIndexSearcher}
     * can execute. Priority order:
     * <ol>
     *   <li>{@link LanceFragmentQueryRequest#filterSql()} — the
     *       coordinator translated the whole top-level query tree to
     *       Lance SQL (bool / term / terms / range / exists /
     *       match_all over mapped columns). Wrapped in
     *       {@link LanceScanFilterQuery} so one Lance native scan per
     *       shard evaluates the predicate and yields the matching row
     *       addresses, optionally clipped to {@code size} when the
     *       shape allows (see {@link #resolveScanFilterTopK}). This
     *       takes precedence over the QueryBuilder because the Lucene
     *       translation of the same tree (PointRange / term queries
     *       that fall back to doc values on a reader without points)
     *       has to load every referenced column through the doc value
     *       path before it can match a single row, whereas the Lance
     *       scan reads only {@code _rowaddr}. {@link #computeMatched}
     *       already trusts the same SQL for {@code hits.total}, so the
     *       two stay consistent by construction.</li>
     *   <li>{@link LanceFragmentQueryRequest#query()} — the top-level
     *       {@link org.opensearch.index.query.QueryBuilder} the
     *       coordinator forwarded, for shapes the translator refused
     *       (match / knn / anything scoring, or a tree touching an
     *       unmapped field). Runs through the local
     *       {@link QueryShardContext#toQuery} so per-node mapping
     *       decisions (Lance FTS field types, knn field types, etc.)
     *       apply.</li>
     *   <li>Otherwise: {@link MatchAllDocsQuery}.</li>
     * </ol>
     * The coordinator ships the QueryBuilder on every request and
     * adds filterSql whenever translation succeeds, so both are
     * commonly set at once; filterSql wins because it is the
     * cheaper, already-validated form of the same predicate.
     */
    private Query resolveLuceneQuery(LanceFragmentQueryRequest request, QueryShardContext qsc) throws java.io.IOException {
        if (request.filterSql() != null) {
            return new LanceScanFilterQuery(request.filterSql(), resolveScanFilterTopK(request));
        }
        if (request.query() != null) {
            // Rewrite before toQuery so that shapes which rely on
            // doRewrite to fold themselves away — most notably
            // RangeQueryBuilder against an unmapped field, which
            // rewrites to MatchNone via
            // RangeQueryBuilder.getRelation returning DISJOINT —
            // are resolved before Lucene translation. Without this
            // step RangeQueryBuilder.doToQuery throws
            // IllegalStateException("Rewrite first"), which the
            // OpenSearch error handler surfaces as a 500. The shard
            // path does the same Rewriteable.rewrite call in
            // QueryShardContext.toQuery before invoking doToQuery.
            org.opensearch.index.query.QueryBuilder rewritten = org.opensearch.index.query.Rewriteable.rewrite(request.query(), qsc, true);
            Query base = rewritten.toQuery(qsc);
            // If the request shape allows top-k pushdown and the
            // resulting Lucene tree is a bare LanceFtsQuery (single
            // lance_match / lance_match_phrase / etc. at the root),
            // ship the size hint into it so Lance's FTS scorer can
            // stop after k score-sorted rows. Callers wrapping the
            // FTS clause in a bool / boost / dis_max keep the
            // sentinel: mixing the top-k with other scorers would
            // clip the wrong side.
            int scanLimit = resolveScanFilterTopK(request);
            if (scanLimit != LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED && base instanceof LanceFtsQuery fts) {
                return fts.withScanLimit(scanLimit);
            }
            return base;
        }
        return MatchAllDocsQuery.INSTANCE;
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
        // Same rewrite-before-toQuery pattern as resolveLuceneQuery;
        // post_filter can also carry a range against an unmapped
        // field and would otherwise raise "Rewrite first" from
        // doToQuery.
        org.opensearch.index.query.QueryBuilder rewritten = org.opensearch.index.query.Rewriteable.rewrite(request.postFilter(), qsc, true);
        Query pf = rewritten.toQuery(qsc);
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
     * <p>The score is the real Lucene score (BM25 for Lance FTS,
     * cosine for Lance knn, 1.0 for {@link MatchAllDocsQuery}).
     * Sort clauses go through the
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
        prefetchHitRows(searcher.getIndexReader(), topDocs.scoreDocs);
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
     * Group the page's doc ids by leaf and hand each Lance-backed leaf
     * its slice so the rows behind the hits are fetched in one Lance
     * take per leaf before the per-doc {@code document(...)} loop
     * runs. Without this, {@link LanceFragmentLeafReader#materialiseStoredFields}
     * would fall back to a single-row take per hit ({@code size} JNI
     * round trips instead of one per leaf touched).
     *
     * <p>Leaves that do not unwrap to a {@link LanceFragmentLeafReader}
     * (which should not happen on this path; every leaf the fragment
     * dispatch reader exposes is Lance-backed) are skipped and fall
     * back to the per-doc path.
     */
    private static void prefetchHitRows(org.apache.lucene.index.IndexReader reader, ScoreDoc[] scoreDocs) throws IOException {
        if (scoreDocs.length == 0) {
            return;
        }
        List<org.apache.lucene.index.LeafReaderContext> leaves = reader.leaves();
        java.util.Map<Integer, List<Integer>> docsByLeaf = new java.util.TreeMap<>();
        for (ScoreDoc scoreDoc : scoreDocs) {
            int leafIndex = org.apache.lucene.index.ReaderUtil.subIndex(scoreDoc.doc, leaves);
            int localDoc = scoreDoc.doc - leaves.get(leafIndex).docBase;
            docsByLeaf.computeIfAbsent(leafIndex, k -> new ArrayList<>()).add(localDoc);
        }
        for (java.util.Map.Entry<Integer, List<Integer>> entry : docsByLeaf.entrySet()) {
            LanceFragmentLeafReader lance = LanceFragmentLeafReader.unwrap(leaves.get(entry.getKey()).reader());
            if (lance == null) {
                continue;
            }
            List<Integer> docs = entry.getValue();
            int[] docIds = new int[docs.size()];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = docs.get(i);
            }
            lance.prefetchRows(docIds);
        }
    }

    /**
     * Translate the request's sort clauses into Lance
     * {@link org.lance.ipc.ColumnOrdering}s so the hits phase can run
     * as a single ordered, limited Lance scan instead of a Lucene
     * {@code TopFieldCollector} over every matching row. Returns
     * {@code null} when the request cannot take that path, in which
     * case the caller uses {@link #scanHitsViaIndexSearcher}.
     *
     * <p>The pushdown is only correct when the Lance scan can
     * reproduce exactly what Lucene would return. That holds when all
     * of the following are true.
     * <ul>
     *   <li>{@code size > 0} and no {@code search_after}: the cursor
     *       is value-based and Lance has no equivalent of Lucene's
     *       after-doc tie-break, so paginated pages stay on the
     *       Lucene path.</li>
     *   <li>Scalar shape: the request is match_all, has no query, or
     *       carries a {@link LanceFragmentQueryRequest#filterSql()}
     *       the coordinator translated from the whole query tree. FTS
     *       and knn shapes score, and their order is defined by
     *       Lucene's scorer, not by a column.</li>
     *   <li>No {@code post_filter} and no aggregations: both need the
     *       full match set on the Lucene side, so the limited scan
     *       would not save anything and the aggregation collector
     *       still walks every doc.</li>
     *   <li>Every clause is a plain {@link FieldSortBuilder} on a
     *       column Lance can order by (integers, floats, booleans,
     *       dates, timestamps, Utf8, or a {@code multi_fields} keyword
     *       sub-field routed to its Utf8 base column), with no nested
     *       sort, no {@code mode}, no {@code numeric_type} cast, and a
     *       {@code missing} of {@code _first} / {@code _last} / unset
     *       (a literal missing value has no ColumnOrdering
     *       equivalent).</li>
     * </ul>
     *
     * <p>The caller additionally refuses the pushdown when a reader
     * wrapper is installed, because the Lance-side hits would bypass
     * DLS / FLS the same way a Lance-side count would.
     */
    private static List<org.lance.ipc.ColumnOrdering> resolvePushdownOrderings(
        LanceFragmentQueryRequest request,
        org.apache.arrow.vector.types.pojo.Schema schema,
        java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields,
        org.opensearch.search.sort.SortAndFormats sortAndFormats
    ) {
        if (request.size() <= 0 || request.searchAfter() != null || request.postFilter() != null) {
            return null;
        }
        // The Lucene SortFields OpenSearch built for this request are
        // the authority on how sort values are typed (Integer for
        // byte / short / integer / boolean, Long for long / date,
        // Float / Double for the two float widths, BytesRef for
        // keyword) and on the missing-value sentinel each comparator
        // reports. Only the two plain field-data SortField shapes are
        // reproducible from a Lance scan; anything else (custom
        // comparator source, script, geo distance) stays on Lucene.
        org.apache.lucene.search.SortField[] sortFields = sortAndFormats.sort.getSort();
        if (sortFields.length != request.sorts().size()) {
            return null;
        }
        for (org.apache.lucene.search.SortField sortField : sortFields) {
            if (sortField instanceof org.apache.lucene.search.SortedNumericSortField numeric) {
                switch (numeric.getNumericType()) {
                    case INT, LONG, FLOAT, DOUBLE -> {
                    }
                    default -> {
                        return null;
                    }
                }
            } else if (!(sortField instanceof org.apache.lucene.search.SortedSetSortField)) {
                return null;
            }
        }
        // The coordinator ships the top-level QueryBuilder on every
        // request and additionally sets filterSql when the whole tree
        // translated to Lance SQL. A non-null filterSql therefore
        // means "scalar filter shape"; a null one with a non-match_all
        // query means FTS / knn / untranslatable, which must stay on
        // the Lucene scorer.
        boolean scalarShape = request.filterSql() != null
            || request.query() == null
            || request.query() instanceof org.opensearch.index.query.MatchAllQueryBuilder;
        if (!scalarShape) {
            return null;
        }
        if (request.aggregations() != null && !request.aggregations().getAggregatorFactories().isEmpty()) {
            return null;
        }
        if (request.sorts().isEmpty()) {
            return null;
        }
        List<org.lance.ipc.ColumnOrdering> orderings = new ArrayList<>(request.sorts().size());
        for (org.opensearch.search.sort.SortBuilder<?> sort : request.sorts()) {
            if (!(sort instanceof org.opensearch.search.sort.FieldSortBuilder field)) {
                return null;
            }
            if (field.getNestedSort() != null || field.sortMode() != null || field.getNumericType() != null) {
                return null;
            }
            String name = field.getFieldName();
            if (name.startsWith("_")) {
                // _score, _doc, _id and other metadata fields have no
                // Lance column behind them.
                return null;
            }
            boolean nullFirst;
            Object missing = field.missing();
            if (missing == null || "_last".equals(missing)) {
                nullFirst = false;
            } else if ("_first".equals(missing)) {
                nullFirst = true;
            } else {
                return null;
            }
            String column = resolveSortColumn(name, schema, multiFields);
            if (column == null) {
                return null;
            }
            org.lance.ipc.ColumnOrdering.Builder builder = new org.lance.ipc.ColumnOrdering.Builder();
            builder.setColumnName(column);
            builder.setAscending(field.order() != org.opensearch.search.sort.SortOrder.DESC);
            builder.setNullFirst(nullFirst);
            orderings.add(builder.build());
        }
        return orderings;
    }

    /**
     * Map a sort field name to the Lance column that backs it, or
     * {@code null} when the column does not exist or its Arrow type is
     * not one Lance / DataFusion can order by. A {@code base.sub}
     * name is accepted when {@code multiFields} declares {@code sub}
     * as a keyword sub-field of {@code base}; the ordering then runs
     * on the base Utf8 column, which is exactly what the reader's
     * {@code getSortedDocValues(base.sub)} resolves to.
     */
    private static String resolveSortColumn(
        String name,
        org.apache.arrow.vector.types.pojo.Schema schema,
        java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields
    ) {
        org.apache.arrow.vector.types.pojo.Field field = schema.findField(name);
        if (field == null) {
            int dot = name.lastIndexOf('.');
            if (dot <= 0 || multiFields == null) {
                return null;
            }
            String base = name.substring(0, dot);
            String sub = name.substring(dot + 1);
            java.util.LinkedHashMap<String, String> subs = multiFields.get(base);
            if (subs == null || !"keyword".equals(subs.get(sub))) {
                return null;
            }
            field = schema.findField(base);
            if (field == null || !(field.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8)) {
                return null;
            }
            return base;
        }
        org.apache.arrow.vector.types.pojo.ArrowType type = field.getType();
        if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int
            || type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Bool
            || type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Date
            || type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
            || type instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8) {
            return name;
        }
        if (type instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint fp) {
            return fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE
                || fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE ? name : null;
        }
        return null;
    }

    /**
     * Hits phase for sorted scalar-filter pages: one Lance scan with
     * {@link LanceFragmentQueryRequest#filterSql()},
     * {@code setColumnOrderings}, {@code limit(size)} and the sort
     * columns projected. Lance evaluates the filter and the top-k in
     * one pass (pylance measures {@code filter + order_by + limit 10}
     * on a 10M-row table at tens of milliseconds where the Lucene
     * collector path needed the whole sort column loaded), and the
     * batches come back already in the requested order. Each row's
     * {@code _rowaddr} is decoded to (fragment id, doc id) and routed
     * to the matching leaf, which fetches {@code _id} / {@code _source}
     * through the same {@link LanceFragmentLeafReader#prefetchRows} /
     * {@link LanceFragmentLeafReader#materialiseStoredFields} path the
     * Lucene hits phase uses, so the response shape is identical.
     *
     * <p>Sort values are read from the projected columns and typed the
     * way the request's Lucene {@link org.apache.lucene.search.SortField}s
     * would type them (see {@link #sortValueFrom}) so
     * {@link SearchHit#sortValues} formats them identically and
     * clients can feed them back as {@code search_after}. Arrow nulls
     * become the missing-value object OpenSearch installed on the
     * SortField for the request's {@code missing} / direction
     * combination.
     */
    private List<SearchHit> scanSortedHitsViaLance(
        Dataset dataset,
        LanceFragmentQueryRequest request,
        List<org.lance.ipc.ColumnOrdering> orderings,
        org.opensearch.search.sort.SortAndFormats sortAndFormats,
        org.apache.lucene.index.IndexReader reader,
        List<Integer> fragmentIds
    ) throws IOException {
        org.apache.lucene.search.SortField[] sortFields = sortAndFormats.sort.getSort();
        java.util.Map<Integer, LanceFragmentLeafReader> leafByFragment = new java.util.HashMap<>();
        for (org.apache.lucene.index.LeafReaderContext ctx : reader.leaves()) {
            LanceFragmentLeafReader lance = LanceFragmentLeafReader.unwrap(ctx.reader());
            if (lance != null) {
                leafByFragment.put(lance.fragmentId(), lance);
            }
        }
        List<String> sortColumns = new ArrayList<>();
        for (org.lance.ipc.ColumnOrdering ordering : orderings) {
            if (!sortColumns.contains(ordering.getColumnName())) {
                sortColumns.add(ordering.getColumnName());
            }
        }
        org.lance.ipc.ScanOptions.Builder builder = new org.lance.ipc.ScanOptions.Builder().fragmentIds(fragmentIds)
            .columns(sortColumns)
            .setColumnOrderings(orderings)
            .limit(request.size())
            .withRowAddress(true);
        if (request.filterSql() != null) {
            builder = builder.filter(request.filterSql());
        }
        // Ordered (fragment id, doc id, raw sort values) triples in the
        // order Lance returned them, which is the response order.
        List<long[]> addresses = new ArrayList<>(request.size());
        List<Object[]> sortValues = new ArrayList<>(request.size());
        try (
            org.lance.ipc.LanceScanner scanner = dataset.newScan(builder.build());
            org.apache.arrow.vector.ipc.ArrowReader arrowReader = scanner.scanBatches()
        ) {
            while (arrowReader.loadNextBatch()) {
                org.apache.arrow.vector.VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
                org.apache.arrow.vector.UInt8Vector rowAddr = (org.apache.arrow.vector.UInt8Vector) root.getVector("_rowaddr");
                org.apache.arrow.vector.FieldVector[] vectors = new org.apache.arrow.vector.FieldVector[orderings.size()];
                for (int o = 0; o < vectors.length; o++) {
                    vectors[o] = root.getVector(orderings.get(o).getColumnName());
                }
                for (int i = 0; i < root.getRowCount(); i++) {
                    long addr = rowAddr.get(i);
                    addresses.add(new long[] { addr >>> 32, addr & 0xFFFFFFFFL });
                    Object[] raw = new Object[orderings.size()];
                    for (int o = 0; o < raw.length; o++) {
                        raw[o] = sortValueFrom(vectors[o], i, sortFields[o]);
                    }
                    sortValues.add(raw);
                }
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
        // One take per leaf for the rows behind the page, then render
        // each hit in Lance's order.
        java.util.Map<Integer, List<Integer>> docsByFragment = new java.util.HashMap<>();
        for (long[] address : addresses) {
            docsByFragment.computeIfAbsent((int) address[0], k -> new ArrayList<>()).add((int) address[1]);
        }
        for (java.util.Map.Entry<Integer, List<Integer>> entry : docsByFragment.entrySet()) {
            LanceFragmentLeafReader lance = leafByFragment.get(entry.getKey());
            if (lance == null) {
                continue;
            }
            int[] docIds = new int[entry.getValue().size()];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = entry.getValue().get(i);
            }
            lance.prefetchRows(docIds);
        }
        List<SearchHit> out = new ArrayList<>(addresses.size());
        float score = request.trackScores() ? 1.0f : Float.NaN;
        for (int i = 0; i < addresses.size(); i++) {
            long[] address = addresses.get(i);
            LanceFragmentLeafReader lance = leafByFragment.get((int) address[0]);
            if (lance == null) {
                // The scan was pinned to fragmentIds, which is the same
                // list the reader was opened with, so every address
                // should map to a leaf. Skipping rather than failing
                // keeps a fragment that vanished between the two opens
                // from taking the whole page down.
                continue;
            }
            HitVisitor visitor = new HitVisitor();
            lance.materialiseStoredFields((int) address[1], visitor);
            SearchHit hit = new SearchHit(out.size(), visitor.idString(), Collections.emptyMap(), Collections.emptyMap());
            hit.score(score);
            if (visitor.source != null) {
                hit.sourceRef(new org.opensearch.core.common.bytes.BytesArray(visitor.source));
            }
            hit.sortValues(sortValues.get(i), sortAndFormats.formats);
            out.add(hit);
        }
        return out;
    }

    /**
     * Read one sort value from a projected column the way Lucene's
     * comparator for the matching {@link org.apache.lucene.search.SortField}
     * would report it, so {@link SearchHit#sortValues} formats it
     * identically to the Lucene hits path and clients can feed it
     * back as {@code search_after}. {@link org.apache.lucene.search.SortedSetSortField}
     * (keyword) yields a {@link org.apache.lucene.util.BytesRef};
     * {@link org.apache.lucene.search.SortedNumericSortField} yields
     * {@code Integer} / {@code Long} / {@code Float} / {@code Double}
     * according to its numeric type (OpenSearch maps byte, short,
     * integer and boolean to INT; long, date and unsigned_long to
     * LONG). Integers, booleans, dates and timestamps go through
     * {@link LanceFragmentLeafReader#readAsLong}, which already
     * normalises date / timestamp units to epoch millis. A null cell
     * yields {@code null} for keyword (what {@code TermOrdValComparator}
     * reports for a missing term) and, for numerics, the exact
     * missing-value object OpenSearch installed on the SortField for
     * the request's {@code missing} / order combination.
     */
    private static Object sortValueFrom(org.apache.arrow.vector.FieldVector vector, int i, org.apache.lucene.search.SortField sortField) {
        if (sortField instanceof org.apache.lucene.search.SortedSetSortField) {
            if (vector.isNull(i)) {
                return null;
            }
            return new org.apache.lucene.util.BytesRef(((org.apache.arrow.vector.VarCharVector) vector).get(i));
        }
        if (vector.isNull(i)) {
            return sortField.getMissingValue();
        }
        org.apache.lucene.search.SortedNumericSortField numeric = (org.apache.lucene.search.SortedNumericSortField) sortField;
        return switch (numeric.getNumericType()) {
            case FLOAT -> ((org.apache.arrow.vector.Float4Vector) vector).get(i);
            case DOUBLE -> ((org.apache.arrow.vector.Float8Vector) vector).get(i);
            case INT -> (int) integralValue(vector, i);
            case LONG -> integralValue(vector, i);
            default -> throw new IllegalStateException("unsupported sort field type " + numeric.getNumericType());
        };
    }

    private static long integralValue(org.apache.arrow.vector.FieldVector vector, int i) {
        if (vector instanceof org.apache.arrow.vector.BitVector bits) {
            return bits.get(i);
        }
        return LanceFragmentLeafReader.readAsLong(vector, i);
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
     * Aggregator path. Runs the request's
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
        // twice; the second call throws "Already been replayed".
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
     * Fetch the reader wrapper installed on the {@link IndexService}
     * via the package-private
     * {@code IndexService#getReaderWrapper()} accessor. Returns
     * {@code null} if no wrapper is installed (no security plugin,
     * or the plugin is present but has not yet registered a
     * wrapper). See {@link #INDEX_SERVICE_GET_READER_WRAPPER} for
     * why this goes through reflection.
     *
     * <p>Split out from {@link #openWrappedReader} so the caller in
     * {@link #execute} can inspect whether a wrapper is installed
     * without also opening the reader. This is what lets
     * {@link #computeMatched} route counts through
     * {@link org.apache.lucene.search.IndexSearcher#count(Query)}
     * whenever a wrapper is present, so a DLS/FLS reader wrapper
     * can restrict {@code hits.total.value} the same way it
     * restricts the returned hits.
     */
    @SuppressWarnings("unchecked")
    private CheckedFunction<DirectoryReader, DirectoryReader, IOException> resolveReaderWrapper(IndexService indexService)
        throws IOException {
        try {
            return (CheckedFunction<DirectoryReader, DirectoryReader, IOException>) INDEX_SERVICE_GET_READER_WRAPPER.invoke(indexService);
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
    }

    /**
     * Open the Lance-backed {@link DirectoryReader} for the caller's
     * fragment subset, wrap it as an
     * {@link OpenSearchDirectoryReader} so downstream code that
     * relies on {@code ShardUtils.extractShardId(reader)} (the
     * security plugin's DLS/FLS wrapper, most importantly) can find
     * the shard id, and apply the {@code readerWrapper} the caller
     * fetched from {@link IndexService}.
     *
     * <p>Lance-backed indexes are single-shard fixed, so the shard
     * id is always {@code indexShard.shardId()} with shard number
     * 0. This matches the shard path (see {@code
     * LanceReadOnlyEngine.openLanceReader}) so wrapper behaviour is
     * consistent across the two paths.
     *
     * <p>{@code readerWrapper} is the value returned by
     * {@link #resolveReaderWrapper}. Passing it in rather than
     * resolving it here lets {@link #execute} record whether a
     * wrapper is installed so {@link #computeMatched} can route
     * counts through the searcher whenever a wrapper may restrict
     * the visible document set. A {@code null} wrapper means no
     * wrapper is installed (empty cluster, no security plugin) and
     * the {@link OpenSearchDirectoryReader} is returned as-is —
     * wrapping is still needed for shard id extraction by other
     * code paths (e.g. the search context).
     *
     * <p>The reader contract is that {@code close()} on the
     * returned reader also closes any nested reader, so the caller
     * only needs to close the return value of this method
     * (typically via try-with-resources). Errors during construction
     * clean up the partially-built chain here.
     */
    private DirectoryReader openWrappedReader(
        IndexShard indexShard,
        Dataset readerDataset,
        String pkField,
        org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType pkType,
        java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields,
        List<Integer> effectiveFragmentIds,
        String filterSql,
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper
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
            if (readerWrapper == null) {
                return wrapped;
            }
            return readerWrapper.apply(wrapped);
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
     * Number of live documents across {@code leaves}, read from each
     * leaf's {@link LeafReader#getLiveDocs()} rather than
     * {@link LeafReader#numDocs()}. The two agree for a plain reader;
     * they differ under a DLS wrapper, which is why the caller uses
     * this instead of the {@link MatchAllDocsQuery} count shortcut.
     */
    static long countLiveDocs(List<LeafReaderContext> leaves) {
        long total = 0L;
        for (LeafReaderContext ctx : leaves) {
            LeafReader leaf = ctx.reader();
            Bits liveDocs = leaf.getLiveDocs();
            if (liveDocs == null) {
                total += leaf.numDocs();
            } else if (liveDocs instanceof FixedBitSet bits) {
                total += bits.cardinality();
            } else {
                int maxDoc = leaf.maxDoc();
                for (int doc = 0; doc < maxDoc; doc++) {
                    if (liveDocs.get(doc)) {
                        total++;
                    }
                }
            }
        }
        return total;
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
     *
     * <p>The {@code hasSecurityWrapper} flag overrides every
     * Lance-side fast path. A non-null reader wrapper on
     * {@link IndexService} indicates that DLS/FLS or another
     * reader-level transform may restrict the visible document
     * set; Lance's metadata-only counts and its native filter scan
     * see the raw Dataset, not the wrapper's view, so serving
     * {@code hits.total.value} from Lance would over-count and
     * disagree with the {@code _count} API (which does route
     * through the searcher). Route every count path through
     * {@link org.apache.lucene.search.IndexSearcher#count(Query)}
     * whenever a wrapper is installed so the count matches the
     * hits the same request returns and the {@code _count} API
     * agrees.
     */
    private long computeMatched(
        Dataset dataset,
        LanceFragmentQueryRequest request,
        ContextIndexSearcher searcher,
        Query luceneQuery,
        boolean hasSecurityWrapper
    ) throws Exception {
        if (hasSecurityWrapper) {
            // A reader wrapper is installed on the IndexService,
            // most likely the security plugin's DLS/FLS wrapper.
            // Any Lance-side count would bypass the wrapper and
            // return the pre-wrapper row count, so route every
            // count through the searcher instead. The searcher's
            // BitSet iteration honours the wrapper's liveDocs the
            // same way the hits phase does, keeping
            // hits.total.value consistent with both the returned
            // hits and the _count API for security-restricted
            // users.
            //
            // MatchAllDocsQuery is the one shape IndexSearcher.count
            // does not iterate: its Weight.count returns
            // reader.numDocs(), and the security plugin's DLS leaf
            // reader swaps in filtered liveDocs but leaves numDocs
            // at the unfiltered value. Count that shape from the
            // liveDocs directly so a DLS user's match_all total
            // equals what _count reports. The normalisation mirrors
            // the first two lines of IndexSearcher.count so a
            // ConstantScoreQuery / BoostQuery / bool-filter wrapper
            // around match_all is caught the same way count would
            // unwrap it.
            Query normalised = searcher.rewrite(new ConstantScoreQuery(luceneQuery));
            if (normalised instanceof ConstantScoreQuery csq) {
                normalised = csq.getQuery();
            }
            if (normalised instanceof MatchAllDocsQuery) {
                return countLiveDocs(searcher.getIndexReader().leaves());
            }
            return searcher.count(luceneQuery);
        }
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
            // pylance measures this at low milliseconds independent of
            // hit count, versus seconds for the Weight-based path on a
            // 20M-row table with 500k hits.
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
        //
        // Ask Lance for zero payload columns and no row address /
        // row id: the batches only need to carry a row count that
        // the loop below accumulates via getRowCount(). Without
        // columns(emptyList()) Lance materialises every column of
        // every matching row (including large text / vector fields)
        // just to count them. This mirrors what countFtsHitsDirectly
        // does for the FTS shape.
        org.lance.ipc.ScanOptions options = new org.lance.ipc.ScanOptions.Builder().filter(filterSql)
            .fragmentIds(fragmentIds)
            .columns(Collections.emptyList())
            .withRowAddress(false)
            .withRowId(false)
            .build();
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
     * the hit count. Without this path an FTS count goes through
     * {@code IndexSearcher.count(luceneQuery)}, which triggers
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
}
