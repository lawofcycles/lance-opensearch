/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ipc.ColumnOrdering;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.ResourceAlreadyExistsException;
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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.engine.ColumnStore;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceFragmentLeafReader;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.query.LanceFtsQueryBuilder;
import org.opensearch.lance.query.LanceHintingWeight;
import org.opensearch.lance.query.LanceInvalidInput;
import org.opensearch.lance.query.LanceKnnFilterTranslator;
import org.opensearch.lance.query.LanceKnnQuery;
import org.opensearch.lance.query.LanceScanFilterQuery;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.MultiBucketCollector;
import org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.approximate.ApproximateScoreQuery;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Per-node handler for {@link LanceFragmentQueryAction}. Takes the
 * table snapshot for the request's version from the node's
 * {@link LanceWarmCache} (which opens the Lance dataset through the
 * shared {@link LanceRegistry} the first time) and runs
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

    /**
     * How many times a request re-resolves its {@link IndexService}
     * when the cluster state applier registers or removes the node's
     * instance while the request is between the lookup and the
     * temporary creation.
     */
    private static final int INDEX_SERVICE_RACE_RETRIES = 2;

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
    /**
     * Pool the aggregation pushdown runs its extra fragment group scans
     * on: the SEARCH pool this action itself executes on. No pool is
     * added for it, and the pushdown never blocks on a scan the pool has
     * not started, so a saturated SEARCH pool degrades the pushdown to
     * one scan on the request's own thread instead of parking it.
     */
    private final Executor pushdownExecutor;
    /**
     * Node scoped snapshot and column cache every request acquires its
     * table view from; created by {@code LancePlugin.createComponents}.
     */
    private final LanceWarmCache warmCache;

    @Inject
    public TransportLanceFragmentQueryAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndicesService indicesService,
        BigArrays bigArrays,
        CircuitBreakerService circuitBreakerService,
        LanceWarmCache warmCache
    ) {
        super(LanceFragmentQueryAction.NAME, transportService, actionFilters, LanceFragmentQueryRequest::new, ThreadPool.Names.SEARCH);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.bigArrays = bigArrays;
        this.circuitBreakerService = circuitBreakerService;
        this.warmCache = warmCache;
        int permits = LancePlugin.FRAGMENT_DISPATCH_MAX_CONCURRENT_SETTING.get(clusterService.getSettings());
        this.concurrencyLimit = new java.util.concurrent.Semaphore(permits, /*fair*/ false);
        this.pushdownExecutor = transportService.getThreadPool().executor(ThreadPool.Names.SEARCH);
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
            // Lance's IllegalArgumentException (a phrase query on an
            // index without positions, a malformed predicate) reaches
            // this catch as the cause of the IOException the Lucene
            // Weight contract forces the query classes to throw. Report
            // the IllegalArgumentException itself: it survives the
            // transport layer as its own class, so the coordinator's
            // RemoteTransportException unwraps to it and the client
            // sees 400 illegal_argument_exception with Lance's message
            // instead of 500 i_o_exception. The original exception,
            // with the executor and Lucene frames, goes to the debug
            // log so a plugin bug that surfaces this way stays
            // visible. Everything else stays a server error and is
            // logged as one.
            Exception reported = LanceInvalidInput.unwrap(e);
            if (reported != e) {
                LOGGER.debug(
                    "fragment query for [{}] rejected by Lance as invalid input: {}",
                    request.tableUri(),
                    reported.getMessage(),
                    e
                );
            } else {
                LOGGER.warn(
                    "fragment query failed on this node for [{}] filter [{}] fragments [{}]",
                    request.tableUri(),
                    request.filterSql() == null ? "<match_all>" : request.filterSql(),
                    request.fragmentIds().isEmpty() ? "<all>" : request.fragmentIds(),
                    e
                );
            }
            listener.onFailure(reported);
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
        IndexMetadata indexMetadata = clusterService.state().metadata().index(request.indexName());
        if (indexMetadata == null) {
            throw new IllegalStateException("Fragment path cannot resolve OpenSearch index [" + request.indexName() + "] on this node");
        }
        Index index = indexMetadata.getIndex();
        String pkField = indexMetadata.getSettings().get("index.lance.primary_key_field", "");
        // Parse the type setting through the same fromSetting helper the
        // engine uses so unknown values fall back to LONG. Empty pkField
        // overrides whatever the type says (see the schema derivation
        // for the canonicalisation).
        LancePrimaryKeyType pkType = pkField.isEmpty()
            ? LancePrimaryKeyType.NONE
            : LancePrimaryKeyType.fromSetting(indexMetadata.getSettings().get("index.lance.primary_key_type", "long"));
        // Multi-fields spec is persisted as JSON in a single setting.
        // Empty (no attach-body clause) leaves the reader with an empty
        // sub-field map. Malformed JSON falls through to
        // IllegalArgumentException, which the outer catch turns into a
        // 500 for the caller; that is loud enough to surface a bad
        // index setting without hiding the failure behind an empty map.
        Map<String, LinkedHashMap<String, String>> multiFields = RestAttachAction.deserialiseMultiFields(
            indexMetadata.getSettings().get("index.lance.multi_fields", "")
        );
        // The snapshot is keyed on the manifest version the coordinator
        // enumerated the fragments from (the pinned or tag version, or
        // the latest it observed), so every node of this request reads
        // the same manifest, a pinned index serves the same rows through
        // _search and _count as through _stats / GET on the shard
        // engine, and a warm request makes no Lance call to find its
        // version. Every Lance call of this request (reader leaves,
        // counts, sorted scans) goes through the snapshot's dataset; the
        // lease keeps it open until the reader has been closed.
        try (
            LanceWarmCache.Lease lease = warmCache.acquire(
                indexMetadata.getIndexUUID(),
                request.tableUri(),
                request.storageOptions(),
                request.pinnedVersionOrEmpty(),
                pkField,
                pkType,
                multiFields
            )
        ) {
            LanceWarmCache.Snapshot snapshot = lease.snapshot();
            List<Integer> allFragmentIds = new ArrayList<>(snapshot.fragments().size());
            for (LanceWarmCache.FragmentMeta fragment : snapshot.fragments()) {
                allFragmentIds.add(fragment.id());
            }
            int fragmentCount = request.fragmentIds().isEmpty() ? allFragmentIds.size() : request.fragmentIds().size();
            List<Integer> effectiveFragmentIds = (request.fragmentIdsOrNull() == null || request.fragmentIdsOrNull().isEmpty())
                ? allFragmentIds
                : request.fragmentIdsOrNull();

            // The executor needs an IndexService for the mapping, the
            // QueryShardContext, the bitset cache and the reader
            // wrapper the security plugin installs; see
            // executeWithLocalOrTempIndexService for where it comes from.
            return executeWithLocalOrTempIndexService(
                index,
                indexMetadata,
                snapshot,
                multiFields,
                request,
                fragmentCount,
                effectiveFragmentIds,
                0
            );
        }
    }

    /**
     * Resolve the {@link IndexService} to run against and hand off to
     * {@link #executeWithIndexService}. A node that hosts the shard
     * copy has a registered instance; every other node builds a
     * temporary one from cluster state for the duration of the
     * request through {@link IndicesService#withTempIndexService}.
     * Both go through the plugins' {@code onIndexModule} hooks, so the
     * security plugin's reader wrapper is present in either case.
     *
     * <p>The cluster state applier can register or remove the node's
     * instance while this runs. {@code withTempIndexService} throws
     * {@link ResourceAlreadyExistsException} when a registered
     * instance appeared after the lookup, and that instance can be
     * gone again by the time it is looked up. The method therefore
     * re-enters itself once per such race, up to a small bound, and
     * then gives up with an {@link IllegalStateException} that names
     * the flapping applier; the index is present in cluster state, so
     * an {@code IndexNotFoundException} would misreport it as missing.
     */
    private LanceFragmentQueryResponse executeWithLocalOrTempIndexService(
        Index index,
        IndexMetadata indexMetadata,
        LanceWarmCache.Snapshot snapshot,
        Map<String, LinkedHashMap<String, String>> multiFields,
        LanceFragmentQueryRequest request,
        int fragmentCount,
        List<Integer> effectiveFragmentIds,
        int attempt
    ) throws Exception {
        IndexService localIndexService = indicesService.indexService(index);
        if (localIndexService != null) {
            return executeWithIndexService(
                localIndexService,
                indexMetadata,
                snapshot,
                multiFields,
                request,
                fragmentCount,
                effectiveFragmentIds
            );
        }
        long tempStart = System.nanoTime();
        try {
            return indicesService.withTempIndexService(indexMetadata, tempIndexService -> {
                // withTempIndexService leaves the MapperService
                // empty; apply the cluster state mapping the same
                // way IndicesClusterStateService does for a fresh
                // IndexService.
                tempIndexService.updateMapping(null, indexMetadata);
                LOGGER.debug(
                    "lance.dispatch: temporary IndexService for [{}] ready in {} us",
                    request.indexName(),
                    (System.nanoTime() - tempStart) / 1_000L
                );
                return executeWithIndexService(
                    tempIndexService,
                    indexMetadata,
                    snapshot,
                    multiFields,
                    request,
                    fragmentCount,
                    effectiveFragmentIds
                );
            });
        } catch (ResourceAlreadyExistsException raced) {
            if (attempt >= INDEX_SERVICE_RACE_RETRIES) {
                throw new IllegalStateException(
                    "the cluster state applier on this node kept registering and removing the IndexService for ["
                        + index.getName()
                        + "] while a fragment query tried to resolve it ("
                        + (attempt + 1)
                        + " attempts)",
                    raced
                );
            }
            // The cluster state applier registered a local
            // IndexService between the lookup above and the temp
            // creation. Look it up again; if it has been removed in
            // the meantime the lookup misses and the temp path runs
            // once more.
            return executeWithLocalOrTempIndexService(
                index,
                indexMetadata,
                snapshot,
                multiFields,
                request,
                fragmentCount,
                effectiveFragmentIds,
                attempt + 1
            );
        }
    }

    /**
     * Run the scan against a resolved {@link IndexService}, either
     * the node's own registered instance or a request-scoped temporary
     * one. Nothing below depends on an {@link org.opensearch.index.shard.IndexShard}:
     * the shard id is fixed at 0 (Lance-backed indexes are
     * single-shard) and the {@link IndexSettings} come from the
     * IndexService.
     */
    private LanceFragmentQueryResponse executeWithIndexService(
        IndexService indexService,
        IndexMetadata indexMetadata,
        LanceWarmCache.Snapshot snapshot,
        Map<String, LinkedHashMap<String, String>> multiFields,
        LanceFragmentQueryRequest request,
        int fragmentCount,
        List<Integer> effectiveFragmentIds
    ) throws Exception {
        ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        Dataset dataset = snapshot.dataset();

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

        // The reader's leaves are views over the snapshot: no dataset
        // open, no schema pass. Numeric and boolean columns come from
        // the node's off-heap column store when the snapshot is cached;
        // with the cache disabled the snapshot is request scoped and the
        // columns load into heap for this request as before. Any reader
        // wrapper installed on IndexService (most importantly the
        // security plugin's DLS/FLS wrapper) is applied before the
        // searcher is built so document- and field-level filtering apply
        // to fragment path hits the same way they apply to shard path
        // hits.
        try (
            DirectoryReader dr = openWrappedReader(
                shardId,
                snapshot,
                snapshot.isCached() ? warmCache.columnStore() : null,
                effectiveFragmentIds,
                // Push the coordinator-translated Lance SQL down
                // to the leaf reader. When the top-level query is
                // a scalar filter LanceKnnFilterTranslator can
                // express (bool / term / terms / range / exists /
                // match_all), request.filterSql() carries the SQL
                // and every request scoped heap column scan the
                // leaf reader issues inside ensureXxxLoaded is
                // layered with that filter, so `filter + terms agg`
                // and `filter + sum` materialise only the matching
                // rows of the aggregated column when the column
                // store cannot serve them. FTS and knn queries have
                // no SQL representation so filterSql is null there.
                request.filterSql(),
                readerWrapper
            )
        ) {
            MultiBucketConsumer bucketConsumer = new MultiBucketConsumer(
                Integer.MAX_VALUE,
                circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
            );
            SearchContextAggregations searchContextAggregations = new SearchContextAggregations(AggregatorFactories.EMPTY, bucketConsumer);

            // Placeholder query for the LanceFragmentSearchContext ctor;
            // resolveLuceneQuery() runs after we have the QueryShardContext.
            Query placeholderQuery = MatchAllDocsQuery.INSTANCE;

            try (
                LanceFragmentSearchContext searchContext = new LanceFragmentSearchContext(
                    shardId,
                    indexService.mapperService(),
                    placeholderQuery,
                    searchContextAggregations,
                    bigArrays,
                    indexService.cache().bitsetFilterCache(),
                    clusterService.localNode().getId()
                )
            ) {
                LanceFragmentIndexSearcher searcher = new LanceFragmentIndexSearcher(
                    dr,
                    indexService.getIndexSettings(),
                    searchContext,
                    circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
                );
                searchContext.withSearcher(searcher);
                QueryShardContext qsc = indexService.newQueryShardContext(0, searcher, System::currentTimeMillis, null);
                searchContext.withQueryShardContext(qsc);

                Query query = resolveLuceneQuery(request, qsc, hasSecurityWrapper, indexMetadata);

                // A bare Lance clause at the top level (LanceFtsQuery,
                // possibly a bool collapsed into one with a SQL
                // prefilter, or LanceKnnQuery) gets one Weight for the
                // whole request, created before the sort comparators
                // and aggregators exist. Its shard-level Lance scan then
                // runs once and serves the hits phase, the aggregators
                // and, for FTS through LanceFtsWeight.hitCount, the
                // match count. Letting each phase build its own Weight
                // through the Query API would repeat the scan per phase.
                // Skipped under a reader wrapper: the count has to go
                // through the searcher there so DLS liveDocs apply, and
                // the hint below must not be marked exclusive.
                Weight lanceWeight = null;
                LanceFtsQuery.LanceFtsWeight ftsWeight = null;
                if (!hasSecurityWrapper && (query instanceof LanceFtsQuery || query instanceof LanceKnnQuery)) {
                    lanceWeight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1f);
                    if (lanceWeight instanceof LanceFtsQuery.LanceFtsWeight lanceFtsWeight) {
                        ftsWeight = lanceFtsWeight;
                    }
                    if (lanceWeight instanceof LanceHintingWeight hinting && hintsHelpBeforeScoring(request)) {
                        hintLeavesExclusive(hinting, searcher.getIndexReader().leaves());
                    }
                }

                // With a post_filter the hits and count queries are a
                // conjunction Lucene builds its own clause Weights for;
                // PrebuiltWeightQuery puts the Weight above in the
                // clause's place so the conjunction reuses its scan.
                // Without a post_filter hitsQuery stays the query
                // itself and the phases receive the Weight directly.
                Query prebuilt = lanceWeight == null ? null : new PrebuiltWeightQuery(lanceWeight, ScoreMode.COMPLETE);
                Query hitsQuery = query;
                if (request.postFilter() != null) {
                    hitsQuery = applyPostFilter(prebuilt == null ? query : prebuilt, request, qsc);
                }
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
                if (prebuilt != null && hitsQuery == query && query instanceof LanceKnnQuery) {
                    // A bare knn query is counted through
                    // searcher.count, which would create a second knn
                    // Weight from the query; hand it the prebuilt one.
                    // A bare FTS query keeps its own class here because
                    // computeMatched reads its count from ftsWeight or
                    // from a count-only scan of the LanceFtsQuery.
                    countQuery = prebuilt;
                }
                SortAndFormats sortAndFormats = resolveSort(request, qsc);

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
                List<ColumnOrdering> pushdownOrderings = hasSecurityWrapper || sortAndFormats == null
                    ? null
                    : resolvePushdownOrderings(request, dataset.getSchema(), multiFields, sortAndFormats);
                HitsPage hits;
                if (pushdownOrderings != null) {
                    hits = scanSortedHitsViaLance(
                        dataset,
                        request,
                        pushdownOrderings,
                        sortAndFormats,
                        searcher.getIndexReader(),
                        effectiveFragmentIds
                    );
                } else {
                    // The shared Weight drives the hits phase only when
                    // hitsQuery is the very query it was created for;
                    // with a post_filter the hits query is a conjunction
                    // Lucene has to build its own Weight for.
                    hits = scanHitsViaIndexSearcher(
                        searcher,
                        hitsQuery,
                        hitsQuery == query ? lanceWeight : null,
                        sortAndFormats,
                        request.searchAfter(),
                        request.size(),
                        request.trackScores()
                    );
                }
                InternalAggregations aggregations;
                MatchedCount matched;
                LanceAggregatePushdown.Plan pushdown = resolveAggregatePushdown(request, hasSecurityWrapper, dataset, multiFields, qsc);
                if (pushdown != null) {
                    // The scan groups and aggregates on the Lance side and
                    // also yields the row total, so neither the Lucene
                    // aggregators nor computeMatched run for this request.
                    // The node's fragments are scanned in up to
                    // pushdown_parallelism groups; the extra scans run on
                    // the SEARCH pool this request already executes on.
                    long pushdownStart = System.nanoTime();
                    LanceAggregatePushdown.Result result = pushdown.execute(
                        dataset,
                        effectiveFragmentIds,
                        request.filterSql(),
                        clusterService.getClusterSettings().get(LancePlugin.AGGREGATION_PUSHDOWN_PARALLELISM_SETTING),
                        pushdownExecutor,
                        name -> emptyTopLevelAggregation(request, searchContext, qsc, name)
                    );
                    LOGGER.debug(
                        "lance.dispatch: aggregation pushdown for [{}] over {} rows in {} scans took {} us",
                        request.indexName(),
                        result.totalRows(),
                        result.scans(),
                        (System.nanoTime() - pushdownStart) / 1_000L
                    );
                    aggregations = result.aggregations();
                    matched = request.trackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_DISABLED
                        ? MatchedCount.NOT_TRACKED
                        : MatchedCount.exact(result.totalRows());
                } else {
                    aggregations = aggregateViaIndexSearcher(request, searchContext, searcher, qsc, query, lanceWeight);
                    matched = computeMatched(dataset, request, searcher, countQuery, hasSecurityWrapper, ftsWeight);
                }
                // A size 0 request (the only shape the pushdown takes)
                // has an empty page, so its row address array is empty
                // as well; every other shape ships the addresses of the
                // hits above for the coordinator's tie break.
                return new LanceFragmentQueryResponse(
                    matched.value(),
                    matched.lowerBound(),
                    fragmentCount,
                    hits.hits(),
                    hits.rowAddrs(),
                    aggregations
                );
            }
        }
    }

    /**
     * The hits of one page together with the Lance row address
     * ({@code fragmentId << 32 | offset}) of each, parallel arrays. The
     * addresses travel to the coordinator, which breaks ties between
     * hits with equal sort values on them.
     */
    private record HitsPage(List<SearchHit> hits, long[] rowAddrs) {
        static final HitsPage EMPTY = new HitsPage(Collections.emptyList(), new long[0]);
    }

    /**
     * Row address of a doc of {@code reader}: the fragment id of the
     * Lance leaf the doc belongs to in the high 32 bits, the offset of
     * the doc inside that leaf in the low 32 bits. Every leaf of a
     * fragment dispatch reader is Lance-backed; any other leaf is a
     * bug in the reader construction, not something to paper over.
     */
    private static long rowAddressOf(IndexReader reader, int doc) {
        List<LeafReaderContext> leaves = reader.leaves();
        LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(doc, leaves));
        LanceFragmentLeafReader lance = LanceFragmentLeafReader.unwrap(leaf.reader());
        if (lance == null) {
            throw new IllegalStateException("leaf " + leaf.ord + " of the fragment reader is not backed by a Lance fragment");
        }
        return ((long) lance.fragmentId() << 32) | ((doc - leaf.docBase) & 0xFFFFFFFFL);
    }

    /**
     * Decide whether this request's aggregations run as a Substrait
     * group by inside the Lance scan ({@link LanceAggregatePushdown})
     * and, when they do, encode the plan. The request qualifies when
     * {@code lance.aggregation.pushdown} is on, it asks for no hits
     * ({@code size} 0) and has no {@code post_filter}, its query is
     * {@code match_all} or a scalar filter the coordinator translated to
     * Lance SQL ({@link LanceFragmentQueryRequest#filterSql()}; FTS and
     * knn queries have no SQL form and stay on the aggregator path), no
     * reader wrapper is installed (DLS / FLS filter documents in the
     * Lucene reader, which the scan never sees), and the aggregation tree
     * and its fields pass {@link LanceAggregatePushdown#plan}. Returns
     * {@code null} otherwise.
     */
    private LanceAggregatePushdown.Plan resolveAggregatePushdown(
        LanceFragmentQueryRequest request,
        boolean hasSecurityWrapper,
        Dataset dataset,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc
    ) {
        if (!clusterService.getClusterSettings().get(LancePlugin.AGGREGATION_PUSHDOWN_SETTING)) {
            return null;
        }
        if (request.size() != 0 || hasSecurityWrapper || request.postFilter() != null || request.aggregations() == null) {
            return null;
        }
        boolean scalarQuery = request.query() == null || request.query() instanceof MatchAllQueryBuilder || request.filterSql() != null;
        if (!scalarQuery) {
            return null;
        }
        return LanceAggregatePushdown.plan(request.aggregations(), dataset.getSchema(), multiFields, qsc);
    }

    /**
     * The {@link InternalAggregation} the top level aggregator named
     * {@code name} builds over zero documents. The pushdown uses it as
     * the prototype for {@code date_histogram}, whose result class has
     * no public constructor but a public {@code create(buckets)}: the
     * aggregators are built exactly as {@link #aggregateViaIndexSearcher}
     * builds them and asked for their empty result without collecting
     * anything. They are not released here: {@code createTopLevelAggregators}
     * registers them with the search context, which releases them when it
     * closes, and a second release would drive the request breaker
     * negative.
     */
    private static InternalAggregation emptyTopLevelAggregation(
        LanceFragmentQueryRequest request,
        LanceFragmentSearchContext searchContext,
        QueryShardContext qsc,
        String name
    ) {
        try {
            AggregatorFactories factories = request.aggregations().build(qsc, null);
            List<Aggregator> aggregators = factories.createTopLevelAggregators(searchContext);
            for (Aggregator aggregator : aggregators) {
                if (aggregator.name().equals(name)) {
                    return aggregator.buildEmptyAggregation();
                }
            }
            throw new IllegalStateException("no top level aggregation named [" + name + "]");
        } catch (IOException e) {
            throw new IllegalStateException("cannot build the empty aggregation for [" + name + "]", e);
        }
    }

    /**
     * Result of {@link #computeMatched}: the number of matching rows
     * on this node, and whether counting stopped at the request's
     * {@code trackTotalHitsUpTo} bound so {@code value} is only a
     * lower bound of the true count.
     */
    record MatchedCount(long value, boolean lowerBound) {
        static final MatchedCount NOT_TRACKED = new MatchedCount(0L, false);

        static MatchedCount exact(long value) {
            return new MatchedCount(value, false);
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
     *       apply. A {@code bool} whose only scoring clause is one
     *       Lance FTS clause and whose {@code filter} / {@code
     *       must_not} clauses all translate to Lance SQL is collapsed
     *       into a single {@link LanceFtsQuery} carrying that SQL as
     *       a prefilter (see {@link #resolveFtsPrefilterShape}), so
     *       Lance evaluates the scalar predicate before the
     *       inverted-index lookup instead of Lucene intersecting two
     *       full scans. The collapse is skipped when a reader wrapper
     *       is installed because DLS filters would not be part of the
     *       Lance-side predicate.</li>
     *   <li>Otherwise: {@link MatchAllDocsQuery}.</li>
     * </ol>
     * The coordinator ships the QueryBuilder on every request and
     * adds filterSql whenever translation succeeds, so both are
     * commonly set at once; filterSql wins because it is the
     * cheaper, already-validated form of the same predicate.
     */
    private Query resolveLuceneQuery(
        LanceFragmentQueryRequest request,
        QueryShardContext qsc,
        boolean hasSecurityWrapper,
        IndexMetadata indexMetadata
    ) throws IOException {
        // Top-k pushdown clips the Lance scan to the first `size`
        // rows before the reader wrapper sees them. Under a wrapper
        // (DLS) some of those rows are hidden afterwards and the page
        // would come back short of `size` although more visible rows
        // match, so the scan stays unbounded and the Lucene collector
        // does the clipping after the liveDocs are applied.
        int scanLimit = hasSecurityWrapper ? LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED : resolveScanFilterTopK(request);
        if (request.filterSql() != null) {
            return new LanceScanFilterQuery(request.filterSql(), scanLimit);
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
            QueryBuilder rewritten = Rewriteable.rewrite(request.query(), qsc, true);
            if (!hasSecurityWrapper) {
                FtsPrefilterShape shape = resolveFtsPrefilterShape(
                    rewritten,
                    TransportLanceCoordinatorAction.buildFieldTypeLookup(indexMetadata)
                );
                if (shape != null) {
                    Query pushed = shape.toQuery(qsc, scanLimit);
                    if (pushed != null) {
                        return pushed;
                    }
                }
            }
            Query base = rewritten.toQuery(qsc);
            // If the request shape allows top-k pushdown and the
            // resulting Lucene tree is a bare LanceFtsQuery (single
            // lance_match / lance_match_phrase / etc. at the root),
            // ship the size hint into it so Lance's FTS scorer can
            // stop after k score-sorted rows. Callers wrapping the
            // FTS clause in a bool / boost / dis_max keep the
            // sentinel: mixing the top-k with other scorers would
            // clip the wrong side. LanceFtsQuery has its own
            // unbounded sentinel, so translate before comparing.
            int ftsScanLimit = scanLimit == LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED ? LanceFtsQuery.SCAN_LIMIT_UNBOUNDED : scanLimit;
            if (ftsScanLimit != LanceFtsQuery.SCAN_LIMIT_UNBOUNDED && base instanceof LanceFtsQuery fts) {
                return fts.withScanLimit(ftsScanLimit);
            }
            return base;
        }
        return MatchAllDocsQuery.INSTANCE;
    }

    /**
     * A {@code bool} query whose scoring part is exactly one Lance FTS
     * clause and whose remaining clauses are scalar predicates Lance
     * can evaluate as an FTS prefilter.
     *
     * @param ftsClause the single {@code must} clause; implements
     *     {@link LanceFtsQueryBuilder} and is also a
     *     {@link QueryBuilder}
     * @param prefilterSql Lance SQL for {@code filter} AND NOT
     *     {@code must_not}, produced by
     *     {@link LanceKnnFilterTranslator#toLanceSql(QueryBuilder, Function)}
     */
    record FtsPrefilterShape(QueryBuilder ftsClause, String prefilterSql) {

        /**
         * Build the Lucene query for the collapsed shape: the FTS
         * clause's own {@link LanceFtsQuery} with {@code prefilterSql}
         * attached. The clause's {@code boost} survives as the
         * {@link BoostQuery} wrapper {@code AbstractQueryBuilder.toQuery}
         * adds, and the scan limit is applied only to the bare
         * {@link LanceFtsQuery} form, matching what
         * {@code resolveLuceneQuery} does for a top-level FTS clause.
         * Returns {@code null} when the clause produced something else,
         * in which case the caller falls back to the plain Lucene
         * tree.
         */
        Query toQuery(QueryShardContext qsc, int scanLimit) throws IOException {
            Query clause = ftsClause.toQuery(qsc);
            if (clause instanceof LanceFtsQuery fts) {
                LanceFtsQuery pushed = fts.withPrefilterSql(prefilterSql);
                return scanLimit == LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED ? pushed : pushed.withScanLimit(scanLimit);
            }
            if (clause instanceof BoostQuery boosted && boosted.getQuery() instanceof LanceFtsQuery fts) {
                return new BoostQuery(fts.withPrefilterSql(prefilterSql), boosted.getBoost());
            }
            return null;
        }
    }

    /**
     * Decide whether {@code query} is a {@code bool} whose scalar
     * clauses can be pushed into the Lance FTS scan as a prefilter,
     * and produce the SQL if so. Returns {@code null} for every other
     * shape, which the caller then translates to the ordinary Lucene
     * tree with no change in behaviour.
     *
     * <p>Accepted shape, checked on the rewritten builder:
     * <ul>
     *   <li>{@link BoolQueryBuilder} with boost {@code 1.0} and no
     *       {@code minimum_should_match}; a bool boost would scale
     *       the FTS scores and is left to Lucene.</li>
     *   <li>Exactly one {@code must} clause and it implements
     *       {@link LanceFtsQueryBuilder} ({@code lance_match},
     *       {@code lance_match_phrase}, {@code lance_multi_match},
     *       {@code lance_fts_bool}, {@code lance_fts_boost}).</li>
     *   <li>No {@code should} clause: OR semantics with the FTS
     *       clause cannot be expressed as a prefilter.</li>
     *   <li>At least one {@code filter} or {@code must_not} clause,
     *       and every one of them translates through
     *       {@link LanceKnnFilterTranslator#toLanceSql(QueryBuilder, Function)}
     *       without naming a field {@code fieldTypeLookup} reports as
     *       unmapped. A {@code match} or a dotted multi-field path in
     *       any of them keeps the whole bool on Lucene.</li>
     * </ul>
     * The scalar clauses are wrapped in a synthetic bool ({@code filter}
     * and {@code must_not} only) and translated as one expression, so
     * the SQL is the translator's own {@code (f1 AND f2 AND NOT (m1))}
     * form. The rewritten bool is used rather than the raw request so
     * that a clause the rewrite folded to match_none has already
     * turned the whole bool into a non-bool builder.
     */
    static FtsPrefilterShape resolveFtsPrefilterShape(QueryBuilder query, Function<String, String> fieldTypeLookup) {
        if (!(query instanceof BoolQueryBuilder bool)) {
            return null;
        }
        if (bool.boost() != AbstractQueryBuilder.DEFAULT_BOOST || bool.minimumShouldMatch() != null) {
            return null;
        }
        if (bool.must().size() != 1 || !bool.should().isEmpty()) {
            return null;
        }
        QueryBuilder must = bool.must().get(0);
        if (!(must instanceof LanceFtsQueryBuilder)) {
            return null;
        }
        if (bool.filter().isEmpty() && bool.mustNot().isEmpty()) {
            return null;
        }
        BoolQueryBuilder scalar = new BoolQueryBuilder();
        for (QueryBuilder clause : bool.filter()) {
            scalar.filter(clause);
        }
        for (QueryBuilder clause : bool.mustNot()) {
            scalar.mustNot(clause);
        }
        if (LanceKnnFilterTranslator.hasUnmappedField(scalar, fieldTypeLookup)) {
            return null;
        }
        try {
            return new FtsPrefilterShape(must, LanceKnnFilterTranslator.toLanceSql(scalar, fieldTypeLookup));
        } catch (IllegalArgumentException outsideTranslator) {
            return null;
        }
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
     * The caller additionally keeps the scan unbounded when a reader
     * wrapper is installed: the wrapper's liveDocs narrow the result
     * after the scan the same way a post_filter would.
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
     * Whether the request reads doc values before Lucene has scored a
     * single doc, so that a hit set delivered to the fragment readers
     * ahead of collection changes what those reads load.
     *
     * <p>Two consumers read keyword ordinals that early: OpenSearch's
     * {@code TermsAggregatorFactory} builds the global ordinal map
     * from every leaf's {@code SortedSetDocValues} while the
     * aggregators are created, and {@code BytesRefFieldComparatorSource}
     * reads the value count while the leaf comparator is created.
     * Without a hint at that point both load the full dictionary of
     * every fragment; with an exclusive hint they build it from the
     * hit rows. So the early hint pays off when the request carries
     * aggregations, or a sort that a hits page will actually use.
     *
     * <p>Every other shape is left alone on purpose. A page clipped to
     * {@code size} ({@link #resolveScanFilterTopK} returned a bound)
     * carries the top {@code size} rows of the scan, which is a
     * different set from the rows a sort or aggregation would visit,
     * and nothing reads doc values before scoring there anyway. A
     * count-only request ({@code size: 0} without aggregations, with
     * or without post_filter) reads no doc values at all, and running
     * the materialising scan for it would replace the cheaper bounded
     * count scan {@link #computeMatched} uses.
     */
    private boolean hintsHelpBeforeScoring(LanceFragmentQueryRequest request) {
        if (resolveScanFilterTopK(request) != LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED) {
            return false;
        }
        boolean hasAggregations = request.aggregations() != null && !request.aggregations().getAggregatorFactories().isEmpty();
        boolean sortsAPage = request.size() > 0 && !request.sorts().isEmpty();
        return hasAggregations || sortsAPage;
    }

    /**
     * Deliver the Weight's per-leaf hit set to every fragment reader as
     * an exclusive hint (see {@link LanceHintingWeight#hintExclusive}).
     * Called only when the top-level query is the bare Lance clause the
     * Weight belongs to and no reader wrapper is installed, which is
     * the executor's proof that nothing but this Weight's hits will be
     * collected on any leaf: the aggregators run over that query alone,
     * and the hits phase runs it either alone or as the required clause
     * of a conjunction with {@code post_filter}, which can only drop
     * docs from the hit set. The first call runs the Lance scan; the
     * hits and aggregation phases then reuse it through the same
     * Weight.
     */
    private static void hintLeavesExclusive(LanceHintingWeight weight, List<LeafReaderContext> leaves) throws IOException {
        for (LeafReaderContext ctx : leaves) {
            weight.hintExclusive(ctx);
        }
    }

    /**
     * Translate the request's sort clauses (native
     * {@link org.opensearch.search.sort.SortBuilder} shape) into an
     * OpenSearch {@link SortAndFormats}
     * pair the shared searcher can pass to
     * {@link org.apache.lucene.search.IndexSearcher#search(Query, int, org.apache.lucene.search.Sort)}.
     * Returns {@code null} when the request has no sort — the
     * searcher then orders by score.
     */
    private SortAndFormats resolveSort(LanceFragmentQueryRequest request, QueryShardContext qsc) throws java.io.IOException {
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
     *
     * <p>When {@code sharedWeight} is non-null the collectors run
     * through {@link LanceFragmentIndexSearcher#search(Weight, org.apache.lucene.search.CollectorManager)}
     * with that Weight instead of letting {@link org.apache.lucene.search.IndexSearcher}
     * create one from {@code query}; the collector managers, the
     * {@code numHits} cap and the total-hits threshold are the ones
     * the stock {@code search} / {@code searchAfter} overloads build
     * internally, so the returned page is the same either way. The
     * caller passes a Weight only for a bare {@link LanceFtsQuery} or
     * {@link LanceKnnQuery} so that its Lance scan is shared with the
     * aggregators and, for FTS, the match count.
     */
    private HitsPage scanHitsViaIndexSearcher(
        LanceFragmentIndexSearcher searcher,
        Query query,
        Weight sharedWeight,
        SortAndFormats sortAndFormats,
        Object[] searchAfter,
        int size,
        boolean trackScores
    ) throws java.io.IOException {
        if (size <= 0) {
            return HitsPage.EMPTY;
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
            FieldDoc after = new FieldDoc(afterDoc, 0f, searchAfter);
            topDocs = sharedWeight == null
                ? searcher.searchAfter(after, query, size, sortAndFormats.sort, trackScores)
                : searchSortedWithWeight(searcher, sharedWeight, after, size, sortAndFormats.sort, trackScores);
        } else if (sortAndFormats == null) {
            topDocs = sharedWeight == null
                ? searcher.search(query, size)
                : searcher.search(sharedWeight, new TopScoreDocCollectorManager(cappedNumHits(searcher, size), null, TOTAL_HITS_THRESHOLD));
        } else {
            topDocs = sharedWeight == null
                ? searcher.search(query, size, sortAndFormats.sort, trackScores)
                : searchSortedWithWeight(searcher, sharedWeight, null, size, sortAndFormats.sort, trackScores);
        }
        prefetchHitRows(searcher.getIndexReader(), topDocs.scoreDocs);
        List<SearchHit> out = new ArrayList<>(topDocs.scoreDocs.length);
        long[] rowAddrs = new long[topDocs.scoreDocs.length];
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[i];
            HitVisitor visitor = new HitVisitor();
            searcher.storedFields().document(scoreDoc.doc, visitor);
            SearchHit hit = new SearchHit(i, visitor.idString(), Collections.emptyMap(), Collections.emptyMap());
            hit.score(scoreDoc.score);
            if (visitor.source != null) {
                hit.sourceRef(new org.opensearch.core.common.bytes.BytesArray(visitor.source));
            }
            if (sortAndFormats != null && scoreDoc instanceof FieldDoc fieldDoc) {
                hit.sortValues(fieldDoc.fields, sortAndFormats.formats);
            }
            out.add(hit);
            rowAddrs[i] = rowAddressOf(searcher.getIndexReader(), scoreDoc.doc);
        }
        return new HitsPage(out, rowAddrs);
    }

    /**
     * Total-hits threshold {@link org.apache.lucene.search.IndexSearcher}
     * hands its own top-docs collector managers ({@code
     * IndexSearcher.TOTAL_HITS_THRESHOLD}, which is private there).
     * Only the {@code TopDocs.totalHits} accounting depends on it;
     * this class reads {@code hits.total} from {@link #computeMatched}
     * and never from the collector, so the value just keeps the
     * Weight-driven page identical to the Query-driven one.
     */
    private static final int TOTAL_HITS_THRESHOLD = 1000;

    /**
     * {@code numHits} cap {@link org.apache.lucene.search.IndexSearcher#searchAfter}
     * applies before building a collector: a top-docs collector
     * rejects {@code numHits > maxDoc} and {@code numHits < 1}, so the
     * result is clamped to {@code [1, max(1, maxDoc)]} whatever
     * {@code size} is.
     */
    private static int cappedNumHits(LanceFragmentIndexSearcher searcher, int size) {
        return Math.max(1, Math.min(size, Math.max(1, searcher.getIndexReader().maxDoc())));
    }

    /**
     * Sorted top-{@code size} page driven by a caller-built
     * {@link Weight}: the same steps as
     * {@link org.apache.lucene.search.IndexSearcher#searchAfter(ScoreDoc, Query, int, org.apache.lucene.search.Sort, boolean)}
     * ({@code Sort.rewrite}, {@link TopFieldCollectorManager} with the
     * stock threshold, score population when {@code trackScores})
     * with the Weight substituted for the Query.
     */
    private static TopFieldDocs searchSortedWithWeight(
        LanceFragmentIndexSearcher searcher,
        Weight weight,
        FieldDoc after,
        int size,
        Sort sort,
        boolean trackScores
    ) throws IOException {
        Sort rewrittenSort = sort.rewrite(searcher);
        TopFieldCollectorManager manager = new TopFieldCollectorManager(
            rewrittenSort,
            cappedNumHits(searcher, size),
            after,
            TOTAL_HITS_THRESHOLD
        );
        TopFieldDocs topDocs = searcher.search(weight, manager);
        if (trackScores) {
            populateScores(topDocs.scoreDocs, searcher, weight);
        }
        return topDocs;
    }

    /**
     * Fill {@link ScoreDoc#score} of a sorted page from {@code weight},
     * the way {@link TopFieldCollector#populateScores(ScoreDoc[], org.apache.lucene.search.IndexSearcher, Query)}
     * does, except that the caller's Weight is used instead of a new
     * one created from the Query (which for a Lance-backed query
     * would run the native scan again). Docs are visited in doc id
     * order so each leaf's scorer is obtained once.
     */
    private static void populateScores(ScoreDoc[] scoreDocs, LanceFragmentIndexSearcher searcher, Weight weight) throws IOException {
        ScoreDoc[] byDoc = scoreDocs.clone();
        Arrays.sort(byDoc, Comparator.comparingInt(scoreDoc -> scoreDoc.doc));
        List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
        LeafReaderContext current = null;
        Scorer scorer = null;
        for (ScoreDoc scoreDoc : byDoc) {
            if (current == null || scoreDoc.doc >= current.docBase + current.reader().maxDoc()) {
                current = leaves.get(ReaderUtil.subIndex(scoreDoc.doc, leaves));
                ScorerSupplier supplier = weight.scorerSupplier(current);
                if (supplier == null) {
                    throw new IllegalStateException("Doc id " + scoreDoc.doc + " does not match the query");
                }
                scorer = supplier.get(1L);
            }
            int leafDoc = scoreDoc.doc - current.docBase;
            if (scorer.iterator().advance(leafDoc) != leafDoc) {
                throw new IllegalStateException("Doc id " + scoreDoc.doc + " does not match the query");
            }
            scoreDoc.score = scorer.score();
        }
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
     * {@link ColumnOrdering}s so the hits phase can run
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
    private static List<ColumnOrdering> resolvePushdownOrderings(
        LanceFragmentQueryRequest request,
        org.apache.arrow.vector.types.pojo.Schema schema,
        java.util.Map<String, java.util.LinkedHashMap<String, String>> multiFields,
        SortAndFormats sortAndFormats
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
        List<ColumnOrdering> orderings = new ArrayList<>(request.sorts().size());
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
            ColumnOrdering.Builder builder = new ColumnOrdering.Builder();
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
    private HitsPage scanSortedHitsViaLance(
        Dataset dataset,
        LanceFragmentQueryRequest request,
        List<ColumnOrdering> orderings,
        SortAndFormats sortAndFormats,
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
        for (ColumnOrdering ordering : orderings) {
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
        long[] rowAddrs = new long[addresses.size()];
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
            rowAddrs[out.size()] = (address[0] << 32) | address[1];
            out.add(hit);
        }
        return new HitsPage(out, out.size() == rowAddrs.length ? rowAddrs : Arrays.copyOf(rowAddrs, out.size()));
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
     *
     * <p>{@code sharedWeight}, when non-null, is the Weight the caller
     * already built for {@code query}; the collector tree is then
     * driven through it so a Lance-backed query's native scan is
     * reused rather than repeated. The aggregators' collector is
     * satisfied by a {@link ScoreMode#COMPLETE} Weight the same way it
     * is by the no-scores Weight the searcher would otherwise build:
     * it simply does not call {@code score()}.
     */
    private InternalAggregations aggregateViaIndexSearcher(
        LanceFragmentQueryRequest request,
        LanceFragmentSearchContext searchContext,
        LanceFragmentIndexSearcher searcher,
        QueryShardContext qsc,
        Query query,
        Weight sharedWeight
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
        if (sharedWeight != null) {
            searcher.search(sharedWeight, wrapped);
        } else {
            searcher.search(query, wrapped);
        }

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
     * {@link #computeMatched} count from the wrapped reader's
     * liveDocs whenever a wrapper is present, so a DLS/FLS reader
     * wrapper can restrict {@code hits.total.value} the same way it
     * restricts the returned hits.
     */
    @SuppressWarnings("unchecked")
    static CheckedFunction<DirectoryReader, DirectoryReader, IOException> resolveReaderWrapper(IndexService indexService)
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
     * <p>Lance-backed indexes are single-shard fixed, so
     * {@code shardId} is always shard number 0 of the index. This
     * matches the shard path (see {@code
     * LanceReadOnlyEngine.openLanceReader}) so wrapper behaviour is
     * consistent across the two paths, and it needs no local shard
     * copy.
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
        ShardId shardId,
        LanceWarmCache.Snapshot snapshot,
        ColumnStore columnStore,
        List<Integer> effectiveFragmentIds,
        String filterSql,
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper
    ) throws IOException {
        DirectoryReader lanceReader = LanceDirectoryReader.openForSnapshot(
            new ByteBuffersDirectory(),
            snapshot,
            columnStore,
            effectiveFragmentIds,
            filterSql,
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
        );
        OpenSearchDirectoryReader wrapped = null;
        try {
            wrapped = OpenSearchDirectoryReader.wrap(lanceReader, shardId);
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
     * Number of documents {@code query} matches on {@code searcher},
     * counted by collecting every doc its scorer yields. The searcher
     * hands the scorer each leaf's {@link LeafReader#getLiveDocs()} as
     * accepted docs, so a reader wrapper's filtered liveDocs apply.
     *
     * <p>{@link org.apache.lucene.search.IndexSearcher#count(Query)} is
     * not used because its {@code TotalHitCountCollector} asks
     * {@link Weight#count(LeafReaderContext)} first and takes that
     * answer without scoring: {@link MatchAllDocsQuery} answers
     * {@link LeafReader#numDocs()}, which the security plugin's DLS
     * leaf reader leaves at the unfiltered value, and other Weights
     * answer from index statistics that predate the wrapper as well.
     * The collector below never consults {@code Weight.count}, so the
     * only thing that decides the count is the scorer intersected with
     * the liveDocs.
     *
     * <p>The counter is a plain {@code long[]}: {@link LanceFragmentIndexSearcher}
     * is built with a null executor, so its slice loop visits every
     * leaf on the calling thread and the collector is never shared
     * across threads. Handing the searcher an executor would require
     * an atomic counter (or a {@link org.apache.lucene.search.CollectorManager})
     * here.
     */
    static long countThroughLiveDocs(LanceFragmentIndexSearcher searcher, Query query) throws IOException {
        long[] total = new long[1];
        searcher.search(query, new SimpleCollector() {
            @Override
            public void collect(int doc) {
                total[0]++;
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }
        });
        return total[0];
    }

    /**
     * Determine the number of rows in this node's fragment subset
     * that satisfy the query, counted as far as the request's
     * {@link LanceFragmentQueryRequest#trackTotalHitsUpTo()} asks.
     * Uses Lance's metadata-only counting whenever the query is a
     * pure filter shape the coordinator has already translated to
     * Lance SQL ({@link LanceFragmentQueryRequest#filterSql()}):
     * <ul>
     *   <li>No filter: sum {@link org.lance.Fragment#countRows()}
     *       across the assigned fragments (Lance metadata, no
     *       scan).</li>
     *   <li>Filter set, all fragments assigned: use
     *       {@link Dataset#countRows(String)}.</li>
     *   <li>Filter set, subset of fragments: run a bounded scan
     *       over the subset and count matching rows.</li>
     * </ul>
     * These counts come from Lance metadata or a scan that reads no
     * payload columns, so they are cheap regardless of the match
     * count and are always reported exact; the {@code track_total_hits}
     * bound only affects how the coordinator presents them.
     *
     * <p>A bare {@link LanceFtsQuery} is counted from the Weight the
     * caller built for the request when that Weight's scan has run
     * (hits or aggregations were collected) and either was unbounded
     * or returned fewer rows than its {@code scanLimit}, since then
     * the scan saw every match. Otherwise the count comes from a
     * dedicated Lance scan that yields no payload columns
     * ({@link #countFtsHitsDirectly}), limited to
     * {@code trackTotalHitsUpTo + 1} rows unless the request asked for
     * an accurate total: reaching the limit proves there are more than
     * {@code trackTotalHitsUpTo} matches, which is all the
     * {@code gte} relation needs, and stops the scan from walking a
     * posting list whose length is what makes large FTS results slow.
     *
     * <p>For every other scoring shape (knn, a bool mixing FTS with
     * other scoring clauses, post_filter over any query) the
     * coordinator leaves filterSql null and only ships the
     * {@link QueryBuilder}. We cannot express those in Lance SQL, so
     * we ask Lucene through
     * {@link org.apache.lucene.search.IndexSearcher#count(Query)}
     * and report the exact number.
     *
     * <p>The {@code hasSecurityWrapper} flag overrides every
     * Lance-side fast path. A non-null reader wrapper on
     * {@link IndexService} indicates that DLS/FLS or another
     * reader-level transform may restrict the visible document
     * set; Lance's metadata-only counts and its native filter scan
     * see the raw Dataset, not the wrapper's view, so serving
     * {@code hits.total.value} from Lance would over-count and
     * disagree with the hits the same request returns. Whenever a
     * wrapper is installed the count is taken from the wrapped
     * leaves' {@link LeafReader#getLiveDocs()} instead, either
     * directly ({@link #countLiveDocs}, for {@code match_all}) or by
     * collecting the query's scorer under those liveDocs
     * ({@link #countThroughLiveDocs}), so the count matches the hits,
     * and {@code _count} (which takes this same path) agrees with
     * {@code _search}.
     */
    private MatchedCount computeMatched(
        Dataset dataset,
        LanceFragmentQueryRequest request,
        LanceFragmentIndexSearcher searcher,
        Query luceneQuery,
        boolean hasSecurityWrapper,
        LanceFtsQuery.LanceFtsWeight ftsWeight
    ) throws Exception {
        int upTo = request.trackTotalHitsUpTo();
        if (upTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
            // track_total_hits: false. The coordinator leaves
            // hits.total out of the response, so no count is needed.
            return MatchedCount.NOT_TRACKED;
        }
        if (hasSecurityWrapper) {
            // A reader wrapper is installed on the IndexService,
            // most likely the security plugin's DLS/FLS wrapper.
            // Invariant of this branch: the count is derived from
            // the wrapped leaves' getLiveDocs(), either read directly
            // (countLiveDocs) or applied by the searcher while it
            // drives a scorer (countThroughLiveDocs). Nothing here
            // may read Lance metadata (Dataset.countRows,
            // Fragment.countRows), run a Lance count scan, or use a
            // Lucene shortcut that answers from LeafReader.numDocs():
            // all of those see the rows before the wrapper and would
            // report hidden rows in hits.total.value while the hits
            // themselves are filtered.
            //
            // The track_total_hits bound is not applied here. Both
            // paths below count every match, so the value is exact,
            // and exact is within the contract for any bound. For an
            // FTS query the Lance scan runs a second time here (the
            // hits phase's LanceFtsWeight and its shardHits are not
            // reused). That repeat is accepted: it is the only count
            // path that sees the wrapper's view, and DLS correctness
            // outranks the saving.
            //
            // match_all is counted from the liveDocs bitset. The
            // security plugin's DLS leaf reader swaps in filtered
            // liveDocs but leaves numDocs() at the unfiltered value,
            // and numDocs() is exactly what MatchAllDocsQuery's
            // Weight.count answers, so IndexSearcher.count must not
            // be used for it. MatchAllQueryBuilder produces an
            // ApproximateScoreQuery around the MatchAllDocsQuery;
            // once the hits phase has run, ContextIndexSearcher.rewrite
            // has called setContext on that instance and its rewrite
            // returns itself instead of the wrapped query, so unwrap
            // it explicitly before the ConstantScoreQuery
            // normalisation (which mirrors the first two lines of
            // IndexSearcher.count and catches a constant_score /
            // boost / bool-filter wrapper around match_all).
            Query normalised = luceneQuery instanceof ApproximateScoreQuery approximate ? approximate.getOriginalQuery() : luceneQuery;
            normalised = searcher.rewrite(new ConstantScoreQuery(normalised));
            if (normalised instanceof ConstantScoreQuery csq) {
                normalised = csq.getQuery();
            }
            if (normalised instanceof MatchAllDocsQuery) {
                return MatchedCount.exact(countLiveDocs(searcher.getIndexReader().leaves()));
            }
            return MatchedCount.exact(countThroughLiveDocs(searcher, luceneQuery));
        }
        List<Integer> fragmentIds = request.fragmentIdsOrNull();
        String filterSql = request.filterSql();
        boolean hasScoringQuery = request.query() != null && filterSql == null;
        boolean hasPostFilter = request.postFilter() != null;
        if (hasScoringQuery && !hasPostFilter && luceneQuery instanceof LanceFtsQuery fts) {
            // Pure FTS shape (no post_filter, no other scoring
            // clause). A collapsed bool query arrives here as the
            // same LanceFtsQuery carrying its scalar clauses as
            // prefilterSql, which every count path below applies
            // too.
            if (ftsWeight != null) {
                // The request's own Weight has scanned already when
                // hits or aggregations were collected. Its count is
                // the true total when the scan saw every match of
                // this executor's fragments: an unbounded scan, or a
                // bounded scan that came back short of its limit
                // before the fragment filter (Lance returns exactly
                // min(limit, matches) rows).
                long scanned = ftsWeight.hitCount();
                if (scanned >= 0 && ftsWeight.complete()) {
                    return MatchedCount.exact(scanned);
                }
            }
            if (upTo == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
                return MatchedCount.exact(countFtsHitsDirectly(dataset, fts, fragmentIds, 0L).own());
            }
            long limit = (long) upTo + 1L;
            FtsHitCount counted = countFtsHitsDirectly(dataset, fts, fragmentIds, limit);
            // The bound is judged on the rows Lance returned before the
            // fragment filter. On a subset executor the own share of a
            // filled scan is a fraction of the limit and says nothing
            // about how many matches were cut off; the scan filling up
            // does, and it fills up on every executor at once, so each
            // reports a lower bound and the coordinator answers gte.
            return new MatchedCount(counted.own(), counted.scanned() >= limit);
        }
        if (hasScoringQuery || hasPostFilter) {
            // post_filter narrows hits.total.value below what
            // filterSql / countRows would return, so ask Lucene
            // directly against the AND-combined query. knn also
            // lands here (its LanceKnnQuery is not the LanceFtsQuery
            // branch above); Lucene serves the count via the shared
            // shard-level nearest scan the Weight already cached.
            return MatchedCount.exact(searcher.count(luceneQuery));
        }
        if (filterSql == null) {
            if (fragmentIds == null) {
                return MatchedCount.exact(dataset.countRows());
            }
            long total = 0L;
            List<Fragment> allFragments = dataset.getFragments();
            for (Fragment fragment : allFragments) {
                if (fragmentIds.contains(fragment.getId())) {
                    total += fragment.countRows();
                }
            }
            return MatchedCount.exact(total);
        }
        if (fragmentIds == null) {
            return MatchedCount.exact(dataset.countRows(filterSql));
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
        return MatchedCount.exact(total);
    }

    /**
     * Count FTS hits without materialising scores or payload columns.
     *
     * <p>Lance's inverted-index scanner can walk the posting list
     * once and stream row counts when we ask for zero columns and
     * no row address / row id. This is the count-only counterpart
     * of {@code Dataset.countRows(sqlFilter)} for scalar filters.
     * Without this path an FTS count goes through
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
     * {@code fragmentIds} is null or covers every fragment of the
     * dataset that is the whole scan, and Lance answers it from the
     * inverted index alone.
     *
     * <p>A proper subset (one executor of a multi node fan out) is
     * not passed to Lance either, because a fragment list makes Lance
     * read {@code _rowid} over the listed fragments as a prefilter
     * (see {@link LanceFtsQuery#restrictToFragmentsUnlessAll}).
     * Instead the scan runs over the whole table with
     * {@code withRowAddress(true)} and the rows whose fragment id
     * (upper 32 bits of {@code _rowaddr}) is in {@code fragmentIds}
     * are counted here, next to the number of rows Lance returned
     * before that filter:
     * <ul>
     *   <li>{@code limit > 0} (the {@code track_total_hits} bound,
     *       passed as {@code upTo + 1}): one scan with that limit.
     *       Every executor sees the same {@code min(total, upTo + 1)}
     *       rows and counts its own fragments' share. The caller
     *       compares {@link FtsHitCount#scanned()} with the limit to
     *       decide whether the share is exact or a lower bound: when
     *       the scan filled its limit the table has more than
     *       {@code upTo} matches and every executor reports a lower
     *       bound, whatever its share. The shares themselves need
     *       not sum to {@code upTo + 1}, since a tie in score at the
     *       limit lets each executor's scan pick a different row.</li>
     *   <li>{@code limit == 0} ({@code track_total_hits: true}): a
     *       probe scan with {@code limit(effectiveSubsetProbeLimit)}
     *       for the rows the executor's fragments hold. When it
     *       comes back short every match has been seen and the
     *       executor's share is the exact count. When it fills up the
     *       probe is discarded and the count-only scan above runs with
     *       the {@code fragmentIds} restriction, paying the prefilter
     *       read for that one shape.</li>
     * </ul>
     *
     * <p>A prefilter carried by {@code fts} (the scalar clauses of a
     * collapsed bool query) is passed the same way the hits scan
     * passes it, so the count covers exactly the rows the hits phase
     * can return.
     *
     * <p>{@code limit} caps the rows the scan returns; {@code 0}
     * means no cap. Even with no payload columns the scan's cost
     * grows with the number of matches (Lance scores and ranks every
     * posting before it can emit rows), so a caller that only needs
     * to know whether more than {@code n} rows match passes
     * {@code n + 1} and stops the scan there.
     */
    static FtsHitCount countFtsHitsDirectly(Dataset dataset, LanceFtsQuery fts, List<Integer> fragmentIds, long limit) throws Exception {
        boolean subset = fragmentIds != null && !LanceFtsQuery.coversAllFragments(fragmentIds, dataset);
        if (!subset) {
            ScanOptions.Builder builder = countOnlyScan(fts);
            if (limit > 0) {
                builder = builder.limit(limit);
            }
            return FtsHitCount.whole(countRows(dataset, builder.build()));
        }
        Set<Integer> own = new HashSet<>(fragmentIds);
        if (limit > 0) {
            return countOwnRows(dataset, rowAddressScan(fts).limit(limit).build(), own);
        }
        long subsetRows = 0L;
        for (Fragment fragment : dataset.getFragments()) {
            if (own.contains(fragment.getId())) {
                subsetRows += fragment.countRows();
            }
        }
        long probeLimit = LanceFtsQuery.effectiveSubsetProbeLimit(subsetRows);
        FtsHitCount probe = countOwnRows(dataset, rowAddressScan(fts).limit(probeLimit).build(), own);
        if (probe.scanned() < probeLimit) {
            return probe;
        }
        return FtsHitCount.whole(
            countRows(dataset, LanceFtsQuery.restrictToFragmentsUnlessAll(countOnlyScan(fts), fragmentIds, dataset).build())
        );
    }

    /** Scan options for a count-only FTS scan: no columns, no row address, no row id. */
    private static ScanOptions.Builder countOnlyScan(LanceFtsQuery fts) {
        ScanOptions.Builder builder = new ScanOptions.Builder().fullTextQuery(fts.fullTextQuery())
            .columns(Collections.emptyList())
            .withRowAddress(false)
            .withRowId(false);
        if (fts.prefilterSql() != null) {
            builder = builder.filter(fts.prefilterSql()).prefilter(true);
        }
        return builder;
    }

    /** Scan options for an FTS scan that returns {@code _rowaddr} only. */
    private static ScanOptions.Builder rowAddressScan(LanceFtsQuery fts) {
        return countOnlyScan(fts).withRowAddress(true);
    }

    private static long countRows(Dataset dataset, ScanOptions options) throws Exception {
        long total = 0L;
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                total += reader.getVectorSchemaRoot().getRowCount();
            }
        }
        return total;
    }

    /**
     * Result of {@link #countFtsHitsDirectly}: the rows Lance returned
     * for the scan before any fragment filter ({@code scanned}), and
     * how many of them belong to the fragments the executor holds
     * ({@code own}). The two are equal when the scan was already
     * restricted to those fragments or covered the whole table on an
     * executor that holds every fragment.
     */
    record FtsHitCount(long scanned, long own) {
        /** A scan whose every returned row belongs to the executor. */
        static FtsHitCount whole(long rows) {
            return new FtsHitCount(rows, rows);
        }
    }

    /**
     * Read {@code options} against {@code dataset} and count the rows
     * whose fragment id is in {@code own} next to every row returned.
     */
    private static FtsHitCount countOwnRows(Dataset dataset, ScanOptions options, Set<Integer> own) throws Exception {
        long scanned = 0L;
        long kept = 0L;
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                int rows = root.getRowCount();
                scanned += rows;
                for (int i = 0; i < rows; i++) {
                    if (own.contains((int) (rowAddr.get(i) >>> 32))) {
                        kept++;
                    }
                }
            }
        }
        return new FtsHitCount(scanned, kept);
    }
}
