/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.index.search.NestedHelper;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.execute.LanceAggregateResults;
import org.opensearch.lance.engine.ColumnStore;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.lance.engine.FragmentGroupScan;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.plan.execute.FragmentPlan;
import org.opensearch.lance.plan.execute.FragmentPlanRefiner;
import org.opensearch.lance.plan.execute.MergeReducer;
import org.opensearch.lance.plan.execute.PlanExecutor;
import org.opensearch.lance.plan.execute.PlanExecutor.MatchedCount;
import org.opensearch.lance.query.FtsAdmission;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.query.LanceHintingWeight;
import org.opensearch.lance.query.LanceInvalidInput;
import org.opensearch.lance.query.LanceKnnQuery;
import org.opensearch.lance.query.LanceScanFilterQuery;
import org.opensearch.script.ScriptService;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.MultiBucketCollector;
import org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.fetch.subphase.FetchDocValuesContext;
import org.opensearch.search.fetch.subphase.FetchFieldsContext;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.rescore.RescorerBuilder;
import org.opensearch.search.searchafter.SearchAfterBuilder;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.tasks.CancellableTask;
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
 *       style top docs collectors return the top-{@code size} docs with
 *       real scores (BM25 for Lance FTS, cosine for Lance knn), with the
 *       request's {@code min_score} and {@code terminate_after} composed
 *       around them ({@link CollectorKnobs}), and {@link FragmentFetchPhase}
 *       renders every hit through the stock fetch sub phases over the
 *       stored fields
 *       {@link org.opensearch.lance.engine.LanceFragmentLeafReader#materialiseStoredFields}
 *       synthesises ({@code _id}, {@code _source} under the request's
 *       source filter, {@code stored_fields}, {@code docvalue_fields},
 *       {@code fields}, {@code _explanation}).</li>
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
 * consulted for it. Under {@code min_score} or {@code terminate_after}
 * the count is what the hits collection saw through those collectors
 * instead, since neither Lance nor the Weight's hit count knows the
 * score threshold or the bound.
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

    /**
     * Pool the intra request work (collection slices, column load
     * group scans, aggregation pushdown scans) is submitted to. A
     * separate constant so a test can pin the split between this pool
     * and the handler's own SEARCH pool: the SEARCH queue admits
     * requests, so filling it with intra request tasks turns load the
     * node could serve into 429s.
     */
    static final String INTRA_REQUEST_POOL = ThreadPool.Names.INDEX_SEARCHER;

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
     * Pool the aggregation pushdown, the column loads and the collection
     * slices run their extra work on: the {@code index_searcher} pool,
     * the one core gives concurrent segment search its slices. It must
     * not be the SEARCH pool this action itself executes on: a slice or
     * scan task submitted there stays in the SEARCH queue as a spent
     * entry after the calling thread has run it (the task executors
     * below never wait for a task the pool has not started), and with
     * every SEARCH thread parked at {@link #concurrencyLimit} those
     * entries drain only when a request completes. Each waiting request
     * then holds its tasks in the queue behind it, the population
     * multiplies with the fan-in, and the queue rejects new fragment
     * requests as 429 well below the load the node can serve. On the
     * {@code index_searcher} pool the intra request work has its own
     * queue; a full or rejecting pool degrades a request to one thread
     * instead of failing it, because the group scans and the slice loop
     * run rejected and unstarted tasks on the calling thread.
     */
    private final Executor intraRequestExecutor;
    /**
     * Reduce context ingredient for the slice level merge of the
     * aggregators; none of the allowed aggregations runs a script there.
     */
    private final ScriptService scriptService;
    /**
     * Node scoped snapshot and column cache every request acquires its
     * table view from; created by {@code LancePlugin.createComponents}.
     */
    private final LanceWarmCache warmCache;
    /**
     * Applies the node local guards to the plan the coordinator
     * shipped: a reader wrapper, the Lucene sort field types, and the
     * resolution of a pushed aggregate against the mapping move a
     * pushed operation to the Lucene side; nothing is planned here.
     */
    private final FragmentPlanRefiner refiner = new FragmentPlanRefiner();
    /**
     * Renders the hits of a page through the stock fetch sub phases;
     * stateless, shared by every request.
     */
    private final FragmentFetchPhase fetchPhase = new FragmentFetchPhase();

    @Inject
    public TransportLanceFragmentQueryAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndicesService indicesService,
        BigArrays bigArrays,
        CircuitBreakerService circuitBreakerService,
        LanceWarmCache warmCache,
        ScriptService scriptService
    ) {
        super(LanceFragmentQueryAction.NAME, transportService, actionFilters, LanceFragmentQueryRequest::new, ThreadPool.Names.SEARCH);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.bigArrays = bigArrays;
        this.circuitBreakerService = circuitBreakerService;
        this.warmCache = warmCache;
        this.scriptService = scriptService;
        int permits = LancePlugin.FRAGMENT_DISPATCH_MAX_CONCURRENT_SETTING.get(clusterService.getSettings());
        this.concurrencyLimit = new java.util.concurrent.Semaphore(permits, /*fair*/ false);
        this.intraRequestExecutor = transportService.getThreadPool().executor(INTRA_REQUEST_POOL);
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
            long start = System.nanoTime();
            LanceFragmentQueryResponse response = execute(request, LanceCancellation.of(task instanceof CancellableTask c ? c : null));
            LOGGER.debug(
                "lance.dispatch: fragment query for [{}] over {} fragments took {} us",
                request.indexName(),
                request.fragmentIds().isEmpty() ? "all" : Integer.toString(request.fragmentIds().size()),
                (System.nanoTime() - start) / 1_000L
            );
            listener.onResponse(response);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            listener.onFailure(interrupted);
        } catch (Exception e) {
            // A cancelled task (the coordinator's request timed out, or
            // the coordinator task itself was cancelled) ends a Lance
            // scan with TaskCancelledException at a batch boundary and
            // a Lucene collection between leaves; the exception may
            // reach here wrapped in the IOException the Weight and
            // reader contracts force on the scan loops. Report the
            // TaskCancelledException itself so the coordinator
            // recognises it, and log it at debug: it is the expected
            // outcome of a cancellation, not a failure of this node.
            TaskCancelledException cancelled = LanceCancellation.findCancelled(e);
            if (cancelled != null) {
                LOGGER.debug(
                    "fragment query for [{}] on [{}] was cancelled: {}",
                    request.indexName(),
                    request.tableUri(),
                    cancelled.getMessage()
                );
                listener.onFailure(cancelled);
                return;
            }
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
                    "fragment query failed on this node for [{}] plan [{}] fragments [{}]",
                    request.tableUri(),
                    request.plan(),
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
     * {@link #execute(LanceFragmentQueryRequest, LanceCancellation)}
     * without a task to cancel, for unit tests.
     */
    LanceFragmentQueryResponse execute(LanceFragmentQueryRequest request) throws Exception {
        return execute(request, LanceCancellation.NONE);
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
     *
     * <p>{@code cancellation} is the task the request runs under. Every
     * Lance scan of the request checks it at its batch boundaries and
     * Lucene's collection loop between leaves, so a cancelled task ends
     * the request with {@link TaskCancelledException} at the next such
     * point; a scan already inside a batch runs that batch to its end.
     */
    LanceFragmentQueryResponse execute(LanceFragmentQueryRequest request, LanceCancellation cancellation) throws Exception {
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
        // Per-column mapping overrides are persisted as JSON in a single
        // setting (with a fallback to the legacy multi_fields setting for
        // indexes created before it existed). Empty leaves the reader
        // with an empty sub-field map. Malformed JSON falls through to
        // IllegalArgumentException, which the outer catch turns into a
        // 500 for the caller; that is loud enough to surface a bad
        // index setting without hiding the failure behind an empty map.
        LanceOverrides overrides = LanceOverrides.of(indexMetadata.getSettings());
        Map<String, LinkedHashMap<String, String>> multiFields = overrides.subFields();
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
                overrides
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
                cancellation,
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
        LanceCancellation cancellation,
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
                cancellation,
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
                    cancellation,
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
                cancellation,
                fragmentCount,
                effectiveFragmentIds,
                attempt + 1
            );
        }
    }

    /**
     * Run the plan against a resolved {@link IndexService}, either
     * the node's own registered instance or a request-scoped temporary
     * one. Nothing below depends on an {@link org.opensearch.index.shard.IndexShard}:
     * the shard id is fixed at 0 (Lance-backed indexes are
     * single-shard) and the {@link IndexSettings} come from the
     * IndexService.
     *
     * <p>The plan the coordinator shipped ({@link LanceFragmentQueryRequest#plan()})
     * is refined once the mapping is at hand ({@link FragmentPlanRefiner}:
     * the reader wrapper, the Lucene sort field types, the resolution of
     * a pushed aggregate) and then executed: a pushed page runs as the
     * ordered, limited Lance scan, a pushed aggregate as the Substrait
     * scan, and everything else through Lucene's collector and
     * aggregators over the fragment readers, driven by the Lucene query
     * the plan's query part builds.
     */
    private LanceFragmentQueryResponse executeWithIndexService(
        IndexService indexService,
        IndexMetadata indexMetadata,
        LanceWarmCache.Snapshot snapshot,
        Map<String, LinkedHashMap<String, String>> multiFields,
        LanceFragmentQueryRequest request,
        LanceCancellation cancellation,
        int fragmentCount,
        List<Integer> effectiveFragmentIds
    ) throws Exception {
        ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        Dataset dataset = snapshot.dataset();
        FragmentPlan planned = request.plan();

        // Fetch the IndexService reader wrapper once so both
        // openWrappedReader and computeMatched see the same
        // wrapper reference. A non-null wrapper here is the
        // signal that DLS/FLS or a similar reader-level
        // transform may filter documents; the refiner moves every
        // wrapper sensitive pushed operation to the Lucene side and
        // computeMatched routes counts through the searcher instead
        // of Lance-side metadata paths, which would bypass the
        // wrapper and return the pre-DLS count.
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
                // Push the plan's scalar Lance SQL down to the leaf
                // reader. When the top-level query is a scalar filter
                // the planner spelled as Lance SQL (bool / term / terms
                // / range / exists / match_all), every request scoped
                // heap column scan the leaf reader issues inside
                // ensureXxxLoaded is layered with that filter, so
                // `filter + terms agg` and `filter + sum` materialise
                // only the matching rows of the aggregated column when
                // the column store cannot serve them. A full text or
                // knn shape carries its SQL as the Lance query's
                // prefilter instead, so the scalar filter is null there.
                // No refinement changes this value: the guards drop
                // pushed pages, aggregates and full text clauses, never
                // the scalar filter.
                planned.scalarFilterSql(),
                readerWrapper,
                cancellation
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
                ).withCancellation(cancellation)
            ) {
                // The searcher cuts the node's fragment leaves into up
                // to lance.fragment_path.slices slices and collects
                // them side by side on the index_searcher pool; the
                // aggregators read the same count through the context
                // to decide how they apply their shard thresholds.
                // Under terminate_after the request collects on one
                // slice: the bound is a count of documents collected in
                // order, which slices collected side by side would each
                // apply to their own share, and the abort that stops the
                // collection at the bound has one collector to reduce.
                int slices = request.terminateAfter() > 0
                    ? 1
                    : clusterService.getClusterSettings().get(LancePlugin.FRAGMENT_PATH_SLICES_SETTING);
                searchContext.withTargetMaxSliceCount(slices).withScriptService(scriptService);
                LanceFragmentIndexSearcher searcher = new LanceFragmentIndexSearcher(
                    dr,
                    indexService.getIndexSettings(),
                    searchContext,
                    circuitBreakerService.getBreaker(CircuitBreaker.REQUEST),
                    intraRequestExecutor
                );
                searchContext.withSearcher(searcher);
                if (cancellation.hasTask()) {
                    // Lucene side of the cancellation, the same hook the
                    // shard path's QueryPhase installs: ContextIndexSearcher
                    // runs it before every leaf and, through
                    // CancellableBulkScorer, every few hundred documents
                    // of a collection, and the Lance Weights created
                    // against this searcher check the same task between
                    // batches.
                    searcher.addQueryCancellation(cancellation::checkCancelled);
                }
                QueryShardContext qsc = indexService.newQueryShardContext(0, searcher, System::currentTimeMillis, null);
                searchContext.withQueryShardContext(qsc);

                SortAndFormats sortAndFormats = resolveSort(request, qsc);
                FieldDoc searchAfter = resolveSearchAfter(request, sortAndFormats);
                // The second pass of the request, built against this
                // node's mapping before the page is collected: the
                // rescorers size the first pass and run over it, the
                // collapse replaces the top docs collector, and both
                // reach the fetch phase through the context (rescore
                // explanations, the collapse field as a doc value field).
                List<RescoreContext> rescorers = resolveRescorers(request, sortAndFormats, indexService.getIndexSettings(), qsc);
                CollapseContext collapse = resolveCollapse(request, sortAndFormats, searchAfter, qsc);
                searchContext.withSecondPass(rescorers, collapse);
                searchContext.withProjection(
                    request.projection().fetchSource(),
                    request.projection().storedFields(),
                    resolveDocValuesContext(request.projection(), indexService),
                    request.projection().fetchFields().isEmpty() ? null : new FetchFieldsContext(request.projection().fetchFields()),
                    request.projection().explain()
                );
                CollectorKnobs knobs = FragmentHitsPages.knobsOf(request);
                int maxGroups = LancePlugin.AGGREGATION_PUSHDOWN_MAX_GROUPS_SETTING.get(qsc.getIndexSettings().getNodeSettings());
                FragmentPlanRefiner.Refined refined = refiner.refine(
                    planned,
                    new FragmentPlanRefiner.Inputs(
                        hasSecurityWrapper,
                        sortAndFormats,
                        request,
                        pushed -> LanceAggregateResults.resolve(
                            pushed.toPushedShape(),
                            pushed.substraitDirect(),
                            request.aggregations(),
                            dataset.getSchema(),
                            multiFields,
                            qsc,
                            maxGroups
                        )
                    )
                );
                FragmentPlan effective = refined.plan();
                if (refined.refined()) {
                    LOGGER.debug(
                        "lance.plan: index [{}] planned [{}] executed [{}] reason {}",
                        request.indexName(),
                        planned,
                        effective,
                        refined.reasons()
                    );
                } else {
                    LOGGER.debug("lance.plan: index [{}] planned [{}] executed [{}]", request.indexName(), planned, effective);
                }

                Query query = applyNonNestedFilter(
                    resolveLuceneQuery(effective, request, qsc, hasSecurityWrapper),
                    indexService.mapperService()
                );

                // Full-text scans rebuild the inverted index document
                // set in native memory when the index does not fit the
                // cache shard — the unbounded shapes (a full-text
                // clause the resolver left without a scan limit, or a
                // bounded page whose exact match count would run the
                // unbounded count-only scan) and the bounded top-k
                // pages alike, because Lance rebuilds the whole set
                // whatever the page size. Refuse the request with 429
                // before any Lance scan of it is created when the
                // node's free memory cannot hold the estimated rebuild
                // plus the scan buffers. The estimate is per table, so
                // it is judged on the table's physical rows, not this
                // executor's share: Lance rebuilds the whole document
                // set whichever fragments the scan keeps.
                FtsAdmission.Shape ftsShape = FtsAdmission.classify(
                    query,
                    request.trackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_ACCURATE
                );
                if (FtsAdmission.gates(ftsShape)) {
                    long tableRows = 0L;
                    for (LanceWarmCache.FragmentMeta fragment : snapshot.fragments()) {
                        tableRows += fragment.physicalRows();
                    }
                    FtsAdmission.admit(request.indexName(), tableRows, ftsShape);
                }

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
                // "explain": true explains every hit against the request's
                // query (not the post_filter conjunction, as on the shard
                // path); through the prebuilt Weight when there is one,
                // so a Lance scored hit is explained from the scan that
                // already ran rather than from a new one per hit. Set for
                // an explaining request only: the aggregators built over
                // this context keep seeing the constructor's placeholder,
                // as they did before the fetch phase existed.
                if (request.projection().explain()) {
                    searchContext.withQuery(prebuilt == null ? query : prebuilt);
                }
                // Match count runs through the same Weight as the
                // hits phase when the query is a scoring Lucene
                // query (FTS, knn), so strip any scan-limit hint
                // from the query before handing it to
                // computeMatched. Without this, an FTS Weight
                // that was clipped to `size` rows during
                // the Lucene hits phase would also clip the
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

                // Aggregations run over the top-level query only —
                // OpenSearch semantics for post_filter say the
                // filter applies to hits (and hits.total.value)
                // but not to aggregations. Hits and matched
                // therefore use the AND-combined query.
                //
                // A pushed page with Lance orderings runs as one
                // ordered, limited Lance scan: Lance returns the top
                // `fetch` rows already ordered, so the hits phase never
                // materialises the sort column for every matching row.
                // A pushed page without orderings (the scan's own order
                // is the page order) and every Lucene plan go through
                // the Lucene collector, with the scan limit the query
                // carries.
                FragmentPlan.TopK pushedTopK = effective.topK();
                boolean pushedPage = pushedTopK != null && !pushedTopK.orderings().isEmpty();
                boolean pushedAggregate = refined.aggregate() != null;
                // Which branch runs the refined plan, for plan.executed
                // in the stats: the Lance scan (an ordered page or a
                // Substrait aggregate) or Lucene's collector and
                // aggregators. Counted before the scans run so a failing
                // scan still shows which path the node took.
                FragmentPlanRefiner.recordExecuted(pushedPage || pushedAggregate);
                FragmentHitsPages.CollectedPage page;
                if (pushedPage) {
                    page = FragmentHitsPages.viaLanceSortedScan(
                        dataset,
                        request,
                        pushedTopK.toColumnOrderings(),
                        pushedTopK.fetch(),
                        pushedTopK.scanFilterSql(effective.filterSql()),
                        sortAndFormats,
                        searcher.getIndexReader(),
                        effectiveFragmentIds,
                        cancellation
                    );
                } else if (collapse != null) {
                    // One hit per distinct value of the collapse field,
                    // from the collapsing collector over the executor's
                    // fragments; the planner keeps a collapse request off
                    // the pushed page and the scan stays unbounded, since
                    // the groups are found among every match.
                    page = FragmentHitsPages.viaCollapsingCollector(
                        searcher,
                        hitsQuery,
                        hitsQuery == query ? lanceWeight : null,
                        collapse,
                        sortAndFormats,
                        searchAfter,
                        request.size(),
                        knobs,
                        request.trackTotalHitsUpTo()
                    );
                } else {
                    // The shared Weight drives the hits phase only when
                    // hitsQuery is the very query it was created for;
                    // with a post_filter the hits query is a conjunction
                    // Lucene has to build its own Weight for. With
                    // rescorers the first pass collects the largest
                    // window and the rescorers cut it back to the page.
                    page = FragmentHitsPages.viaIndexSearcher(
                        searcher,
                        hitsQuery,
                        hitsQuery == query ? lanceWeight : null,
                        sortAndFormats,
                        searchAfter,
                        request.firstPassSize(),
                        request.trackScores(),
                        knobs,
                        request.trackTotalHitsUpTo()
                    );
                    if (!rescorers.isEmpty()) {
                        page = FragmentHitsPages.rescore(page, rescorers, searcher, request.size());
                    }
                }
                Boolean terminatedEarly = page.terminatedEarly();
                InternalAggregations aggregations;
                MatchedCount matched;
                if (pushedAggregate) {
                    // The scan groups and aggregates on the Lance side and
                    // also yields the row total, so neither the Lucene
                    // aggregators nor computeMatched run for this request.
                    // The node's fragments are scanned in up to
                    // pushdown_parallelism groups; the extra scans run on
                    // the index_searcher pool.
                    long pushdownStart = System.nanoTime();
                    LanceAggregateResults.Result result = refined.aggregate()
                        .execute(
                            dataset,
                            effectiveFragmentIds,
                            effective.filterSql(),
                            clusterService.getClusterSettings().get(LancePlugin.AGGREGATION_PUSHDOWN_PARALLELISM_SETTING),
                            intraRequestExecutor,
                            cancellation,
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
                    AggregationsResult aggregated = aggregateViaIndexSearcher(
                        request,
                        searchContext,
                        searcher,
                        qsc,
                        query,
                        lanceWeight,
                        knobs
                    );
                    aggregations = aggregated.aggregations();
                    terminatedEarly = MergeReducer.mergeTerminatedEarly(terminatedEarly, aggregated.terminatedEarly());
                    if (page.collected() != null) {
                        // min_score or terminate_after: the count is what
                        // the hits collection saw through the knobs; the
                        // Lance side counts and the Weight's hit count do
                        // not know the score threshold or the bound.
                        matched = request.trackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_DISABLED
                            ? MatchedCount.NOT_TRACKED
                            : new MatchedCount(
                                page.collected().value(),
                                page.collected().relation() == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                            );
                    } else {
                        matched = PlanExecutor.computeMatched(
                            dataset,
                            request,
                            effective.scalarFilterSql(),
                            searcher,
                            countQuery,
                            hasSecurityWrapper,
                            ftsWeight,
                            cancellation
                        );
                    }
                }
                FragmentHitsPages.HitsPage hits = FragmentHitsPages.materialise(
                    searchContext,
                    fetchPhase,
                    searcher.getIndexReader(),
                    page.scoreDocs(),
                    sortAndFormats
                );
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "lance.dispatch: fragment path slices for [{}]: {} leaves in {} slices (lance.fragment_path.slices {})",
                        request.indexName(),
                        searcher.getIndexReader().leaves().size(),
                        searcher.getSlices().length,
                        slices
                    );
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
                    aggregations,
                    terminatedEarly
                );
            }
        }
    }

    /**
     * Confine {@code query} to parent (non nested) docs when the mapping
     * has nested fields, the way the shard path's
     * {@code DefaultSearchContext.buildFilteredQuery} does. Without the
     * filter a {@code match_all} or any doc-value query over a reader
     * with nested columns would collect the hidden child docs as hits
     * and count them in {@code hits.total}.
     *
     * <p>The Lance-side queries are exempt: their scorers decode row
     * addresses and map them to parent doc ids, so they can never yield
     * a child doc, and wrapping them would push the executor off the
     * shared-Weight and count fast paths for no gain.
     */
    private static Query applyNonNestedFilter(Query query, MapperService mapperService) {
        if (!mapperService.hasNested()) {
            return query;
        }
        if (query instanceof LanceScanFilterQuery || query instanceof LanceFtsQuery || query instanceof LanceKnnQuery) {
            return query;
        }
        if (!new NestedHelper(mapperService).mightMatchNestedDocs(query)) {
            return query;
        }
        return new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST)
            .add(Queries.newNonNestedFilter(), BooleanClause.Occur.FILTER)
            .build();
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
     * Strip any scan-limit hint from a Lance-backed Query so it can
     * be reused for match-count purposes.
     *
     * <p>{@link LanceFtsQuery} and {@link LanceScanFilterQuery} both
     * embed an optional top-k inside the {@link Query} instance
     * itself: the fragment scan uses that hint to stop early during
     * the hits phase. The same instance is also what
     * {@link PlanExecutor#computeMatched} hands to {@code IndexSearcher.count}
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
     * Build the Lucene {@link Query} the shared {@link ContextIndexSearcher}
     * executes from the plan's query part. Priority order:
     * <ol>
     *   <li>{@link FragmentPlan#lanceClause()} — the planner pushed a
     *       full text or knn clause into the scan (alone, or as the
     *       single {@code must} of a {@code bool} whose scalar
     *       {@code filter} / {@code must_not} companions, or the
     *       {@code lance_knn}'s inner {@code filter}, print as Lance
     *       SQL). The clause's own Lucene query is built through
     *       {@code toQuery}, so the mapping validation (field types,
     *       vector dimension) and the clause boost behave as on the
     *       unplanned path, and the plan's filter SQL rides on the
     *       {@link LanceFtsQuery} / {@link LanceKnnQuery} as the scan's
     *       prefilter: Lance evaluates the scalar predicate before the
     *       inverted-index or nearest lookup instead of Lucene
     *       intersecting two full scans. A plain FTS clause takes the
     *       scan limit {@link #resolveScanFilterTopK} allows; a boosted
     *       one stays unbounded like the unplanned boosted path.</li>
     *   <li>{@link FragmentPlan#filterSql()} — the planner spelled the
     *       whole top-level query tree as Lance SQL (bool / term / terms
     *       / range / exists / match_all over mapped columns). Wrapped
     *       in {@link LanceScanFilterQuery} so one Lance native scan per
     *       shard evaluates the predicate and yields the matching row
     *       addresses, optionally clipped to {@code size} when the shape
     *       allows. This beats the Lucene translation of the same tree
     *       (PointRange / term queries that fall back to doc values on a
     *       reader without points), which has to load every referenced
     *       column through the doc value path before it can match a
     *       single row, whereas the Lance scan reads only
     *       {@code _rowaddr}. {@link PlanExecutor#computeMatched}
     *       trusts the same SQL for {@code hits.total}, so the two stay
     *       consistent by construction.</li>
     *   <li>{@link LanceFragmentQueryRequest#query()} — the top-level
     *       {@link org.opensearch.index.query.QueryBuilder} the
     *       coordinator forwarded, for shapes the planner refused
     *       (match / anything scoring, a tree touching an unmapped or
     *       ip / geo field, a full text clause under a reader wrapper).
     *       Runs through the local {@link QueryShardContext#toQuery} so
     *       per-node mapping decisions apply.</li>
     *   <li>Otherwise: {@link MatchAllDocsQuery}.</li>
     * </ol>
     */
    private Query resolveLuceneQuery(
        FragmentPlan plan,
        LanceFragmentQueryRequest request,
        QueryShardContext qsc,
        boolean hasSecurityWrapper
    ) throws IOException {
        // Top-k pushdown clips the Lance scan to the first `size`
        // rows before the reader wrapper sees them. Under a wrapper
        // (DLS) some of those rows are hidden afterwards and the page
        // would come back short of `size` although more visible rows
        // match, so the scan stays unbounded and the Lucene collector
        // does the clipping after the liveDocs are applied.
        int scanLimit = hasSecurityWrapper ? LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED : resolveScanFilterTopK(request);
        int ftsScanLimit = scanLimit == LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED ? LanceFtsQuery.SCAN_LIMIT_UNBOUNDED : scanLimit;
        if (plan.lanceClause() != null) {
            Query clause = plan.lanceClause().toQuery(qsc);
            if (clause instanceof LanceFtsQuery fts) {
                LanceFtsQuery pushed = fts.withScanFilterSql(plan.filterSql());
                return ftsScanLimit == LanceFtsQuery.SCAN_LIMIT_UNBOUNDED ? pushed : pushed.withScanLimit(ftsScanLimit);
            }
            if (clause instanceof BoostQuery boosted && boosted.getQuery() instanceof LanceFtsQuery fts) {
                // The boosted clause keeps its BoostQuery wrapper and
                // the scan stays unbounded, like the top-level boosted
                // FTS path.
                return new BoostQuery(fts.withScanFilterSql(plan.filterSql()), boosted.getBoost());
            }
            if (clause instanceof LanceKnnQuery knn) {
                return knn.withScanFilterSql(plan.filterSql());
            }
            if (clause instanceof BoostQuery boosted && boosted.getQuery() instanceof LanceKnnQuery knn) {
                return new BoostQuery(knn.withScanFilterSql(plan.filterSql()), boosted.getBoost());
            }
            throw new IllegalStateException(
                "the planned Lance clause [" + plan.lanceClause().getWriteableName() + "] built " + clause.getClass().getSimpleName()
            );
        }
        if (plan.filterSql() != null) {
            return new LanceScanFilterQuery(plan.filterSql(), scanLimit);
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
            if (ftsScanLimit != LanceFtsQuery.SCAN_LIMIT_UNBOUNDED && base instanceof LanceFtsQuery fts) {
                return fts.withScanLimit(ftsScanLimit);
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
     *       returns for a scalar query. An explicit {@code _score}
     *       sort also keeps the scan unbounded on purpose: the
     *       bounded FTS scan decides ties inside Lance, and the
     *       documented way to a stable tie order is spelling the
     *       score sort out.</li>
     *   <li>No aggregations — aggregators need every matched doc to
     *       accumulate bucket counts and metric state.</li>
     *   <li>No post_filter — post_filter narrows below the scan and
     *       would leave the caller short of the requested rows.</li>
     * </ul>
     * The caller additionally keeps the scan unbounded when a reader
     * wrapper is installed: the wrapper's liveDocs narrow the result
     * after the scan the same way a post_filter would. {@code min_score}
     * and {@code terminate_after} keep it unbounded too: the collectors
     * that apply them also count the matches, and a scan clipped to
     * {@code size} rows would clip that count. A {@code collapse} keeps
     * it unbounded because its groups are found among every match, and
     * a {@code rescore} raises the clip to the rescorers' window.
     *
     * <p>The coordinator already folds the top-level {@code from} into
     * {@code size} before shipping the request (see
     * {@link TransportLanceCoordinatorAction#executeCoordinated}'s
     * {@code perNodeSize = from + size} calculation), so
     * {@link LanceFragmentQueryRequest#size()} is the per-node top-k
     * the coordinator is asking for. Using it directly here is safe.
     *
     * <p>{@code hits.total.value} is served by
     * {@link PlanExecutor#computeMatched}, which for the scalar-filter shape
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
        if (request.hasCollectorKnobs()) {
            return LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED;
        }
        if (request.aggregations() != null && !request.aggregations().getAggregatorFactories().isEmpty()) {
            return LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED;
        }
        if (request.postFilter() != null) {
            return LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED;
        }
        if (request.collapse() != null) {
            // The collapsing collector picks one hit per group among
            // every match; a scan clipped to the page would hide the
            // groups past the clip.
            return LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED;
        }
        // With rescorers the first pass collects the largest window, so
        // the clip is the window, not the page.
        int size = request.firstPassSize();
        if (size <= 0) {
            // size:0 count-only shape never enters
            // the Lucene hits phase, and computeMatched serves the
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
     * count scan {@link PlanExecutor#computeMatched} uses.
     */
    private boolean hintsHelpBeforeScoring(LanceFragmentQueryRequest request) {
        if (resolveScanFilterTopK(request) != LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED) {
            return false;
        }
        boolean hasAggregations = request.aggregations() != null && !request.aggregations().getAggregatorFactories().isEmpty();
        boolean sortsAPage = request.size() > 0 && !request.sorts().isEmpty();
        // The collapsing collector reads the collapse field's doc values
        // (keyword ordinals included) for every hit it groups.
        boolean collapsesAPage = request.size() > 0 && request.collapse() != null;
        return hasAggregations || sortsAPage || collapsesAPage;
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
        return SortBuilder.buildSort(request.sorts(), qsc).orElse(null);
    }

    /**
     * The request's rescorers as {@link RescoreContext}s built against
     * the node's mapping ({@link RescorerBuilder#buildContext}), in body
     * order; empty without {@code rescore}. The two checks
     * {@code DefaultSearchContext.preProcess} applies come first, with
     * its messages: a rescore next to a sort that builds a Lucene sort
     * (a lone descending {@code _score} builds none and passes, as
     * there), and a window above {@code index.max_rescore_window}.
     */
    private static List<RescoreContext> resolveRescorers(
        LanceFragmentQueryRequest request,
        SortAndFormats sortAndFormats,
        IndexSettings indexSettings,
        QueryShardContext qsc
    ) throws IOException {
        if (request.rescores().isEmpty()) {
            return List.of();
        }
        if (sortAndFormats != null) {
            throw new IllegalArgumentException("Cannot use [sort] option in conjunction with [rescore].");
        }
        int maxWindow = indexSettings.getMaxRescoreWindow();
        List<RescoreContext> rescorers = new ArrayList<>(request.rescores().size());
        for (RescorerBuilder<?> rescore : request.rescores()) {
            RescoreContext rescoreContext = rescore.buildContext(qsc);
            if (rescoreContext.getWindowSize() > maxWindow) {
                throw new IllegalArgumentException(
                    "Rescore window ["
                        + rescoreContext.getWindowSize()
                        + "] is too large. "
                        + "It must be less than ["
                        + maxWindow
                        + "]. This prevents allocating massive heaps for storing the results "
                        + "to be rescored. This limit can be set by changing the ["
                        + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey()
                        + "] index level setting."
                );
            }
            rescorers.add(rescoreContext);
        }
        return rescorers;
    }

    /**
     * The request's {@code collapse} as a {@link CollapseContext} built
     * against the node's mapping with the checks of
     * {@code CollapseBuilder.build} and its messages: the field has to be
     * mapped, a keyword or a number, with doc values. The one check not
     * applied is the one refusing {@code inner_hits} on a field that is
     * not indexed: every Lance derived field is mapped {@code index:
     * false} (the fragment path answers its term queries from the Lance
     * scan and the shard path from doc values), so the group searches of
     * the expansion can pin a collapse value on it, which is all the
     * check guards on a plain index. {@code null} without a collapse.
     * With a {@code search_after} cursor the sort has to be the collapse
     * field alone, the check {@code SearchService.parseSource} applies
     * with its message; the shard path reports it as a search exception
     * (500), here it is the client's error and answers 400.
     */
    private static CollapseContext resolveCollapse(
        LanceFragmentQueryRequest request,
        SortAndFormats sortAndFormats,
        FieldDoc searchAfter,
        QueryShardContext qsc
    ) {
        if (request.collapse() == null) {
            return null;
        }
        if (searchAfter != null) {
            SortField[] sort = sortAndFormats.sort.getSort();
            if (sort.length != 1 || !request.collapse().getField().equals(sort[0].getField())) {
                throw new IllegalArgumentException(
                    "collapse field and sort field must be the same when use `collapse` in conjunction with `search_after`"
                );
            }
        }
        String field = request.collapse().getField();
        MappedFieldType fieldType = qsc.fieldMapper(field);
        if (fieldType == null) {
            throw new IllegalArgumentException("no mapping found for `" + field + "` in order to collapse on");
        }
        if (fieldType.unwrap() instanceof KeywordFieldMapper.KeywordFieldType == false
            && fieldType.unwrap() instanceof NumberFieldMapper.NumberFieldType == false) {
            throw new IllegalArgumentException("unknown type for collapse field `" + field + "`, only keywords and numbers are accepted");
        }
        if (fieldType.hasDocValues() == false) {
            throw new IllegalArgumentException("cannot collapse on field `" + field + "` without `doc_values`");
        }
        return new CollapseContext(field, fieldType, request.collapse().getInnerHits());
    }

    /**
     * The request's {@code search_after} cursor typed against the Lucene
     * sort the mapping built, through the same
     * {@link SearchAfterBuilder#buildFieldDoc} the shard path uses: each
     * JSON value is converted to its {@link org.apache.lucene.search.SortField}
     * type (a {@code Double} to the {@code Float} of a {@code _score}
     * clause, an {@code Integer} to the {@code Long} of a numeric
     * field), and a cursor whose length differs from the sort's is
     * refused with the shard path's message. A cursor without a sort
     * ({@code sort} absent, which the coordinator already refuses, or a
     * lone descending {@code _score}, which
     * {@code SortBuilder.buildSort} folds into no sort) is refused with
     * the shard path's message as well. {@code null} without a cursor.
     */
    private static FieldDoc resolveSearchAfter(LanceFragmentQueryRequest request, SortAndFormats sortAndFormats) {
        if (request.searchAfter() == null) {
            return null;
        }
        return SearchAfterBuilder.buildFieldDoc(sortAndFormats, request.searchAfter());
    }

    /**
     * The request's {@code docvalue_fields} resolved against the mapping
     * the way {@code SearchService.parseSource} resolves them: patterns
     * expanded to field names and the total bounded by
     * {@code index.max_docvalue_fields_search}. {@code null} when the
     * request has none.
     */
    private static FetchDocValuesContext resolveDocValuesContext(HitProjection projection, IndexService indexService) {
        if (projection.docValueFields().isEmpty()) {
            return null;
        }
        return FetchDocValuesContext.create(
            indexService.mapperService()::simpleMatchToFullName,
            indexService.getIndexSettings().getMaxDocvalueFields(),
            projection.docValueFields()
        );
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

    /** The aggregations of a request and whether {@code terminate_after} stopped their collection ({@code null} without the knob). */
    private record AggregationsResult(InternalAggregations aggregations, Boolean terminatedEarly) {
        static final AggregationsResult NONE = new AggregationsResult(null, null);
    }

    /**
     * Aggregator path. Runs the request's
     * {@link org.opensearch.search.aggregations.AggregatorFactories.Builder}
     * against the shared per-fragment reader and returns the
     * per-node {@link InternalAggregations} for the coordinator to
     * reduce.
     *
     * <p>Returns no aggregations when the request carries none; the
     * response ships {@code aggregations == null} in that case.
     *
     * <p>{@code sharedWeight}, when non-null, is the Weight the caller
     * already built for {@code query}; the collector tree is then
     * driven through it so a Lance-backed query's native scan is
     * reused rather than repeated. The aggregators' collector is
     * satisfied by a {@link ScoreMode#COMPLETE} Weight the same way it
     * is by the no-scores Weight the searcher would otherwise build:
     * it simply does not call {@code score()}.
     *
     * <p>The aggregators run through a {@link CollectorManager} of the
     * shape of OpenSearch's {@code AggregationCollectorManager}: one
     * aggregator tree per slice, built by {@code newCollector} from
     * the same factories, so the searcher collects the slices side by
     * side, and a {@code reduce} that reads each tree's result and
     * merges them with the slice level partial reduce
     * ({@link LanceFragmentSearchContext#partialOnShard()}). With one
     * slice the single tree's result is returned as it is, which is
     * what the shard path does without concurrent segment search and
     * what this method returned before slicing.
     *
     * <p>{@code knobs} composes {@code min_score} and
     * {@code terminate_after} around every tree, as {@code QueryPhase}
     * composes them around the aggregation collectors: a document below
     * the score threshold is not aggregated, and the aggregators see
     * the documents collected before the bound stopped the collection.
     * The bound aborts the search before the searcher post collects the
     * tree, so that step runs here on the abort path.
     */
    private AggregationsResult aggregateViaIndexSearcher(
        LanceFragmentQueryRequest request,
        LanceFragmentSearchContext searchContext,
        LanceFragmentIndexSearcher searcher,
        QueryShardContext qsc,
        Query query,
        Weight sharedWeight,
        CollectorKnobs knobs
    ) throws Exception {
        AggregatorFactories.Builder factoriesBuilder = request.aggregations();
        if (factoriesBuilder == null || factoriesBuilder.getAggregatorFactories().isEmpty()) {
            return AggregationsResult.NONE;
        }

        AggregatorFactories factories = factoriesBuilder.build(qsc, null);
        SliceAggregationCollectorManager trees = new SliceAggregationCollectorManager(searchContext, factories);
        CollectorKnobs.Wrapped<Collector, InternalAggregations> manager = knobs.wrap(trees);
        Boolean terminatedEarly = knobs.terminatesEarly() ? Boolean.FALSE : null;
        try {
            InternalAggregations aggregations = sharedWeight != null
                ? searcher.search(sharedWeight, manager)
                : searcher.search(query, manager);
            return new AggregationsResult(aggregations, terminatedEarly);
        } catch (CollectorKnobs.Terminated terminated) {
            for (Collector tree : manager.innersCreated()) {
                searchContext.bucketCollectorProcessor().processPostCollection(tree);
            }
            return new AggregationsResult(manager.reduceCreated(), Boolean.TRUE);
        }
    }

    /**
     * One aggregator tree per slice, reduced on the executor. The
     * slice loop of {@link LanceFragmentIndexSearcher} ends every slice
     * with {@code BucketCollectorProcessor.processPostCollection},
     * which runs {@code postCollection()} and {@code buildTopLevel()}
     * on each top-level aggregator of the slice's tree on the slice's
     * own thread and stores the result inside the aggregator;
     * {@link #reduce} reads those stored results back through
     * {@code toInternalAggregations}, as the shard path's
     * {@code AggregationCollectorManager} does. Calling
     * {@code postCollection()} or {@code buildAggregations()} a second
     * time here would drive a {@code DeferableBucketAggregator}
     * (breadth_first terms with metric children) through
     * {@code BestBucketsDeferringCollector#prepareSelectedBuckets}
     * twice; the second call throws "Already been replayed".
     *
     * <p>The request's {@link MultiBucketConsumer} is shared by the
     * trees of every slice, as it is between slices on the shard path,
     * and reset once the slice results have been read, before the
     * partial reduce counts its own buckets, at the same point
     * {@code AggregationCollectorManager.reduce} resets it.
     */
    private static final class SliceAggregationCollectorManager implements CollectorManager<Collector, InternalAggregations> {
        private final LanceFragmentSearchContext searchContext;
        private final AggregatorFactories factories;

        SliceAggregationCollectorManager(LanceFragmentSearchContext searchContext, AggregatorFactories factories) {
            this.searchContext = searchContext;
            this.factories = factories;
        }

        @Override
        public Collector newCollector() throws IOException {
            List<Aggregator> topLevelAggregators = factories.createTopLevelAggregators(searchContext);
            BucketCollector collector = MultiBucketCollector.wrap(topLevelAggregators);
            collector.preCollection();
            return collector;
        }

        @Override
        public InternalAggregations reduce(Collection<Collector> collectors) throws IOException {
            List<InternalAggregation> internals = searchContext.bucketCollectorProcessor().toInternalAggregations(collectors);
            searchContext.aggregations().multiBucketConsumer().reset();
            InternalAggregations aggregations = InternalAggregations.from(internals);
            if (!searchContext.shouldUseConcurrentSearch()) {
                return aggregations;
            }
            InternalAggregations reduced = InternalAggregations.reduce(
                Collections.singletonList(aggregations),
                searchContext.partialOnShard()
            );
            // The reduce groups the slice results by name in a hash map
            // and returns them in that map's order. Put them back in the
            // request's order, which is the order a single tree yields,
            // so the executor's answer does not depend on the slice count.
            Map<String, InternalAggregation> byName = new LinkedHashMap<>();
            for (InternalAggregation aggregation : internals) {
                byName.putIfAbsent(aggregation.getName(), null);
            }
            for (Aggregation aggregation : reduced.asList()) {
                byName.put(aggregation.getName(), (InternalAggregation) aggregation);
            }
            return InternalAggregations.from(new ArrayList<>(byName.values()));
        }
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
     * {@link PlanExecutor#computeMatched} count from the wrapped reader's
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
     * wrapper is installed so {@link PlanExecutor#computeMatched} can route
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
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper,
        LanceCancellation cancellation
    ) throws IOException {
        // Column loads of this reader (the store's and the heap
        // fallback's) scan the node's fragments in up to
        // lance.fragment_path.parallelism groups on the index_searcher
        // pool, so a column is read into its arrays on several cores.
        // The scan carries the request's cancellation so every group,
        // on whichever thread it runs, stops at its next batch once
        // the task is cancelled.
        FragmentGroupScan groupScan = new FragmentGroupScan(
            intraRequestExecutor,
            clusterService.getClusterSettings().get(LancePlugin.FRAGMENT_PATH_PARALLELISM_SETTING),
            cancellation
        );
        DirectoryReader lanceReader = LanceDirectoryReader.openForSnapshot(
            new ByteBuffersDirectory(),
            snapshot,
            columnStore,
            effectiveFragmentIds,
            filterSql,
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST),
            groupScan
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
}
