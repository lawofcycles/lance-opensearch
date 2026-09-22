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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
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
import org.apache.calcite.rel.RelNode;
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
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.index.search.NestedHelper;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.LanceMappingMeta;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.NativeMemoryLimit;
import org.opensearch.lance.execute.LanceAggregateResults;
import org.opensearch.lance.engine.ColumnStore;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.lance.engine.FragmentGroupScan;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.engine.LanceDirectoryReader;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceFragmentLeafReader;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchemas;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.LanceTopK;
import org.opensearch.lance.plan.rel.PushedOperation;
import org.opensearch.lance.plan.rel.PushedOperation.PushedAggregate;
import org.opensearch.lance.plan.rel.PushedOperation.PushedFts;
import org.opensearch.lance.plan.rel.PushedOperation.PushedKnn;
import org.opensearch.lance.plan.rel.PushedOperation.PushedTopK;
import org.opensearch.lance.plan.rules.SortResolution;
import org.opensearch.lance.plan.translate.QueryToRex;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.lance.query.FtsAdmission;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceHintingWeight;
import org.opensearch.lance.query.LanceInvalidInput;
import org.opensearch.lance.query.LanceKnnQuery;
import org.opensearch.lance.query.LanceScanFilterQuery;
import org.opensearch.script.ScriptService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
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
     * Pool the aggregation pushdown, the column loads and the collection
     * slices run their extra work on: the SEARCH pool this action itself
     * executes on. No pool is added for it. A group scan and a slice loop
     * never block on a task the pool has not started (the calling thread
     * runs whatever the pool does not pick up), so a saturated SEARCH
     * pool degrades a request to one thread instead of parking it.
     */
    private final Executor searchExecutor;
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
     * Builds the Volcano planner an aggregation request is routed
     * through: when the pushdown rule fires, the physical plan is the
     * scan carrying the Substrait bytes and the request skips the
     * Lucene aggregators. The budgets mirror the explain action's.
     */
    private final LancePlannerFactory plannerFactory;

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
        this.searchExecutor = transportService.getThreadPool().executor(ThreadPool.Names.SEARCH);
        long nativeBudgetBytes = NativeMemoryLimit.parse(
            LancePlugin.NATIVE_MEMORY_LIMIT_SETTING.get(clusterService.getSettings()),
            LancePlugin.NATIVE_MEMORY_LIMIT_SETTING.getKey()
        );
        this.plannerFactory = new LancePlannerFactory(nativeBudgetBytes, Runtime.getRuntime().maxMemory());
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
            TaskCancelledException cancelled = findCancelled(e);
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
     * The {@link TaskCancelledException} in the cause chain of
     * {@code e}, or {@code null} when the chain has none.
     */
    static TaskCancelledException findCancelled(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof TaskCancelledException cancelled) {
                return cancelled;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return null;
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
        LanceCancellation cancellation,
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
                // a scalar filter the planner can print as Lance
                // SQL (bool / term / terms / range / exists /
                // match_all), request.filterSql() carries the SQL
                // and every request scoped heap column scan the
                // leaf reader issues inside ensureXxxLoaded is
                // layered with that filter, so `filter + terms agg`
                // and `filter + sum` materialise only the matching
                // rows of the aggregated column when the column
                // store cannot serve them. FTS and knn queries have
                // no SQL representation so filterSql is null there.
                request.filterSql(),
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
                // them side by side on the SEARCH pool; the aggregators
                // read the same count through the context to decide
                // how they apply their shard thresholds.
                int slices = clusterService.getClusterSettings().get(LancePlugin.FRAGMENT_PATH_SLICES_SETTING);
                searchContext.withTargetMaxSliceCount(slices).withScriptService(scriptService);
                LanceFragmentIndexSearcher searcher = new LanceFragmentIndexSearcher(
                    dr,
                    indexService.getIndexSettings(),
                    searchContext,
                    circuitBreakerService.getBreaker(CircuitBreaker.REQUEST),
                    searchExecutor
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

                Query query = applyNonNestedFilter(
                    resolveLuceneQuery(request, qsc, hasSecurityWrapper, indexMetadata, dataset, multiFields, searcher.getIndexReader()),
                    indexService.mapperService()
                );

                // Unbounded full-text scans (a full-text clause the
                // resolver left without a scan limit, or a bounded
                // page whose exact match count would run the unbounded
                // count-only scan) rebuild the inverted index document
                // set in native memory when the index does not fit the
                // cache shard. Refuse the request with 429 before any
                // Lance scan of it is created when the node's free
                // memory cannot hold that rebuild. The estimate is per
                // table, so it is judged on the table's physical rows,
                // not this executor's share: Lance rebuilds the whole
                // document set whichever fragments the scan keeps.
                if (FtsAdmission.runsUnboundedFtsScan(query, request.trackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_ACCURATE)) {
                    long tableRows = 0L;
                    for (LanceWarmCache.FragmentMeta fragment : snapshot.fragments()) {
                        tableRows += fragment.physicalRows();
                    }
                    FtsAdmission.admit(request.indexName(), tableRows);
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
                // Sorted scalar-filter pages go through the planner:
                // SearchRequestToRel.translateQuery builds the query
                // root, the sort clauses become the LanceTopK's
                // collations, and PushSortLimitIntoLanceScan folds
                // both into the scan when every collation resolves to
                // a Lance ColumnOrdering (and the search_after cursor,
                // when present, to a strict SQL bound). Lance then
                // returns the top `size` rows already ordered, so the
                // hits phase never materialises the sort column for
                // every matching row. Every shape the planner does not
                // fold goes through the Lucene collector.
                LanceTableScan plannedTopK = hasSecurityWrapper || sortAndFormats == null
                    ? null
                    : resolvePlannedTopK(request, qsc, indexMetadata, dataset, multiFields, searcher.getIndexReader(), sortAndFormats);
                HitsPage hits;
                if (plannedTopK != null) {
                    PushedTopK pushedTopK = plannedTopK.pushedTopK().orElseThrow();
                    hits = scanSortedHitsViaLance(
                        dataset,
                        request,
                        pushedTopK.toScanOrderings(),
                        pushedTopK.fetch(),
                        plannedScanFilter(plannedTopK, pushedTopK),
                        sortAndFormats,
                        searcher.getIndexReader(),
                        effectiveFragmentIds,
                        cancellation
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
                LanceAggregateResults pushdown = resolveAggregatePushdown(
                    request,
                    hasSecurityWrapper,
                    dataset,
                    multiFields,
                    qsc,
                    searcher.getIndexReader()
                );
                if (pushdown != null) {
                    // The scan groups and aggregates on the Lance side and
                    // also yields the row total, so neither the Lucene
                    // aggregators nor computeMatched run for this request.
                    // The node's fragments are scanned in up to
                    // pushdown_parallelism groups; the extra scans run on
                    // the SEARCH pool this request already executes on.
                    long pushdownStart = System.nanoTime();
                    LanceAggregateResults.Result result = pushdown.execute(
                        dataset,
                        effectiveFragmentIds,
                        request.filterSql(),
                        clusterService.getClusterSettings().get(LancePlugin.AGGREGATION_PUSHDOWN_PARALLELISM_SETTING),
                        searchExecutor,
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
                    aggregations = aggregateViaIndexSearcher(request, searchContext, searcher, qsc, query, lanceWeight);
                    matched = computeMatched(dataset, request, searcher, countQuery, hasSecurityWrapper, ftsWeight, cancellation);
                }
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
                    aggregations
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
        return ((long) lance.fragmentId() << 32) | (lance.rowOf(doc - leaf.docBase) & 0xFFFFFFFFL);
    }

    /**
     * Decide whether this request's aggregations run as a Substrait
     * group by inside the Lance scan and, when they do, prepare the
     * executor. The request qualifies when
     * {@code lance.aggregation.pushdown} is on, it asks for no hits
     * ({@code size} 0) and has no {@code post_filter}, its query is
     * {@code match_all} or a scalar filter the coordinator translated to
     * Lance SQL ({@link LanceFragmentQueryRequest#filterSql()}; FTS and
     * knn queries have no SQL form and stay on the aggregator path), no
     * reader wrapper is installed (DLS / FLS filter documents in the
     * Lucene reader, which the scan never sees), and the planner pushed
     * the aggregation tree into the scan: the translator accepts the
     * tree, the Volcano planner's pushdown rule obtains the Substrait
     * bytes from the producer, and {@link LanceAggregateResults#resolve}
     * pairs the request's builders with the pushed aggregate. Returns
     * {@code null} otherwise, in which case the Lucene aggregators run.
     */
    private LanceAggregateResults resolveAggregatePushdown(
        LanceFragmentQueryRequest request,
        boolean hasSecurityWrapper,
        Dataset dataset,
        Map<String, LinkedHashMap<String, String>> multiFields,
        QueryShardContext qsc,
        IndexReader reader
    ) {
        if (hasSecurityWrapper) {
            // This gate must stay first and must not be folded into the
            // combined condition below: the planner model built further
            // down reads reader::numDocs from the raw reader, which a
            // DLS / FLS wrapper has not filtered, and the pushed scan
            // itself never sees the wrapper. Nothing wrapper sensitive
            // may run past this point.
            return null;
        }
        if (!clusterService.getClusterSettings().get(LancePlugin.AGGREGATION_PUSHDOWN_SETTING)) {
            return null;
        }
        if (request.size() != 0 || request.postFilter() != null || request.aggregations() == null) {
            return null;
        }
        boolean scalarQuery = request.query() == null || request.query() instanceof MatchAllQueryBuilder || request.filterSql() != null;
        if (!scalarQuery) {
            return null;
        }
        if (!LanceAggregationSupport.isPushdownCandidate(request.aggregations())) {
            return null;
        }
        RelNode logical;
        try {
            LanceSchemas.IndexModel model = LanceSchemas.model(request.indexName(), dataset.getSchema(), multiFields, reader::numDocs);
            logical = SearchRequestToRel.translateAggregations(request.aggregations(), model, plannerFactory);
        } catch (UnsupportedOperationException unsupported) {
            return null;
        }
        RelNode physical = plannerFactory.plan(logical);
        if (!(physical instanceof LanceTableScan scan)) {
            return null;
        }
        PushedAggregate pushed = scan.pushedAggregate().orElse(null);
        if (pushed == null) {
            return null;
        }
        int maxGroups = LancePlugin.AGGREGATION_PUSHDOWN_MAX_GROUPS_SETTING.get(qsc.getIndexSettings().getNodeSettings());
        return LanceAggregateResults.resolve(
            pushed.aggregate(),
            pushed.substrait(),
            request.aggregations(),
            dataset.getSchema(),
            multiFields,
            qsc,
            maxGroups
        );
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
     *       must_not} clauses all translate to Lance SQL, and a
     *       {@code lance_knn} with an inner {@code filter}, are planned
     *       through {@link SearchRequestToRel#translateQuery} and the
     *       fuse rules first (see {@link #resolvePlannedLanceQuery}):
     *       the pushed operation's SQL rides on the
     *       {@link LanceFtsQuery} / {@link LanceKnnQuery} as the scan's
     *       prefilter, so Lance evaluates the scalar predicate before
     *       the inverted-index or nearest lookup instead of Lucene
     *       intersecting two full scans. The FTS collapse is skipped
     *       when a reader wrapper is installed because DLS filters
     *       would not be part of the Lance-side predicate.</li>
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
        IndexMetadata indexMetadata,
        Dataset dataset,
        Map<String, LinkedHashMap<String, String>> multiFields,
        IndexReader reader
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
            Query planned = resolvePlannedLanceQuery(
                rewritten,
                qsc,
                hasSecurityWrapper,
                scanLimit,
                request,
                indexMetadata,
                dataset,
                multiFields,
                reader
            );
            if (planned != null) {
                return planned;
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
     * Resolve the two query shapes the planner folds into the Lance
     * dataset scan: a {@code bool} whose {@code must} is exactly one
     * Lance FTS clause with scalar {@code filter} / {@code must_not}
     * companions, and a {@code lance_knn} with an inner {@code filter}.
     * {@link SearchRequestToRel#translateQuery} builds the
     * {@code LanceFtsMatch} / {@code LanceKnnSearch} tree, the Volcano
     * run fires the fuse rules, and the pushed operation's filter SQL
     * rides on the {@link LanceFtsQuery} / {@link LanceKnnQuery} whose
     * Weight hands it to the scan as a prefilter. The clause's own
     * Lucene query is still built through {@code toQuery}, so the
     * mapping validation (field types, vector dimension) and the
     * clause boost behave exactly as on the unplanned path.
     *
     * <p>Returns null for every shape the planner does not fold — a
     * bare FTS clause (the plain {@code toQuery} path already builds
     * the same scan and attaches the top-k), an unfiltered
     * {@code lance_knn}, a scalar tree, a bool the shape detection or
     * the scalar translation refuses, and any FTS shape under a reader
     * wrapper (the collapse would move clauses out of the wrapper's
     * view) — and the caller keeps the Lucene composition.
     *
     * <p>A {@code lance_knn} whose inner filter does not plan (the
     * translator refuses a clause, a field is unmapped or ip mapped,
     * or the predicate has no SQL spelling) throws
     * {@link IllegalArgumentException} and answers 400: the filter is
     * a prefilter by contract and silently dropping it would return
     * wrong nearest rows. The knn shape resolves under a reader
     * wrapper too, because the inner filter is the caller's own
     * predicate, applied before DLS narrows the hits on the Lucene
     * side.
     */
    private Query resolvePlannedLanceQuery(
        QueryBuilder rewritten,
        QueryShardContext qsc,
        boolean hasSecurityWrapper,
        int scanLimit,
        LanceFragmentQueryRequest request,
        IndexMetadata indexMetadata,
        Dataset dataset,
        Map<String, LinkedHashMap<String, String>> multiFields,
        IndexReader reader
    ) throws IOException {
        boolean knnShape = rewritten instanceof LanceKnnQueryBuilder;
        if (!knnShape && !(rewritten instanceof BoolQueryBuilder)) {
            return null;
        }
        if (!knnShape && hasSecurityWrapper) {
            return null;
        }
        LanceKnnQueryBuilder knn = knnShape ? (LanceKnnQueryBuilder) rewritten : null;
        if (knn != null && knn.filter() == null) {
            return null;
        }
        Set<String> excludedColumns = TransportLanceCoordinatorAction.sqlExcludedColumns(LanceOverrides.of(indexMetadata.getSettings()));
        if (knn != null && QueryToRex.referencesAny(knn.filter(), excludedColumns)) {
            throw knnFilterRefusal(knn, "predicates on ip and geo_point fields are evaluated over encoded doc values on the Lucene side");
        }
        if (knn == null && rewritten instanceof BoolQueryBuilder bool && QueryToRex.referencesAny(bool, excludedColumns)) {
            // An ip or geo predicate has no Lance SQL form; keep the
            // whole bool on the Lucene composition.
            return null;
        }
        RelNode physical;
        try {
            LanceSchemas.IndexModel model = plannerQueryModel(request.indexName(), indexMetadata, dataset, multiFields, reader);
            physical = plannerFactory.plan(SearchRequestToRel.translateQuery(rewritten, model, plannerFactory));
        } catch (UnsupportedOperationException unsupported) {
            if (knn != null) {
                throw knnFilterRefusal(knn, unsupported.getMessage());
            }
            return null;
        }
        if (!(physical instanceof LanceTableScan scan)) {
            if (knn != null) {
                throw knnFilterRefusal(knn, "the filter has no Lance SQL form");
            }
            return null;
        }
        PushedFts pushedFts = scan.pushedFts().orElse(null);
        if (pushedFts != null) {
            Query clause = pushedFts.fts().ftsClause().toQuery(qsc);
            if (clause instanceof LanceFtsQuery fts) {
                LanceFtsQuery pushed = fts.withScanFilterSql(pushedFts.filterSql());
                int ftsScanLimit = scanLimit == LanceScanFilterQuery.SCAN_LIMIT_UNBOUNDED ? LanceFtsQuery.SCAN_LIMIT_UNBOUNDED : scanLimit;
                return ftsScanLimit == LanceFtsQuery.SCAN_LIMIT_UNBOUNDED ? pushed : pushed.withScanLimit(ftsScanLimit);
            }
            if (clause instanceof BoostQuery boosted && boosted.getQuery() instanceof LanceFtsQuery fts) {
                // The boosted clause keeps its BoostQuery wrapper and
                // the scan stays unbounded, like the top-level boosted
                // FTS path.
                return new BoostQuery(fts.withScanFilterSql(pushedFts.filterSql()), boosted.getBoost());
            }
            return null;
        }
        PushedKnn pushedKnn = scan.pushedKnn().orElse(null);
        if (pushedKnn != null) {
            Query clause = pushedKnn.knn().knnClause().toQuery(qsc);
            if (clause instanceof LanceKnnQuery knnQuery) {
                return knnQuery.withScanFilterSql(pushedKnn.filterSql());
            }
            if (clause instanceof BoostQuery boosted && boosted.getQuery() instanceof LanceKnnQuery knnQuery) {
                return new BoostQuery(knnQuery.withScanFilterSql(pushedKnn.filterSql()), boosted.getBoost());
            }
        }
        if (knn != null) {
            throw knnFilterRefusal(knn, "the filter has no Lance SQL form");
        }
        return null;
    }

    /**
     * The 400 a filtered {@code lance_knn} answers when its filter
     * cannot travel to the Lance scan, naming the filter clause's
     * builder class so the caller sees which part was refused.
     */
    private static IllegalArgumentException knnFilterRefusal(LanceKnnQueryBuilder knn, String reason) {
        return new IllegalArgumentException(
            "[lance_knn] filter type ["
                + knn.filter().getClass().getSimpleName()
                + "] is not supported by the pre-filter translator: "
                + reason
        );
    }

    /**
     * The planner's model of the index for query clause translation:
     * the Arrow schema from the open dataset, the attach's multi field
     * spec, the mapping's recorded renames, and the primary key column
     * {@code ids} queries resolve against.
     */
    private static LanceSchemas.IndexModel plannerQueryModel(
        String indexName,
        IndexMetadata indexMetadata,
        Dataset dataset,
        Map<String, LinkedHashMap<String, String>> multiFields,
        IndexReader reader
    ) {
        Map<String, String> renamedFields = new LinkedHashMap<>();
        for (LanceMappingMeta.RenamedField renamed : LanceMappingMeta.renamedFields(indexMetadata.mapping())) {
            renamedFields.put(renamed.from(), renamed.to());
        }
        String primaryKeyField = indexMetadata.getSettings().get("index.lance.primary_key_field", "");
        Set<String> dateOverrideColumns = LanceOverrides.of(indexMetadata.getSettings()).dateColumns().keySet();
        return LanceSchemas.model(
            indexName,
            dataset.getSchema(),
            multiFields,
            renamedFields,
            primaryKeyField,
            dateOverrideColumns,
            reader::numDocs
        );
    }

    /**
     * Plan a sorted hits page through the planner: the query root from
     * {@link SearchRequestToRel#translateQuery}, the sort clauses as
     * the {@link LanceTopK}'s collations, and the Volcano run firing
     * {@code PushSortLimitIntoLanceScan}. Returns the scan carrying
     * the {@link PushedTopK} (and, for a scalar page, the pushed
     * filter whose SQL the ordered scan evaluates) when the planner
     * folded the page, or null when the shape stays on the Lucene
     * collector.
     *
     * <p>The guards mirror what the fold can reproduce. {@code size}
     * must be positive with no {@code post_filter} and no aggregations
     * (both need the full match set on the Lucene side). Every Lucene
     * {@link org.apache.lucene.search.SortField} OpenSearch built must
     * be one of the two plain field-data shapes, because
     * {@link #sortValueFrom} types the page's sort values from them;
     * an {@code ip} format is excluded because address order is not
     * the stored strings' order. The shape must be scalar: a non-null
     * {@link LanceFragmentQueryRequest#filterSql()} (the coordinator
     * translated the whole tree), no query, or {@code match_all};
     * FTS and knn order by score, which the fold serves through the
     * scan limit instead (see {@link #resolveScanFilterTopK}). A
     * {@code search_after} whose cursor equals a sort field's missing
     * sentinel stays on Lucene: the strict SQL bound cannot tell the
     * sentinel from a stored value.
     */
    private LanceTableScan resolvePlannedTopK(
        LanceFragmentQueryRequest request,
        QueryShardContext qsc,
        IndexMetadata indexMetadata,
        Dataset dataset,
        Map<String, LinkedHashMap<String, String>> multiFields,
        IndexReader reader,
        SortAndFormats sortAndFormats
    ) {
        if (request.size() <= 0 || request.postFilter() != null) {
            return null;
        }
        if (request.aggregations() != null && !request.aggregations().getAggregatorFactories().isEmpty()) {
            return null;
        }
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
        for (DocValueFormat format : sortAndFormats.formats) {
            if (format == DocValueFormat.IP) {
                return null;
            }
        }
        boolean scalarShape = request.filterSql() != null
            || request.query() == null
            || request.query() instanceof org.opensearch.index.query.MatchAllQueryBuilder;
        if (!scalarShape) {
            return null;
        }
        Object[] searchAfter = request.searchAfter();
        if (searchAfter != null && (searchAfter.length != sortFields.length || cursorHitsMissingSentinel(searchAfter, sortFields))) {
            return null;
        }
        try {
            LanceSchemas.IndexModel model = plannerQueryModel(request.indexName(), indexMetadata, dataset, multiFields, reader);
            QueryBuilder rewritten = request.query() == null ? null : Rewriteable.rewrite(request.query(), qsc, true);
            RelNode root = SearchRequestToRel.translateQuery(rewritten, model, plannerFactory);
            List<org.apache.calcite.rel.RelFieldCollation> collations = SortResolution.collationsOf(
                request.sorts(),
                root.getRowType(),
                model
            );
            LanceTopK topK = new LanceTopK(
                root.getCluster(),
                root.getCluster().traitSetOf(org.apache.calcite.plan.Convention.NONE),
                root,
                collations,
                request.size(),
                0,
                searchAfter == null ? null : Arrays.asList(searchAfter)
            );
            RelNode physical = plannerFactory.plan(topK);
            if (physical instanceof LanceTableScan scan) {
                PushedTopK pushed = scan.pushedTopK().orElse(null);
                if (pushed != null && !pushed.toScanOrderings().isEmpty()) {
                    return scan;
                }
            }
            return null;
        } catch (UnsupportedOperationException | IOException notPlanned) {
            // A sort clause without a collation spelling (geo distance,
            // script, nested, mode, literal missing) or a query the
            // translator refuses: the Lucene collector serves it.
            return null;
        }
    }

    /**
     * Whether a {@code search_after} value equals the missing-value
     * sentinel its sort field reports for null rows. A client paging
     * past a null row feeds the sentinel back; the strict SQL bound
     * compares stored values only, so such a cursor stays on the
     * Lucene comparator, which knows the sentinel.
     */
    private static boolean cursorHitsMissingSentinel(Object[] searchAfter, org.apache.lucene.search.SortField[] sortFields) {
        for (int i = 0; i < searchAfter.length; i++) {
            Object missing = sortFields[i].getMissingValue();
            if (missing instanceof Number sentinel
                && searchAfter[i] instanceof Number cursor
                && Double.compare(sentinel.doubleValue(), cursor.doubleValue()) == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * The filter of the ordered Lance scan: the pushed filter's SQL
     * (the query the page cuts) ANDed with the {@code search_after}
     * cursor bound, either alone when the other is absent, null when
     * the page is an unfiltered first page.
     */
    private static String plannedScanFilter(LanceTableScan scan, PushedTopK pushedTopK) {
        String filterSql = scan.pushedFilter().map(PushedOperation.PushedFilter::sql).orElse(null);
        String cursorSql = pushedTopK.cursorSql();
        if (filterSql == null) {
            return cursorSql;
        }
        if (cursorSql == null) {
            return filterSql;
        }
        return "(" + filterSql + ") AND (" + cursorSql + ")";
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
     * Hits phase for sorted scalar-filter pages: one Lance scan with
     * the pushed query's SQL (plus the {@code search_after} cursor
     * bound, see {@link #plannedScanFilter}),
     * {@code setColumnOrderings}, {@code limit(fetch)} and the sort
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
        int fetch,
        String filterSql,
        SortAndFormats sortAndFormats,
        org.apache.lucene.index.IndexReader reader,
        List<Integer> fragmentIds,
        LanceCancellation cancellation
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
            .limit(fetch)
            .withRowAddress(true);
        if (filterSql != null) {
            builder = builder.filter(filterSql);
        }
        // Ordered (fragment id, doc id, raw sort values) triples in the
        // order Lance returned them, which is the response order.
        List<long[]> addresses = new ArrayList<>(fetch);
        List<Object[]> sortValues = new ArrayList<>(fetch);
        try (
            org.lance.ipc.LanceScanner scanner = dataset.newScan(builder.build());
            org.apache.arrow.vector.ipc.ArrowReader arrowReader = scanner.scanBatches()
        ) {
            while (arrowReader.loadNextBatch()) {
                cancellation.checkCancelled();
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
        } catch (IOException | TaskCancelledException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
        // One take per leaf for the rows behind the page, then render
        // each hit in Lance's order. The decoded offsets are physical
        // rows; the leaf's stored-fields and prefetch paths are keyed by
        // doc id, so each offset maps through docOfRow (identity unless
        // the table has nested columns).
        java.util.Map<Integer, List<Integer>> docsByFragment = new java.util.HashMap<>();
        for (long[] address : addresses) {
            LanceFragmentLeafReader lance = leafByFragment.get((int) address[0]);
            if (lance == null) {
                continue;
            }
            docsByFragment.computeIfAbsent((int) address[0], k -> new ArrayList<>()).add(lance.docOfRow((int) address[1]));
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
            lance.materialiseStoredFields(lance.docOfRow((int) address[1]), visitor);
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
        CollectorManager<Collector, InternalAggregations> manager = new SliceAggregationCollectorManager(searchContext, factories);
        if (sharedWeight != null) {
            return searcher.search(sharedWeight, manager);
        }
        return searcher.search(query, manager);
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
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper,
        LanceCancellation cancellation
    ) throws IOException {
        // Column loads of this reader (the store's and the heap
        // fallback's) scan the node's fragments in up to
        // lance.fragment_path.parallelism groups on the SEARCH pool,
        // so a column is read into its arrays on several cores. The
        // scan carries the request's cancellation so every group, on
        // whichever thread it runs, stops at its next batch once the
        // task is cancelled.
        FragmentGroupScan groupScan = new FragmentGroupScan(
            searchExecutor,
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
     * <p>The counter is a plain {@code long[]}: {@code search(Query, Collector)}
     * of {@link LanceFragmentIndexSearcher} visits every leaf on the
     * calling thread whatever the searcher's slice count (a single
     * collector cannot be shared between slices), so the collector is
     * never touched by two threads. Counting through a
     * {@link org.apache.lucene.search.CollectorManager} would run the
     * slices in parallel and need one counter per slice.
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
     *   <li>Filter set, no fragment list: use
     *       {@link Dataset#countRows(String)}.</li>
     *   <li>Filter set, fragment list: {@link #countScalarFilter},
     *       which asks Lance to count the matches of the listed
     *       fragments natively when the request wants an accurate
     *       total and otherwise scans at most
     *       {@code trackTotalHitsUpTo + 1} rows to decide between an
     *       exact count and a lower bound.</li>
     * </ul>
     * The first two read Lance metadata or run one native count, so
     * they are cheap regardless of the match count and are reported
     * exact; the {@code track_total_hits} bound then only affects how
     * the coordinator presents them.
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
        LanceFtsQuery.LanceFtsWeight ftsWeight,
        LanceCancellation cancellation
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
            // scanFilterSql, which every count path below applies
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
                return MatchedCount.exact(countFtsHitsDirectly(dataset, fts, fragmentIds, 0L, cancellation).own());
            }
            long limit = (long) upTo + 1L;
            FtsHitCount counted = countFtsHitsDirectly(dataset, fts, fragmentIds, limit, cancellation);
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
        return countScalarFilter(dataset, filterSql, fragmentIds, upTo, cancellation);
    }

    /**
     * Count the rows of {@code fragmentIds} that match the scalar
     * filter {@code filterSql}, honouring the {@code track_total_hits}
     * bound {@code upTo}.
     *
     * <p>The coordinator always hands an executor its fragment list,
     * so every scalar-filter count of the fragment path arrives here.
     * The scan asks Lance for zero payload columns and no row address
     * or row id, so nothing but a row count crosses from Lance to
     * Java. The fragment list is passed to Lance: for a scalar filter
     * it only narrows the fragments the filtered read opens (unlike
     * an FTS scan, where a fragment list turns into a prefilter over
     * {@code _rowid}), so there is no reason to scan the whole table
     * and sort the rows by fragment here.
     *
     * <ul>
     *   <li>{@code upTo == TRACK_TOTAL_HITS_ACCURATE}
     *       ({@code track_total_hits: true}, which is also what
     *       {@code _count} sends): {@link LanceScanner#countRows()}.
     *       Lance puts a {@code count(*)} on top of the filtered read
     *       and runs it across its own thread pool; the result comes
     *       back as one number. Pulling the same rows through
     *       {@code scanBatches()} instead would hand every match to
     *       this search thread one batch at a time, which for a
     *       filter that matches most of a large table costs seconds
     *       of a single core.</li>
     *   <li>Otherwise: one scan with {@code limit(upTo + 1)}, whose
     *       returned rows are counted. Lance plans the limit as a
     *       node above the filtered read, so the read stops once
     *       {@code upTo + 1} rows are through; at most that many rows
     *       reach Java. Reaching the limit proves the executor holds
     *       more than {@code upTo} matches, which is all the
     *       coordinator needs for {@code gte}, so the result is then
     *       a lower bound; coming back short means every match was
     *       seen and the count is exact. Because the scan is already
     *       restricted to the executor's fragments, every returned
     *       row is the executor's own and the returned count itself
     *       is compared with the limit. (The FTS counterpart judges
     *       on the rows before its fragment filter because that scan
     *       runs over the whole table.)</li>
     * </ul>
     *
     * <p>{@link LanceScanner#countRows()} is not used for the bounded
     * case: Lance applies its {@code count(*)} before the limit node,
     * so the limit would be ignored and the count would be exact at
     * full cost, which is what the bound exists to avoid.
     *
     * <p>{@link #NATIVE_SCALAR_COUNTS} and {@link #BOUNDED_SCALAR_COUNT_SCANS}
     * record which of the two paths ran, so a test can tell a native
     * count from a batch loop that happens to return the same number.
     */
    static MatchedCount countScalarFilter(Dataset dataset, String filterSql, List<Integer> fragmentIds, int upTo) throws Exception {
        return countScalarFilter(dataset, filterSql, fragmentIds, upTo, LanceCancellation.NONE);
    }

    /**
     * {@link #countScalarFilter(Dataset, String, List, int)} whose bounded
     * scan stops once {@code cancellation} reports a cancelled task. The
     * native count of an exact request runs inside Lance in one call and
     * has no batch boundary to stop at.
     */
    static MatchedCount countScalarFilter(
        Dataset dataset,
        String filterSql,
        List<Integer> fragmentIds,
        int upTo,
        LanceCancellation cancellation
    ) throws Exception {
        ScanOptions.Builder builder = countOnlyScan(filterSql, fragmentIds);
        if (upTo == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
            cancellation.checkCancelled();
            try (LanceScanner scanner = dataset.newScan(builder.build())) {
                long counted = scanner.countRows();
                NATIVE_SCALAR_COUNTS.incrementAndGet();
                return MatchedCount.exact(counted);
            }
        }
        long limit = (long) upTo + 1L;
        long counted = countRows(dataset, builder.limit(limit).build(), cancellation);
        BOUNDED_SCALAR_COUNT_SCANS.incrementAndGet();
        return new MatchedCount(counted, counted >= limit);
    }

    /**
     * Number of scalar filter counts {@link #countScalarFilter} answered
     * through {@link LanceScanner#countRows()} since the class loaded.
     */
    static final AtomicLong NATIVE_SCALAR_COUNTS = new AtomicLong();

    /**
     * Number of scalar filter counts {@link #countScalarFilter} answered
     * by a scan limited to {@code upTo + 1} rows since the class loaded.
     */
    static final AtomicLong BOUNDED_SCALAR_COUNT_SCANS = new AtomicLong();

    /**
     * Scan options for a count-only scalar filter scan over
     * {@code fragmentIds}: no columns, no row address, no row id.
     */
    private static ScanOptions.Builder countOnlyScan(String filterSql, List<Integer> fragmentIds) {
        return new ScanOptions.Builder().filter(filterSql)
            .fragmentIds(fragmentIds)
            .columns(Collections.emptyList())
            .withRowAddress(false)
            .withRowId(false);
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
        return countFtsHitsDirectly(dataset, fts, fragmentIds, limit, LanceCancellation.NONE);
    }

    /** {@link #countFtsHitsDirectly(Dataset, LanceFtsQuery, List, long)} whose scans stop once {@code cancellation} reports a cancelled task. */
    static FtsHitCount countFtsHitsDirectly(
        Dataset dataset,
        LanceFtsQuery fts,
        List<Integer> fragmentIds,
        long limit,
        LanceCancellation cancellation
    ) throws Exception {
        boolean subset = fragmentIds != null && !LanceFtsQuery.coversAllFragments(fragmentIds, dataset);
        if (!subset) {
            ScanOptions.Builder builder = countOnlyScan(fts);
            if (limit > 0) {
                builder = builder.limit(limit);
            }
            return FtsHitCount.whole(countRows(dataset, builder.build(), cancellation));
        }
        Set<Integer> own = new HashSet<>(fragmentIds);
        if (limit > 0) {
            return countOwnRows(dataset, rowAddressScan(fts).limit(limit).build(), own, cancellation);
        }
        long subsetRows = 0L;
        for (Fragment fragment : dataset.getFragments()) {
            if (own.contains(fragment.getId())) {
                subsetRows += fragment.countRows();
            }
        }
        long probeLimit = LanceFtsQuery.effectiveSubsetProbeLimit(subsetRows);
        FtsHitCount probe = countOwnRows(dataset, rowAddressScan(fts).limit(probeLimit).build(), own, cancellation);
        if (probe.scanned() < probeLimit) {
            return probe;
        }
        return FtsHitCount.whole(
            countRows(dataset, LanceFtsQuery.restrictToFragmentsUnlessAll(countOnlyScan(fts), fragmentIds, dataset).build(), cancellation)
        );
    }

    /** Scan options for a count-only FTS scan: no columns, no row address, no row id. */
    private static ScanOptions.Builder countOnlyScan(LanceFtsQuery fts) {
        ScanOptions.Builder builder = new ScanOptions.Builder().fullTextQuery(fts.fullTextQuery())
            .columns(Collections.emptyList())
            .withRowAddress(false)
            .withRowId(false);
        if (fts.scanFilterSql() != null) {
            builder = builder.filter(fts.scanFilterSql()).prefilter(true);
        }
        return builder;
    }

    /** Scan options for an FTS scan that returns {@code _rowaddr} only. */
    private static ScanOptions.Builder rowAddressScan(LanceFtsQuery fts) {
        return countOnlyScan(fts).withRowAddress(true);
    }

    private static long countRows(Dataset dataset, ScanOptions options, LanceCancellation cancellation) throws Exception {
        long total = 0L;
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                cancellation.checkCancelled();
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
    private static FtsHitCount countOwnRows(Dataset dataset, ScanOptions options, Set<Integer> own, LanceCancellation cancellation)
        throws Exception {
        long scanned = 0L;
        long kept = 0L;
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                cancellation.checkCancelled();
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
