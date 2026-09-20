/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.indices.breaker.BreakerSettings;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.TransportLanceAttachAction;
import org.opensearch.lance.dispatch.LanceDispatchActionFilter;
import org.opensearch.lance.dispatch.LanceCreateIndexActionFilter;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.index.LanceBuildIndexesAction;
import org.opensearch.lance.index.TransportLanceBuildIndexesAction;
import org.opensearch.lance.mapper.LanceTextFieldMapper;
import org.opensearch.lance.mapper.LanceVectorFieldMapper;
import org.opensearch.lance.namespace.AllowedTableRoots;
import org.opensearch.lance.namespace.LanceNamespaceListAction;
import org.opensearch.lance.namespace.LanceNamespaceService;
import org.opensearch.lance.namespace.TransportLanceNamespaceListAction;
import org.opensearch.lance.query.LanceFtsBoolQueryBuilder;
import org.opensearch.lance.query.LanceFtsBoostQueryBuilder;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchPhraseQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.lance.query.LanceMultiMatchQueryBuilder;
import org.opensearch.lance.refs.LanceRefsAction;
import org.opensearch.lance.refs.TransportLanceRefsAction;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.lance.rest.RestBuildIndexesAction;
import org.opensearch.lance.rest.RestNamespaceAction;
import org.opensearch.lance.rest.RestRefsAction;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ActionPlugin.ActionHandler;
import org.opensearch.plugins.CircuitBreakerPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

/**
 * Plugin entry point. Registers the reader-side surface for Lance tables:
 * the REST endpoints for namespace / attach / build_indexes, the
 * {@link LanceEngineFactory} that wraps each Lance-backed index in a
 * read-only engine, mapping type parsers, and the {@code lance_knn} query.
 */
public class LancePlugin extends Plugin implements ActionPlugin, EnginePlugin, MapperPlugin, SearchPlugin, CircuitBreakerPlugin {

    private static final Logger LOGGER = LogManager.getLogger(LancePlugin.class);

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(
            new QuerySpec<>(LanceKnnQueryBuilder.NAME, LanceKnnQueryBuilder::new, LanceKnnQueryBuilder::fromXContent),
            new QuerySpec<>(LanceMatchQueryBuilder.NAME, LanceMatchQueryBuilder::new, LanceMatchQueryBuilder::fromXContent),
            new QuerySpec<>(
                LanceMatchPhraseQueryBuilder.NAME,
                LanceMatchPhraseQueryBuilder::new,
                LanceMatchPhraseQueryBuilder::fromXContent
            ),
            new QuerySpec<>(LanceMultiMatchQueryBuilder.NAME, LanceMultiMatchQueryBuilder::new, LanceMultiMatchQueryBuilder::fromXContent),
            new QuerySpec<>(LanceFtsBoostQueryBuilder.NAME, LanceFtsBoostQueryBuilder::new, LanceFtsBoostQueryBuilder::fromXContent),
            new QuerySpec<>(LanceFtsBoolQueryBuilder.NAME, LanceFtsBoolQueryBuilder::new, LanceFtsBoolQueryBuilder::fromXContent)
        );
    }

    @Override
    public java.util.Map<String, Mapper.TypeParser> getMappers() {
        return java.util.Map.of(
            LanceTextFieldMapper.CONTENT_TYPE,
            LanceTextFieldMapper.PARSER,
            LanceVectorFieldMapper.CONTENT_TYPE,
            LanceVectorFieldMapper.PARSER
        );
    }

    public static final Setting<String> TABLE_SETTING = Setting.simpleString(
        LanceEngineFactory.TABLE_SETTING,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    public static final Setting<String> PRIMARY_KEY_FIELD_SETTING = Setting.simpleString(
        LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING,
        "",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    /**
     * String form of the declared primary key's Arrow type family, used by
     * {@link LanceEngineFactory} to pick the right lookup strategy. Only
     * {@code "long"} (signed integer PK, default) and {@code "keyword"}
     * (Utf8 PK) are recognised; unknown values fall back to {@code "long"}
     * so indices created before this setting existed stay readable. The
     * setting has no meaning when
     * {@link #PRIMARY_KEY_FIELD_SETTING} is empty (the table has no PK).
     */
    public static final Setting<String> PRIMARY_KEY_TYPE_SETTING = Setting.simpleString(
        LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING,
        "long",
        LancePlugin::validatePrimaryKeyType,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    public static final Setting<Long> VERSION_SETTING = Setting.longSetting(
        LanceEngineFactory.VERSION_SETTING,
        -1L,
        -1L,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    /**
     * Lance tag the index follows, written by attach when the body carries
     * {@code "tag"}. Empty means the index follows the latest manifest (or
     * a pinned version when {@link #VERSION_SETTING} is set).
     */
    public static final Setting<String> TAG_SETTING = Setting.simpleString(
        LanceEngineFactory.TAG_SETTING,
        "",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    /**
     * JSON stringified multi-fields spec, persisted by attach so the
     * engine can rehydrate keyword sub-fields on shard open. Empty
     * means no sub-fields declared.
     */
    public static final Setting<String> MULTI_FIELDS_SETTING = Setting.simpleString(
        LanceEngineFactory.MULTI_FIELDS_SETTING,
        "",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    public static final Setting<String> UNCOVERED_FRAGMENT_POLICY_SETTING = Setting.simpleString(
        "index.lance.uncovered_fragment_policy",
        "immediate",
        LancePlugin::validateUncoveredFragmentPolicy,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );
    public static final Setting<TimeValue> NAMESPACE_POLL_CADENCE_SETTING = Setting.timeSetting(
        "lance.namespace.poll_cadence",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );
    /**
     * How long a Lance-backed index that got deleted from OpenSearch
     * (through {@code DELETE /{index}}) is held in the namespace poll's
     * tombstone list so a subsequent poll cycle does not immediately
     * recreate it. Zero disables the guard (poll re-surfaces
     * immediately). Applies only to
     * indexes that were surfaced or attached by this plugin; ordinary
     * OpenSearch indexes are never in the tombstone list.
     */
    public static final Setting<TimeValue> NAMESPACE_RESURFACE_GRACE_SETTING = Setting.timeSetting(
        "lance.namespace.resurface_guard_grace",
        TimeValue.timeValueHours(1),
        TimeValue.timeValueMillis(0),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Long> BUILDER_MAX_ROWS_SETTING = Setting.longSetting(
        "lance.builder.max_rows",
        1_000_000L,
        1L,
        Setting.Property.NodeScope
    );
    public static final Setting<List<String>> ALLOWED_TABLE_ROOTS_SETTING = Setting.listSetting(
        "lance.allowed_table_roots",
        List.of(),
        java.util.function.Function.identity(),
        Setting.Property.NodeScope
    );
    public static final Setting<Settings> STORAGE_OPTIONS_SETTING = Setting.groupSetting(
        StorageOptions.INDEX_SETTING_PREFIX,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Node-scoped upper bound on the memory that Lance's shared
     * {@link org.lance.Session} may consume for its index and metadata
     * caches. Accepts either an absolute {@link org.opensearch.core.common.unit.ByteSizeValue}
     * (for example {@code "10gb"}) or a percentage of the memory left on
     * the host once the JVM heap is subtracted (for example {@code "40%"}).
     *
     * <p>The default of {@code "40%"} lets Lance scale with the node's
     * physical memory rather than a fixed byte count, and leaves room
     * for the k-NN plugin's own {@code knn.memory.circuit_breaker.limit}
     * (default {@code "50%"}) on nodes that host both plugins. Layer 2
     * of the native-memory design will make this dynamic; for now the
     * setting is node-scoped only, so a change requires a rolling
     * restart to take effect.
     */
    public static final Setting<String> NATIVE_MEMORY_LIMIT_SETTING = Setting.simpleString(
        "lance.native_memory.limit",
        "40%",
        LancePlugin::validateNativeMemoryLimit,
        Setting.Property.NodeScope
    );

    /**
     * Toggles the circuit breaker that rejects FTS and knn queries when
     * Lance's shared {@link org.lance.Session} caches have caught up to
     * the limit configured by {@link #NATIVE_MEMORY_LIMIT_SETTING}. Left
     * on by default; operators may temporarily disable it if the check
     * itself gets in the way of an investigation. The breaker's byte
     * limit is not configurable through this setting; it always mirrors
     * the Session cache limit so operators have one number to reason
     * about.
     */
    public static final Setting<Boolean> NATIVE_MEMORY_CB_ENABLED_SETTING = Setting.boolSetting(
        "lance.native_memory.circuit_breaker.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How often the plugin samples {@link org.lance.Session#sizeBytes()}
     * and forwards the reading to the {@code lance_native} circuit
     * breaker's accounting. Kept intentionally short (five seconds by
     * default) because a single 100M-row FTS query can grow the cache
     * by several GiB, and a slower cadence would let the breaker lag
     * far behind the real footprint. Node scoped and dynamic so
     * operators can tune it without a restart.
     */
    public static final Setting<TimeValue> NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING = Setting.timeSetting(
        "lance.native_memory.circuit_breaker.poll_interval",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Cap on how many fragment path queries this node executes in
     * parallel. Fragment path processes every fragment of an index on
     * one node, so per-query heap (FTS score arrays sized by
     * {@code maxDoc}, aggregation buffers) scales with the number of
     * concurrent requests rather than with cluster fan-out. The
     * {@code lance_native} circuit breaker still catches individual
     * runaway queries, but at high concurrency allocation races the
     * breaker and the node can drop into {@code OutOfMemoryError}
     * before the breaker fires; a bounded semaphore backstops that
     * race by serialising the tail once the limit is reached.
     *
     * <p>Default {@code 4} is chosen so that fragment path stays
     * comfortably below the search threadpool size (which is
     * {@code (allocated_processors * 3) / 2 + 1}) on typical
     * hardware, and matches the concurrency level at which the
     * evaluation observed the parent circuit breaker successfully
     * rejecting overflow with 429 rather than the JVM dying. Node
     * scoped and static: changing the value requires a restart
     * because the underlying semaphore's permit count is fixed at
     * plugin init.
     */
    public static final Setting<Integer> FRAGMENT_DISPATCH_MAX_CONCURRENT_SETTING = Setting.intSetting(
        "lance.fragment_dispatch.max_concurrent",
        4,
        1,
        128,
        Setting.Property.NodeScope
    );

    /**
     * Whether the fragment path keeps a node scoped snapshot of each
     * Lance table version it has served (open dataset, fragment metadata,
     * schema) and an off-heap cache of the numeric and boolean columns it
     * has read, so a second request against the same version opens no
     * dataset and scans no column it already holds. Dynamic: turning it
     * off retires every snapshot at once and later requests open the
     * table per request as before.
     */
    public static final Setting<Boolean> CACHE_ENABLED_SETTING = Setting.boolSetting(
        "lance.cache.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How many table snapshots {@link org.opensearch.lance.engine.LanceWarmCache}
     * keeps before it closes the least recently used one that no request
     * holds. Each snapshot is one open Lance dataset plus a few kilobytes
     * of metadata per fragment. Static, node scope.
     */
    public static final Setting<Integer> CACHE_MAX_SNAPSHOTS_SETTING = Setting.intSetting(
        "lance.cache.max_snapshots",
        64,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Fraction of {@link #NATIVE_MEMORY_LIMIT_SETTING} reserved for the
     * off-heap column cache. The remainder goes to the Lance Session's
     * index and metadata caches in their 6:1 ratio. Static, node scope.
     */
    public static final Setting<Double> CACHE_COLUMN_SHARE_SETTING = Setting.doubleSetting(
        "lance.cache.column_share",
        0.4,
        0.0,
        0.95,
        Setting.Property.NodeScope
    );

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            TABLE_SETTING,
            PRIMARY_KEY_FIELD_SETTING,
            PRIMARY_KEY_TYPE_SETTING,
            VERSION_SETTING,
            TAG_SETTING,
            MULTI_FIELDS_SETTING,
            UNCOVERED_FRAGMENT_POLICY_SETTING,
            NAMESPACE_POLL_CADENCE_SETTING,
            NAMESPACE_RESURFACE_GRACE_SETTING,
            BUILDER_MAX_ROWS_SETTING,
            ALLOWED_TABLE_ROOTS_SETTING,
            STORAGE_OPTIONS_SETTING,
            NATIVE_MEMORY_LIMIT_SETTING,
            NATIVE_MEMORY_CB_ENABLED_SETTING,
            NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING,
            FRAGMENT_DISPATCH_MAX_CONCURRENT_SETTING,
            CACHE_ENABLED_SETTING,
            CACHE_MAX_SNAPSHOTS_SETTING,
            CACHE_COLUMN_SHARE_SETTING
        );
    }

    private static void validateUncoveredFragmentPolicy(String value) {
        if (!"wait".equals(value) && !"immediate".equals(value)) {
            throw new IllegalArgumentException("index.lance.uncovered_fragment_policy must be 'wait' or 'immediate', got '" + value + "'");
        }
    }

    private static void validatePrimaryKeyType(String value) {
        // Empty is accepted so the setting can be omitted on indices that
        // do not declare a primary key (the runtime path treats the PK
        // field name as the source of truth for "PK present"). Otherwise
        // restrict to the two enum-mapped forms so a typo like "keywords"
        // fails at CreateIndex time rather than silently falling back to
        // long.
        if (value == null || value.isEmpty()) {
            return;
        }
        if (!"long".equals(value) && !"keyword".equals(value) && !"unsigned_long".equals(value) && !"none".equals(value)) {
            throw new IllegalArgumentException(
                "index.lance.primary_key_type must be 'long', 'unsigned_long', or 'keyword', got '" + value + "'"
            );
        }
    }

    private static void validateNativeMemoryLimit(String value) {
        // Delegate to the parser so validation and resolution stay in
        // one place. The parser throws OpenSearchParseException /
        // IllegalArgumentException on malformed input, which
        // Setting.simpleString surfaces back to the operator as a
        // 400-style validation error.
        NativeMemoryLimit.parse(value, "lance.native_memory.limit");
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (indexSettings.getSettings().get(LanceEngineFactory.TABLE_SETTING) != null) {
            return Optional.of(new LanceEngineFactory());
        }
        return Optional.empty();
    }

    /**
     * No plugin-level index events are wired. Lance 12 keys its
     * index-metadata cache on the manifest ETag
     * (<a href="https://github.com/lancedb/lance/pull/8904">lance#8904</a>),
     * so a table recreated at the same path gets a fresh cache slot on
     * first access without a DELETE-time invalidation from the plugin;
     * {@code LanceAttachIT#testAttachRecreateAtSamePathServesNewContent}
     * covers that. The override is kept as the wiring point for any
     * future per-index hook (warm cache, per-index breaker).
     */
    @Override
    public void onIndexModule(org.opensearch.index.IndexModule indexModule) {}

    private LanceNamespaceService namespaceService;
    private org.opensearch.threadpool.ThreadPool threadPool;
    private AllowedTableRoots allowedTableRoots;
    private LanceDispatchActionFilter dispatchActionFilter;
    private LanceCreateIndexActionFilter createIndexActionFilter;
    private volatile LanceWarmCache warmCache;

    /**
     * Cancellable handle for the scheduled task that samples the shared
     * Lance Session and updates the {@code lance_native} circuit breaker's
     * accounting. Held so {@link #close()} can stop the task, and so the
     * settings-change listener can restart it with a new poll interval.
     */
    private volatile Cancellable circuitBreakerPollTask;

    /**
     * Current poll interval used by the scheduled task above. Kept
     * separately from the setting so the listener can compare and
     * avoid restarting the task when an unrelated cluster setting
     * update fires.
     */
    private volatile TimeValue circuitBreakerPollInterval;

    @Override
    public BreakerSettings getCircuitBreaker(Settings settings) {
        // Register a plugin-owned breaker keyed on {@link
        // LanceCircuitBreaker#NAME}. The byte limit mirrors
        // lance.native_memory.limit so operators have one number to
        // configure, and the overhead is 1.0 because the accounting we
        // push in from the polling loop is already actual usage, not
        // an estimate that needs scaling. TRANSIENT durability tells
        // OpenSearch that the condition is expected to resolve without
        // operator intervention (LRU eviction or another polling
        // cycle), which surfaces as a 429 response category rather
        // than a stuck cluster-level error.
        String rawLimit = NATIVE_MEMORY_LIMIT_SETTING.get(settings);
        long limitBytes = NativeMemoryLimit.parse(rawLimit, NATIVE_MEMORY_LIMIT_SETTING.getKey());
        return new BreakerSettings(
            LanceCircuitBreaker.NAME,
            limitBytes,
            1.0,
            CircuitBreaker.Type.MEMORY,
            CircuitBreaker.Durability.TRANSIENT
        );
    }

    @Override
    public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
        // OpenSearch calls this once at node startup with the breaker
        // it built from getCircuitBreaker's BreakerSettings. Hand the
        // reference to the static helper so the FTS / knn scorers can
        // reach it from query paths that only see a QueryShardContext.
        LanceCircuitBreaker.setBreaker(circuitBreaker);
    }

    @Override
    public java.util.Collection<Object> createComponents(
        org.opensearch.transport.client.Client client,
        org.opensearch.cluster.service.ClusterService clusterService,
        org.opensearch.threadpool.ThreadPool threadPool,
        org.opensearch.watcher.ResourceWatcherService resourceWatcherService,
        org.opensearch.script.ScriptService scriptService,
        org.opensearch.core.xcontent.NamedXContentRegistry xContentRegistry,
        org.opensearch.env.Environment environment,
        org.opensearch.env.NodeEnvironment nodeEnvironment,
        org.opensearch.core.common.io.stream.NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<org.opensearch.repositories.RepositoriesService> repositoriesServiceSupplier
    ) {
        this.threadPool = threadPool;
        TimeValue cadence = NAMESPACE_POLL_CADENCE_SETTING.get(environment.settings());
        long builderMaxRows = BUILDER_MAX_ROWS_SETTING.get(environment.settings());
        this.allowedTableRoots = new AllowedTableRoots(ALLOWED_TABLE_ROOTS_SETTING.get(environment.settings()));

        // Install the node-scoped Lance Session before any Dataset is
        // opened. LanceEngineFactory and the REST attach / namespace
        // handlers all route their Dataset.open calls through
        // LanceRegistry.openDataset, so once the Session is set here
        // every shard on the node will share its index and metadata
        // caches instead of each shard allocating its own 6 GiB / 1 GiB
        // budget out of native memory. The column cache takes its share
        // of the same limit first; the Session gets the rest.
        String rawLimit = NATIVE_MEMORY_LIMIT_SETTING.get(environment.settings());
        long totalBytes = NativeMemoryLimit.parse(rawLimit, NATIVE_MEMORY_LIMIT_SETTING.getKey());
        double columnShare = CACHE_COLUMN_SHARE_SETTING.get(environment.settings());
        long columnCacheBytes = NativeMemoryLimit.columnCacheBytes(totalBytes, columnShare);
        long sessionBytes = NativeMemoryLimit.sessionCacheBytes(totalBytes, columnShare);
        long indexCacheBytes = NativeMemoryLimit.indexCacheBytes(sessionBytes);
        long metadataCacheBytes = NativeMemoryLimit.metadataCacheBytes(sessionBytes);
        LanceRegistry.initSession(indexCacheBytes, metadataCacheBytes);
        LOGGER.info(
            "installed shared Lance Session: limit [{}] -> index cache [{}], metadata cache [{}], column cache [{}] "
                + "(from lance.native_memory.limit [{}], lance.cache.column_share [{}])",
            NativeMemoryLimit.humanReadable(totalBytes),
            NativeMemoryLimit.humanReadable(indexCacheBytes),
            NativeMemoryLimit.humanReadable(metadataCacheBytes),
            NativeMemoryLimit.humanReadable(columnCacheBytes),
            rawLimit,
            columnShare
        );

        // Node scoped snapshot and column cache for the fragment path.
        // Created before the transport actions so Guice can inject it
        // into TransportLanceFragmentQueryAction.
        this.warmCache = new LanceWarmCache(
            LanceRegistry.allocator(),
            columnCacheBytes,
            CACHE_MAX_SNAPSHOTS_SETTING.get(environment.settings()),
            CACHE_ENABLED_SETTING.get(environment.settings())
        );
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CACHE_ENABLED_SETTING, warmCache::setEnabled);

        // Prime the circuit-breaker helper with the current cluster
        // settings and start the polling loop that keeps its accounting
        // aligned with Session.sizeBytes() plus the column cache. The
        // listener below picks up dynamic changes to both the enabled
        // flag and the poll cadence; the breaker itself has already been
        // handed to LanceCircuitBreaker by setCircuitBreaker earlier in
        // the node lifecycle.
        LanceCircuitBreaker.setEnabled(NATIVE_MEMORY_CB_ENABLED_SETTING.get(environment.settings()));
        this.circuitBreakerPollInterval = NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING.get(environment.settings());
        this.circuitBreakerPollTask = scheduleCircuitBreakerPoll(threadPool, circuitBreakerPollInterval);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(NATIVE_MEMORY_CB_ENABLED_SETTING, LanceCircuitBreaker::setEnabled);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING, this::updatePollInterval);

        // Register the shard-free dispatch ActionFilter. It
        // intercepts every _search request against Lance-backed
        // indices, delegating to the plugin's own coordinator; the
        // shard fan-out via ReadOnlyEngine only runs when the
        // fragment executor cannot answer a shape yet (from > 0,
        // search_after, highlighter, suggest, post_filter).
        this.dispatchActionFilter = new LanceDispatchActionFilter(clusterService, indexNameExpressionResolver, client, threadPool);
        this.createIndexActionFilter = new LanceCreateIndexActionFilter(threadPool);

        namespaceService = new LanceNamespaceService(
            client,
            clusterService,
            threadPool,
            cadence,
            builderMaxRows,
            NAMESPACE_RESURFACE_GRACE_SETTING.get(environment.settings()),
            warmCache
        );
        // Register a reactive consumer so an operator can adjust the grace
        // period at runtime without a rolling restart.
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(NAMESPACE_RESURFACE_GRACE_SETTING, namespaceService::setResurfaceGrace);
        // The components are injected into the plugin's transport
        // actions (attach, build_indexes, namespace list / update,
        // fragment query).
        return List.of(namespaceService, allowedTableRoots, warmCache);
    }

    /**
     * Schedule the periodic sampler that reads the current
     * {@code Session.sizeBytes()} and the column cache's allocated bytes
     * and pushes their sum into the circuit breaker via
     * {@link LanceCircuitBreaker#updateUsage(long, long)}. Runs on the
     * generic thread pool so it does not steal capacity from the search
     * or write executors.
     */
    private Cancellable scheduleCircuitBreakerPoll(ThreadPool pool, TimeValue interval) {
        Runnable sampler = () -> {
            try {
                org.lance.Session session = LanceRegistry.currentSession();
                if (session == null || session.isClosed()) {
                    return;
                }
                long sessionBytes = session.sizeBytes();
                LanceWarmCache cache = warmCache;
                long columnBytes = cache == null ? 0L : cache.columnCacheBytes();
                LanceCircuitBreaker.updateUsage(sessionBytes, columnBytes);
            } catch (Throwable t) {
                // Never let a poll iteration throw out of the
                // scheduler; a failed reading just means the breaker's
                // accounting stays as it was for one more cycle.
                LOGGER.warn("lance_native circuit breaker poll iteration failed", t);
            }
        };
        return pool.scheduleWithFixedDelay(sampler, interval, ThreadPool.Names.GENERIC);
    }

    private synchronized void updatePollInterval(TimeValue newInterval) {
        if (newInterval.equals(circuitBreakerPollInterval)) {
            return;
        }
        if (circuitBreakerPollTask != null) {
            circuitBreakerPollTask.cancel();
        }
        circuitBreakerPollInterval = newInterval;
        circuitBreakerPollTask = scheduleCircuitBreakerPoll(threadPool, newInterval);
        LOGGER.info("lance_native circuit breaker poll interval updated to [{}]", newInterval);
    }

    @Override
    public void close() throws IOException {
        // Cancel the polling loop before releasing the Session so the
        // sampler can never observe a half-closed Session on its way
        // out.
        Cancellable task = circuitBreakerPollTask;
        if (task != null) {
            task.cancel();
            circuitBreakerPollTask = null;
        }
        // Close every cached snapshot (their datasets) and the column
        // cache allocator before the Session goes away.
        LanceWarmCache cache = warmCache;
        if (cache != null) {
            cache.close();
            warmCache = null;
        }
        // Release the shared native Session so a test-framework restart
        // within the same JVM doesn't accumulate stale Session handles.
        // Existing Dataset handles keep their own Arc reference to the
        // underlying native session, so this call is safe even if some
        // shards are still open at the moment of shutdown.
        LanceRegistry.closeSession();
        super.close();
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        // The filter is created lazily in createComponents, so return
        // an empty list until then. In practice OpenSearch calls
        // createComponents before it consults getActionFilters, so the
        // filter is always present when the search machinery starts
        // routing through it; the null guard exists purely for the
        // test framework's out-of-order invocations.
        LanceDispatchActionFilter dispatch = dispatchActionFilter;
        LanceCreateIndexActionFilter guard = createIndexActionFilter;
        if (dispatch == null && guard == null) {
            return List.of();
        }
        if (guard == null) {
            return List.of(dispatch);
        }
        if (dispatch == null) {
            return List.of(guard);
        }
        return List.of(dispatch, guard);
    }

    @Override
    public
        List<
            org.opensearch.plugins.ActionPlugin.ActionHandler<
                ? extends org.opensearch.action.ActionRequest,
                ? extends org.opensearch.core.action.ActionResponse>>
        getActions() {
        return List.of(
            new org.opensearch.plugins.ActionPlugin.ActionHandler<>(
                org.opensearch.lance.dispatch.LanceFragmentQueryAction.INSTANCE,
                org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction.class
            ),
            new org.opensearch.plugins.ActionPlugin.ActionHandler<>(
                org.opensearch.lance.dispatch.LanceCoordinatorAction.INSTANCE,
                org.opensearch.lance.dispatch.TransportLanceCoordinatorAction.class
            ),
            new org.opensearch.plugins.ActionPlugin.ActionHandler<>(
                org.opensearch.lance.namespace.LanceNamespaceUpdateAction.INSTANCE,
                org.opensearch.lance.namespace.TransportLanceNamespaceUpdateAction.class
            ),
            new ActionHandler<>(LanceNamespaceListAction.INSTANCE, TransportLanceNamespaceListAction.class),
            new ActionHandler<>(LanceAttachAction.INSTANCE, TransportLanceAttachAction.class),
            new ActionHandler<>(LanceBuildIndexesAction.INSTANCE, TransportLanceBuildIndexesAction.class),
            new ActionHandler<>(LanceRefsAction.INSTANCE, TransportLanceRefsAction.class)
        );
    }

    @Override
    public List<org.opensearch.core.common.io.stream.NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new org.opensearch.core.common.io.stream.NamedWriteableRegistry.Entry(
                org.opensearch.cluster.metadata.Metadata.Custom.class,
                org.opensearch.lance.namespace.LanceNamespaceMetadata.TYPE,
                org.opensearch.lance.namespace.LanceNamespaceMetadata::new
            ),
            new org.opensearch.core.common.io.stream.NamedWriteableRegistry.Entry(
                org.opensearch.cluster.NamedDiff.class,
                org.opensearch.lance.namespace.LanceNamespaceMetadata.TYPE,
                org.opensearch.lance.namespace.LanceNamespaceMetadata::readDiffFrom
            )
        );
    }

    @Override
    public List<org.opensearch.core.xcontent.NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new org.opensearch.core.xcontent.NamedXContentRegistry.Entry(
                org.opensearch.cluster.metadata.Metadata.Custom.class,
                new org.opensearch.core.ParseField(org.opensearch.lance.namespace.LanceNamespaceMetadata.TYPE),
                org.opensearch.lance.namespace.LanceNamespaceMetadata::fromXContent
            )
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestAttachAction(), new RestNamespaceAction(), new RestBuildIndexesAction(), new RestRefsAction());
    }
}
