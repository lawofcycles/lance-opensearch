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
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.mapper.LanceTextFieldMapper;
import org.opensearch.lance.mapper.LanceVectorFieldMapper;
import org.opensearch.lance.namespace.AllowedTableRoots;
import org.opensearch.lance.namespace.LanceNamespaceService;
import org.opensearch.lance.query.LanceFtsBoolQueryBuilder;
import org.opensearch.lance.query.LanceFtsBoostQueryBuilder;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchPhraseQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.lance.query.LanceMultiMatchQueryBuilder;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.lance.rest.RestBuildIndexesAction;
import org.opensearch.lance.rest.RestNamespaceAction;
import org.opensearch.plugins.ActionPlugin;
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
    public static final Setting<String> UNCOVERED_FRAGMENT_POLICY_SETTING = Setting.simpleString(
        "index.lance.uncovered_fragment_policy",
        "wait",
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
     * about. Independent headroom is deferred to a follow-up.
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

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            TABLE_SETTING,
            PRIMARY_KEY_FIELD_SETTING,
            UNCOVERED_FRAGMENT_POLICY_SETTING,
            NAMESPACE_POLL_CADENCE_SETTING,
            BUILDER_MAX_ROWS_SETTING,
            ALLOWED_TABLE_ROOTS_SETTING,
            STORAGE_OPTIONS_SETTING,
            NATIVE_MEMORY_LIMIT_SETTING,
            NATIVE_MEMORY_CB_ENABLED_SETTING,
            NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING
        );
    }

    private static void validateUncoveredFragmentPolicy(String value) {
        if (!"wait".equals(value) && !"immediate".equals(value)) {
            throw new IllegalArgumentException("index.lance.uncovered_fragment_policy must be 'wait' or 'immediate', got '" + value + "'");
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

    private LanceNamespaceService namespaceService;
    private org.opensearch.threadpool.ThreadPool threadPool;
    private AllowedTableRoots allowedTableRoots;

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
        // budget out of native memory.
        String rawLimit = NATIVE_MEMORY_LIMIT_SETTING.get(environment.settings());
        long totalBytes = NativeMemoryLimit.parse(rawLimit, NATIVE_MEMORY_LIMIT_SETTING.getKey());
        long indexCacheBytes = NativeMemoryLimit.indexCacheBytes(totalBytes);
        long metadataCacheBytes = NativeMemoryLimit.metadataCacheBytes(totalBytes);
        LanceRegistry.initSession(indexCacheBytes, metadataCacheBytes);
        LOGGER.info(
            "installed shared Lance Session: limit [{}] -> index cache [{}], metadata cache [{}] (from lance.native_memory.limit [{}])",
            NativeMemoryLimit.humanReadable(totalBytes),
            NativeMemoryLimit.humanReadable(indexCacheBytes),
            NativeMemoryLimit.humanReadable(metadataCacheBytes),
            rawLimit
        );

        // Prime the circuit-breaker helper with the current cluster
        // settings and start the polling loop that keeps its accounting
        // aligned with Session.sizeBytes(). The listener below picks up
        // dynamic changes to both the enabled flag and the poll
        // cadence; the breaker itself has already been handed to
        // LanceCircuitBreaker by setCircuitBreaker earlier in the node
        // lifecycle.
        LanceCircuitBreaker.setEnabled(NATIVE_MEMORY_CB_ENABLED_SETTING.get(environment.settings()));
        this.circuitBreakerPollInterval = NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING.get(environment.settings());
        this.circuitBreakerPollTask = scheduleCircuitBreakerPoll(threadPool, circuitBreakerPollInterval);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(NATIVE_MEMORY_CB_ENABLED_SETTING, LanceCircuitBreaker::setEnabled);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING, this::updatePollInterval);

        namespaceService = new LanceNamespaceService(client, threadPool, cadence, builderMaxRows);
        return List.of(namespaceService);
    }

    /**
     * Schedule the periodic sampler that reads the current
     * {@code Session.sizeBytes()} and pushes the reading into the
     * circuit breaker via {@link LanceCircuitBreaker#updateUsage(long)}.
     * Runs on the generic thread pool so it does not steal capacity
     * from the search or write executors.
     */
    private Cancellable scheduleCircuitBreakerPoll(ThreadPool pool, TimeValue interval) {
        Runnable sampler = () -> {
            try {
                org.lance.Session session = LanceRegistry.currentSession();
                if (session == null || session.isClosed()) {
                    return;
                }
                long bytes = session.sizeBytes();
                LanceCircuitBreaker.updateUsage(bytes);
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
        // Release the shared native Session so a test-framework restart
        // within the same JVM doesn't accumulate stale Session handles.
        // Existing Dataset handles keep their own Arc reference to the
        // underlying native session, so this call is safe even if some
        // shards are still open at the moment of shutdown.
        LanceRegistry.closeSession();
        super.close();
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
        return List.of(
            new RestAttachAction(threadPool, allowedTableRoots, namespaceService),
            new RestNamespaceAction(namespaceService, allowedTableRoots),
            new RestBuildIndexesAction(threadPool)
        );
    }
}
