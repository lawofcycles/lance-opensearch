/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.lance.Session;
import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.indices.breaker.BreakerSettings;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.TransportLanceAttachAction;
import org.opensearch.lance.execute.LanceAggregateResults;
import org.opensearch.lance.dispatch.LanceClearCacheActionFilter;
import org.opensearch.lance.dispatch.LanceCoordinatorAction;
import org.opensearch.lance.dispatch.LanceFragmentQueryAction;
import org.opensearch.lance.dispatch.TransportLanceCoordinatorAction;
import org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction;
import org.opensearch.lance.dispatch.LanceDispatchActionFilter;
import org.opensearch.lance.dispatch.LanceGetIndexActionFilter;
import org.opensearch.lance.dispatch.LanceCreateIndexActionFilter;
import org.opensearch.lance.dispatch.LanceRequestCache;
import org.opensearch.lance.dispatch.LanceRequestCacheClearAction;
import org.opensearch.lance.dispatch.LanceFragmentFetchAction;
import org.opensearch.lance.dispatch.LanceStatisticsPrefetchAction;
import org.opensearch.lance.dispatch.TransportLanceRequestCacheClearAction;
import org.opensearch.lance.dispatch.TransportLanceFragmentFetchAction;
import org.opensearch.lance.dispatch.TransportLanceStatisticsPrefetchAction;
import org.opensearch.lance.engine.HidingReaderWrapper;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceIndexWarmer;
import org.opensearch.lance.engine.LanceServedVersions;
import org.opensearch.lance.engine.LanceFetchCache;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.mapper.LanceTextFieldMapper;
import org.opensearch.lance.mapper.LanceVectorFieldMapper;
import org.opensearch.lance.namespace.AllowedCatalogEndpoints;
import org.opensearch.lance.namespace.AllowedTableRoots;
import org.opensearch.lance.namespace.LanceIndexFreshnessService;
import org.opensearch.lance.namespace.LanceIndexSyncAction;
import org.opensearch.lance.namespace.LanceNamespaceListAction;
import org.opensearch.lance.namespace.LanceNamespacePollAction;
import org.opensearch.lance.namespace.LanceNamespaceMetadata;
import org.opensearch.lance.namespace.LanceNamespaceService;
import org.opensearch.lance.namespace.LanceNamespaceUpdateAction;
import org.opensearch.lance.namespace.TransportLanceNamespaceUpdateAction;
import org.opensearch.lance.namespace.TransportLanceIndexSyncAction;
import org.opensearch.lance.namespace.TransportLanceNamespaceListAction;
import org.opensearch.lance.namespace.TransportLanceNamespacePollAction;
import org.opensearch.lance.plan.explain.LanceExplainAction;
import org.opensearch.lance.plan.explain.TransportLanceExplainAction;
import org.opensearch.lance.query.ScanAdmission;
import org.opensearch.lance.query.LanceFtsBoolQueryBuilder;
import org.opensearch.lance.query.LanceFtsBoostQueryBuilder;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchPhraseQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.lance.query.LanceMultiMatchQueryBuilder;
import org.opensearch.lance.refs.LanceRefsAction;
import org.opensearch.lance.refs.TransportLanceRefsAction;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.lance.rest.RestLanceExplainAction;
import org.opensearch.lance.rest.RestNamespaceAction;
import org.opensearch.lance.rest.RestLanceStatsAction;
import org.opensearch.lance.rest.RestLanceSyncAction;
import org.opensearch.lance.rest.RestRefsAction;
import org.opensearch.lance.stats.LanceStatsAction;
import org.opensearch.lance.stats.LanceStatsCollector;
import org.opensearch.lance.stats.TransportLanceStatsAction;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.CircuitBreakerPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

/**
 * Plugin entry point. Registers the reader-side surface for Lance tables:
 * the REST endpoints for namespace / attach, the
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
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(
            LanceTextFieldMapper.CONTENT_TYPE,
            LanceTextFieldMapper.PARSER,
            LanceVectorFieldMapper.CONTENT_TYPE,
            LanceVectorFieldMapper.PARSER
        );
    }

    /** The plugin's settings: {@link LanceSettings#all()}. */
    @Override
    public List<Setting<?>> getSettings() {
        return LanceSettings.all();
    }

    /**
     * The credential entries of {@code index.plugins.lance.storage_options.*}
     * are withheld from {@code GET /<index>/_settings} and the cluster
     * state API through OpenSearch's settings filter. The patterns come
     * from {@link StorageOptions#SENSITIVE_INDEX_SETTING_PATTERNS}, so
     * the keys the filter hides are the ones the namespace listing
     * redacts. Region, endpoint and {@code allow_http} stay visible. The
     * filter acts on the API output only: the shard reads the values
     * from the cluster state as before, and a snapshot's index metadata
     * still carries them. {@code GET /<index>} is covered by
     * {@link LanceGetIndexActionFilter}, because core applies this
     * filter to that API's defaults block only.
     */
    @Override
    public List<String> getSettingsFilter() {
        return StorageOptions.SENSITIVE_INDEX_SETTING_PATTERNS;
    }

    /**
     * Name of the thread pool the coordinator side of the fragment path
     * runs on: the entry of every {@code _search} against a Lance-backed
     * index (resolving the request, enumerating fragments, sending the
     * per-node requests) and the merge of the per-node responses (hit
     * sort merge, aggregation reduce). The per-node fragment executors
     * stay on the {@code search} pool. Keeping the two apart means a
     * burst of coordinator work cannot fill the {@code search} queue of
     * a data node, and a full {@code search} queue cannot make the
     * transport layer drop a fragment response.
     */
    public static final String LANCE_COORDINATOR_THREAD_POOL = "lance_coordinator";

    /**
     * Default queue length of {@link #LANCE_COORDINATOR_THREAD_POOL}.
     * Bounded, so a coordinator that cannot keep up rejects requests
     * with 429 instead of queueing them without limit; large enough that
     * the bound is only reached under a sustained overload.
     */
    static final int LANCE_COORDINATOR_QUEUE_SIZE = 10_000;

    /**
     * Register {@link #LANCE_COORDINATOR_THREAD_POOL} as a fixed pool of
     * {@code max(1, allocated processors / 2)} threads, and
     * {@link LanceIndexWarmer#THREAD_POOL} as a fixed pool of one thread
     * with a queue of {@link #LANCE_WARM_UP_QUEUE_SIZE}: one table warms
     * at a time so the warm-ups do not compete with each other or with
     * requests for the object store, and the queue holds the tables that
     * appeared while one was warming. The sizes and the queue lengths
     * are node settings under {@code thread_pool.<name>.*} like every
     * other pool; OpenSearch registers them from these builders, so they
     * are not part of {@link #getSettings()}.
     */
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        int size = Math.max(1, OpenSearchExecutors.allocatedProcessors(settings) / 2);
        return List.of(
            new FixedExecutorBuilder(
                settings,
                LANCE_COORDINATOR_THREAD_POOL,
                size,
                LANCE_COORDINATOR_QUEUE_SIZE,
                "thread_pool." + LANCE_COORDINATOR_THREAD_POOL
            ),
            new FixedExecutorBuilder(
                settings,
                LanceIndexWarmer.THREAD_POOL,
                1,
                LANCE_WARM_UP_QUEUE_SIZE,
                "thread_pool." + LanceIndexWarmer.THREAD_POOL
            )
        );
    }

    /** Default queue length of {@link LanceIndexWarmer#THREAD_POOL}: tables waiting for their warm-up. */
    static final int LANCE_WARM_UP_QUEUE_SIZE = 1_000;

    /**
     * Lance-backed indexes get the read-only engine over the node's
     * {@link LanceWarmCache}, so the shard's reader and the fragment path
     * share one snapshot per table version. Index services are created
     * after {@link #createComponents} has run and after Guice bound the
     * node's {@code IndicesService} into {@link #indicesServiceHolder}, so
     * both are present; a {@code null} cache here (the factory asked for
     * before the components exist, as a test harness may do) makes the
     * engine open its own dataset per reader instead, and a {@code null}
     * indices service makes a GET outside the shard reader refuse.
     */
    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (LanceEngineFactory.isLanceIndex(indexSettings.getSettings())) {
            return Optional.of(new LanceEngineFactory(warmCache, () -> maxDocsPerReader, servedVersions, indicesServiceHolder.get()));
        }
        return Optional.empty();
    }

    /**
     * {@link IndicesServiceHolder.Binder}, which Guice constructs with the
     * node's {@code IndicesService} and {@link #indicesServiceHolder}
     * (returned from {@link #createComponents}, so bound), before the node
     * starts.
     */
    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return List.of(IndicesServiceHolder.Binder.class);
    }

    /**
     * Every Lance-backed index gets the node's
     * {@link LanceIndexFreshnessService} as a shard lifecycle listener:
     * a started shard registers for freshness checks on this node and a
     * closing shard unregisters. No DELETE-time cache invalidation is
     * wired: Lance 12 keys its index-metadata cache on the manifest ETag
     * (<a href="https://github.com/lancedb/lance/pull/8904">lance#8904</a>),
     * so a table recreated at the same path gets a fresh cache slot on
     * first access; {@code LanceAttachIT#testAttachRecreateAtSamePathServesNewContent}
     * covers that. The service is {@code null} only for an index module
     * built before the components exist (a test harness).
     *
     * <p>While {@link LanceSettings#TEST_HIDING_WRAPPER_INDEX_PREFIX_SETTING} is set,
     * a Lance backed index whose name starts with its prefix also gets
     * the {@link HidingReaderWrapper} the value describes as its reader
     * wrapper. The hook runs for the node's own index service and for the
     * temporary one the fragment executor and the probes build, so both
     * see the same wrapper, as they do the security plugin's.
     */
    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (!LanceEngineFactory.isLanceIndex(indexModule.getSettings())) {
            return;
        }
        LanceIndexFreshnessService freshness = freshnessService;
        if (freshness != null) {
            indexModule.addIndexEventListener(freshness);
        }
        ClusterService cluster = clusterService;
        if (cluster != null) {
            HidingReaderWrapper.Rule rule = HidingReaderWrapper.Rule.parse(
                cluster.getClusterSettings().get(LanceSettings.TEST_HIDING_WRAPPER_INDEX_PREFIX_SETTING)
            );
            if (rule != null && rule.appliesTo(indexModule.getIndex().getName())) {
                indexModule.setReaderWrapper(indexService -> new HidingReaderWrapper(rule));
            }
        }
    }

    private LanceNamespaceService namespaceService;
    private volatile LanceIndexFreshnessService freshnessService;
    /** Served manifest version of every open Lance engine on this node, published by the engines. */
    private final LanceServedVersions servedVersions = new LanceServedVersions();
    /** The node's {@code IndicesService}, bound by Guice through {@link IndicesServiceHolder.Binder}; the engine factories read it. */
    private final IndicesServiceHolder indicesServiceHolder = new IndicesServiceHolder();
    /** The node's cluster service, kept so {@link #close} can take the listeners registered in createComponents off it. */
    private volatile ClusterService clusterService;
    private AllowedTableRoots allowedTableRoots;
    private AllowedCatalogEndpoints allowedCatalogEndpoints;
    private LanceDispatchActionFilter dispatchActionFilter;
    private LanceCreateIndexActionFilter createIndexActionFilter;
    private LanceClearCacheActionFilter clearCacheActionFilter;
    private volatile LanceRequestCache requestCache;
    private volatile LanceFetchCache fetchCache;
    private volatile LanceWarmCache warmCache;
    private volatile LanceIndexWarmer indexWarmer;
    /** The loop that keeps the {@code lance_native} breaker's accounting aligned with the native caches; stopped by {@link #close}. */
    private volatile LanceCircuitBreaker.Sampler circuitBreakerSampler;
    /**
     * Current {@link LanceSettings#MAX_DOCS_PER_READER_SETTING}, handed to
     * the engine factories as a supplier so a reader opened after a
     * settings update sees the new bound. The Lucene bound until the
     * components are created.
     */
    private volatile long maxDocsPerReader = IndexWriter.MAX_DOCS;

    /** The {@code lance_native} breaker, as {@link LanceCircuitBreaker#breakerSettings} describes it. */
    @Override
    public BreakerSettings getCircuitBreaker(Settings settings) {
        return LanceCircuitBreaker.breakerSettings(settings);
    }

    /**
     * OpenSearch calls this once at node start with the breaker it built
     * from {@link #getCircuitBreaker}. The static holder makes it
     * reachable from the query paths, which only see a
     * {@code QueryShardContext}.
     */
    @Override
    public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
        LanceCircuitBreaker.setBreaker(circuitBreaker);
    }

    /**
     * Builds the node's components in dependency order. Each component
     * reads its own settings and registers its own dynamic update
     * consumers through its {@code fromSettings} or {@code bindSettings}
     * entry; this method only decides what exists on a node, what is
     * handed to what, and which components listen to cluster state (the
     * listeners come off again in {@link #close}, in reverse order).
     * <ol>
     * <li>The shared Lance Session, sized by {@link NativeMemoryLimit.Budget}
     * from {@code plugins.lance.native_memory.limit}, before any dataset
     * is opened: every shard on the node then shares one index cache and
     * one metadata cache instead of allocating its own.</li>
     * <li>The three caches: the data nodes' fetch cache, the snapshot and
     * column cache the fragment path reads through (the column cache takes
     * its share of the budget), and the coordinator's request cache.</li>
     * <li>The warm-up, the freshness checks and the stats collector over
     * the caches.</li>
     * <li>The breaker sampler and the static settings holders of the
     * query, admission and aggregation code.</li>
     * <li>The action filters and the namespace service.</li>
     * </ol>
     * The returned components are injected into the plugin's transport
     * actions; the indices service holder is bound by Guice through
     * {@link #getGuiceServiceClasses}.
     */
    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        Settings settings = environment.settings();
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        this.clusterService = clusterService;
        this.allowedTableRoots = new AllowedTableRoots(LanceSettings.ALLOWED_TABLE_ROOTS_SETTING.get(settings));
        this.allowedCatalogEndpoints = new AllowedCatalogEndpoints(LanceSettings.ALLOWED_CATALOG_ENDPOINTS_SETTING.get(settings));

        // 1. The shared Session.
        NativeMemoryLimit.Budget budget = NativeMemoryLimit.Budget.fromSettings(settings);
        LanceRegistry.initSession(budget.indexCache(), budget.metadataCacheBytes());
        LOGGER.info("installed shared Lance Session: {}", budget.describe());

        // 2. The caches.
        this.fetchCache = LanceFetchCache.fromSettings(settings, clusterSettings);
        clusterService.addListener(fetchCache);
        this.warmCache = LanceWarmCache.fromSettings(
            settings,
            clusterSettings,
            LanceRegistry.allocator(),
            budget.columnCacheBytes(),
            threadPool.executor(ThreadPool.Names.GENERIC),
            fetchCache
        );
        this.requestCache = LanceRequestCache.fromSettings(settings, clusterSettings);
        clusterService.addListener(requestCache);

        // 3. Warm-up, freshness and stats over the caches.
        this.indexWarmer = LanceIndexWarmer.fromSettings(
            settings,
            clusterSettings,
            warmCache,
            threadPool.executor(LanceIndexWarmer.THREAD_POOL)
        );
        clusterService.addListener(indexWarmer);
        this.maxDocsPerReader = LanceSettings.MAX_DOCS_PER_READER_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(LanceSettings.MAX_DOCS_PER_READER_SETTING, value -> maxDocsPerReader = value);
        this.freshnessService = new LanceIndexFreshnessService(
            client,
            threadPool,
            LanceSettings.NAMESPACE_POLL_CADENCE_SETTING.get(settings),
            warmCache,
            servedVersions
        );
        // The session size is read through the registry because the stats
        // package cannot see the registry's package-private session accessor.
        LanceStatsCollector statsCollector = new LanceStatsCollector(warmCache, () -> {
            Session session = LanceRegistry.currentSession();
            return session == null || session.isClosed() ? 0L : session.sizeBytes();
        }, LanceRegistry::indexCacheSizing, indexWarmer, freshnessService::stats, requestCache::stats, fetchCache::stats);

        // 4. The breaker sampler and the static settings holders. The
        // breaker itself was handed to LanceCircuitBreaker by
        // setCircuitBreaker earlier in the node lifecycle.
        LanceWarmCache columns = warmCache;
        this.circuitBreakerSampler = LanceCircuitBreaker.Sampler.start(threadPool, settings, clusterSettings, columns::columnCacheBytes);
        LanceFtsQuery.bindSettings(settings, clusterSettings);
        ScanAdmission.bindSettings(settings, clusterSettings);
        ScanAdmission.setTableStatistics(warmCache.tableStatistics());
        LanceAggregateResults.bindSettings(settings, clusterSettings);

        // 5. The action filters and the namespace service. The dispatch
        // filter intercepts every _search against a Lance backed index
        // and hands it to the plugin's coordinator; the shard fan out
        // through ReadOnlyEngine runs only for the shapes the fragment
        // executor cannot answer yet. The clear cache filter drops the
        // entries of a Lance backed index from every node's result cache
        // before OpenSearch's own action clears the shard caches.
        this.dispatchActionFilter = new LanceDispatchActionFilter(clusterService, indexNameExpressionResolver, client, threadPool);
        this.createIndexActionFilter = new LanceCreateIndexActionFilter(threadPool);
        this.clearCacheActionFilter = new LanceClearCacheActionFilter(clusterService, indexNameExpressionResolver, client);
        this.namespaceService = LanceNamespaceService.fromSettings(
            client,
            clusterService,
            threadPool,
            warmCache,
            allowedTableRoots,
            allowedCatalogEndpoints
        );

        return List.of(
            namespaceService,
            freshnessService,
            allowedTableRoots,
            allowedCatalogEndpoints,
            warmCache,
            statsCollector,
            requestCache,
            fetchCache,
            indicesServiceHolder
        );
    }

    /**
     * Closes what {@link #createComponents} built, in this order.
     * <ol>
     * <li>The background loops stop: the circuit breaker sampler, the
     * namespace poll (with the namespace handles), the freshness checks
     * and the warm-ups (which waits for a warm-up inside a Lance scan).
     * Nothing starts new work against the caches from here on.</li>
     * <li>The cluster state listeners come off the cluster service: the
     * fetch cache, the request cache and the warmer.
     * A restart within one JVM (the test framework) would otherwise
     * leave them attached to a cluster service the next plugin instance
     * shares, and its state events would reach the closed instance.</li>
     * <li>The request cache and the fetch cache drop their entries.</li>
     * <li>The snapshot cache closes: it waits for the leases requests
     * still hold, closes every dataset, then the table statistics and
     * the off heap column store.</li>
     * <li>The shared Lance session is released, last, because the
     * datasets closed above were opened against it.</li>
     * </ol>
     */
    @Override
    public void close() throws IOException {
        // 1. Background loops.
        LanceCircuitBreaker.Sampler sampler = circuitBreakerSampler;
        if (sampler != null) {
            sampler.close();
            circuitBreakerSampler = null;
        }
        LanceNamespaceService namespaces = namespaceService;
        if (namespaces != null) {
            namespaces.close();
            namespaceService = null;
        }
        LanceIndexFreshnessService freshness = freshnessService;
        if (freshness != null) {
            freshness.close();
            freshnessService = null;
        }
        LanceIndexWarmer warmer = indexWarmer;
        if (warmer != null) {
            warmer.close();
        }
        // 2. Cluster state listeners.
        ClusterService cluster = clusterService;
        LanceRequestCache requests = requestCache;
        LanceFetchCache fetches = fetchCache;
        if (cluster != null) {
            removeListener(cluster, fetches);
            removeListener(cluster, requests);
            removeListener(cluster, warmer);
            clusterService = null;
        }
        indexWarmer = null;
        // 3. The coordinator's and the data nodes' heap caches.
        if (requests != null) {
            requests.close();
            requestCache = null;
        }
        if (fetches != null) {
            fetches.close();
            fetchCache = null;
        }
        // 4. The snapshot cache: every dataset and the column store.
        LanceWarmCache cache = warmCache;
        if (cache != null) {
            ScanAdmission.setTableStatistics(null);
            cache.close();
            warmCache = null;
        }
        // 5. The shared native session. Dataset handles that are still
        // open keep their own reference to the native session, so this
        // is safe when a shard is still open at shutdown.
        LanceRegistry.closeSession();
        super.close();
    }

    private static void removeListener(ClusterService clusterService, ClusterStateListener listener) {
        if (listener != null) {
            clusterService.removeListener(listener);
        }
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        // The filter is created lazily in createComponents, so return
        // an empty list until then. In practice OpenSearch calls
        // createComponents before it consults getActionFilters, so the
        // filter is always present when the search machinery starts
        // routing through it; the null guard exists purely for the
        // test framework's out-of-order invocations.
        List<ActionFilter> filters = new ArrayList<>(4);
        LanceDispatchActionFilter dispatch = dispatchActionFilter;
        if (dispatch != null) {
            filters.add(dispatch);
        }
        LanceCreateIndexActionFilter guard = createIndexActionFilter;
        if (guard != null) {
            filters.add(guard);
        }
        LanceClearCacheActionFilter clear = clearCacheActionFilter;
        if (clear != null) {
            filters.add(clear);
        }
        // GET /<index> writes each index's settings as they are in the
        // cluster state; the settings filter registered through
        // getSettingsFilter does not reach that view, so this filter
        // applies the same patterns to the response. It holds no node
        // state and needs no createComponents.
        filters.add(new LanceGetIndexActionFilter());
        return List.copyOf(filters);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(LanceFragmentQueryAction.INSTANCE, TransportLanceFragmentQueryAction.class),
            new ActionHandler<>(LanceFragmentFetchAction.INSTANCE, TransportLanceFragmentFetchAction.class),
            new ActionHandler<>(LanceCoordinatorAction.INSTANCE, TransportLanceCoordinatorAction.class),
            new ActionHandler<>(LanceNamespaceUpdateAction.INSTANCE, TransportLanceNamespaceUpdateAction.class),
            new ActionHandler<>(LanceNamespaceListAction.INSTANCE, TransportLanceNamespaceListAction.class),
            new ActionHandler<>(LanceNamespacePollAction.INSTANCE, TransportLanceNamespacePollAction.class),
            new ActionHandler<>(LanceIndexSyncAction.INSTANCE, TransportLanceIndexSyncAction.class),
            new ActionHandler<>(LanceAttachAction.INSTANCE, TransportLanceAttachAction.class),
            new ActionHandler<>(LanceRefsAction.INSTANCE, TransportLanceRefsAction.class),
            new ActionHandler<>(LanceStatsAction.INSTANCE, TransportLanceStatsAction.class),
            new ActionHandler<>(LanceExplainAction.INSTANCE, TransportLanceExplainAction.class),
            new ActionHandler<>(LanceRequestCacheClearAction.INSTANCE, TransportLanceRequestCacheClearAction.class),
            new ActionHandler<>(LanceStatisticsPrefetchAction.INSTANCE, TransportLanceStatisticsPrefetchAction.class)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, LanceNamespaceMetadata.TYPE, LanceNamespaceMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, LanceNamespaceMetadata.TYPE, LanceNamespaceMetadata::readDiffFrom)
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(LanceNamespaceMetadata.TYPE),
                LanceNamespaceMetadata::fromXContent
            )
        );
    }

    /**
     * The plugin's REST surface. Every path sits under
     * {@code /_plugins/_lance}:
     * <ul>
     *   <li>{@code POST /_plugins/_lance/attach} ({@link RestAttachAction})</li>
     *   <li>{@code POST GET DELETE /_plugins/_lance/namespace},
     *       {@code POST /_plugins/_lance/namespace/tables},
     *       {@code POST /_plugins/_lance/namespace/_poll} ({@link RestNamespaceAction})</li>
     *   <li>{@code GET /_plugins/_lance/refs/{index}} ({@link RestRefsAction})</li>
     *   <li>{@code GET /_plugins/_lance/stats},
     *       {@code GET /_plugins/_lance/{node_id}/stats} ({@link RestLanceStatsAction})</li>
     *   <li>{@code GET /_plugins/_lance/explain/{index}} ({@link RestLanceExplainAction})</li>
     *   <li>{@code POST /_plugins/_lance/sync/{index}} ({@link RestLanceSyncAction})</li>
     * </ul>
     */
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
            new RestAttachAction(),
            new RestNamespaceAction(),
            new RestRefsAction(),
            new RestLanceStatsAction(),
            new RestLanceExplainAction(),
            new RestLanceSyncAction()
        );
    }
}
