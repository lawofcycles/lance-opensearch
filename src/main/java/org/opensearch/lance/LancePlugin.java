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
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.mapper.Mapper;
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
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

/**
 * Plugin entry point. Registers the reader-side surface for Lance tables:
 * the REST endpoints for namespace / attach / build_indexes, the
 * {@link LanceEngineFactory} that wraps each Lance-backed index in a
 * read-only engine, mapping type parsers, and the {@code lance_knn} query.
 */
public class LancePlugin extends Plugin implements ActionPlugin, EnginePlugin, MapperPlugin, SearchPlugin {

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
            new QuerySpec<>(
                LanceMultiMatchQueryBuilder.NAME,
                LanceMultiMatchQueryBuilder::new,
                LanceMultiMatchQueryBuilder::fromXContent
            ),
            new QuerySpec<>(
                LanceFtsBoostQueryBuilder.NAME,
                LanceFtsBoostQueryBuilder::new,
                LanceFtsBoostQueryBuilder::fromXContent
            ),
            new QuerySpec<>(
                LanceFtsBoolQueryBuilder.NAME,
                LanceFtsBoolQueryBuilder::new,
                LanceFtsBoolQueryBuilder::fromXContent
            )
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
            NATIVE_MEMORY_LIMIT_SETTING
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

        namespaceService = new LanceNamespaceService(client, threadPool, cadence, builderMaxRows);
        return List.of(namespaceService);
    }

    @Override
    public void close() throws IOException {
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
