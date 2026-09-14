/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

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
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

/** PoC plugin exposing Lance tables through REST endpoints and a read only engine. */
public class LancePlugin extends Plugin implements ActionPlugin, EnginePlugin, MapperPlugin, SearchPlugin {

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(new QuerySpec<>(LanceKnnQueryBuilder.NAME, LanceKnnQueryBuilder::new, LanceKnnQueryBuilder::fromXContent));
    }

    @Override
    public java.util.Map<String, Mapper.TypeParser> getMappers() {
        return java.util.Map.of(LanceTextFieldMapper.CONTENT_TYPE, LanceTextFieldMapper.PARSER);
    }

    public static final Setting<String> TABLE_SETTING = Setting.simpleString(
        LanceEngineFactory.TABLE_SETTING,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    public static final Setting<String> PRIMARY_KEY_FIELD_SETTING = Setting.simpleString(
        LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING,
        "id",
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

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            TABLE_SETTING,
            PRIMARY_KEY_FIELD_SETTING,
            UNCOVERED_FRAGMENT_POLICY_SETTING,
            NAMESPACE_POLL_CADENCE_SETTING,
            BUILDER_MAX_ROWS_SETTING
        );
    }

    private static void validateUncoveredFragmentPolicy(String value) {
        if (!"wait".equals(value) && !"immediate".equals(value)) {
            throw new IllegalArgumentException("index.lance.uncovered_fragment_policy must be 'wait' or 'immediate', got '" + value + "'");
        }
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
        namespaceService = new LanceNamespaceService(client, threadPool, cadence, builderMaxRows);
        return List.of(namespaceService);
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
            new RestAttachAction(),
            new RestScanAction(),
            new RestQueryAction(),
            new RestNamespaceAction(namespaceService),
            new RestBuildIndexesAction(threadPool)
        );
    }
}
