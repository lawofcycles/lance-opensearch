/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceIndexWarmer;
import org.opensearch.lance.mapper.LanceTextFieldMapper;
import org.opensearch.lance.namespace.LanceNamespaceMetadata;
import org.opensearch.lance.query.LanceFtsBoolQueryBuilder;
import org.opensearch.lance.query.LanceFtsBoostQueryBuilder;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchPhraseQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.lance.query.LanceMultiMatchQueryBuilder;
import org.opensearch.plugins.SearchPlugin.QuerySpec;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The plugin's registrations (queries, actions, mapper, settings) and its
 * lifecycle. Thread leak checking is off at the suite level because the
 * lifecycle test installs a Lance session, whose native runtime threads
 * cannot be shut down from Java.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LancePluginTests extends OpenSearchTestCase {

    private LancePlugin plugin;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        plugin = new LancePlugin();
    }

    public void testPluginInstantiation() {
        assertNotNull(plugin);
    }

    public void testRegistersLanceKnnQuery() {
        List<QuerySpec<?>> queries = plugin.getQueries();
        assertEquals(6, queries.size());
        java.util.Set<String> names = queries.stream()
            .map(q -> q.getName().getPreferredName())
            .collect(java.util.stream.Collectors.toSet());
        assertTrue("expected lance_knn in registered queries: " + names, names.contains(LanceKnnQueryBuilder.NAME));
        assertTrue("expected lance_match in registered queries: " + names, names.contains(LanceMatchQueryBuilder.NAME));
        assertTrue("expected lance_match_phrase in registered queries: " + names, names.contains(LanceMatchPhraseQueryBuilder.NAME));
        assertTrue("expected lance_multi_match in registered queries: " + names, names.contains(LanceMultiMatchQueryBuilder.NAME));
        assertTrue("expected lance_fts_boost in registered queries: " + names, names.contains(LanceFtsBoostQueryBuilder.NAME));
        assertTrue("expected lance_fts_bool in registered queries: " + names, names.contains(LanceFtsBoolQueryBuilder.NAME));
    }

    public void testExposesLanceActionNames() {
        // The action names are the privileges operators grant to roles, so
        // a rename is a breaking change and has to show up in review.
        Set<String> names = plugin.getActions().stream().map(h -> h.getAction().name()).collect(java.util.stream.Collectors.toSet());
        assertTrue(names.toString(), names.contains("cluster:admin/lance/attach"));
        assertTrue(names.toString(), names.contains("indices:admin/lance/build_indexes"));
        assertTrue(names.toString(), names.contains("indices:monitor/lance/refs"));
        assertTrue(names.toString(), names.contains("cluster:monitor/lance/namespace"));
        assertTrue(names.toString(), names.contains("cluster:admin/lance/namespace/update"));
        assertTrue(names.toString(), names.contains("cluster:admin/lance/namespace/poll"));
        assertTrue(names.toString(), names.contains("indices:admin/lance/sync"));
        assertTrue(names.toString(), names.contains("cluster:admin/lance/statistics/prefetch"));
    }

    public void testRegistersLanceTextMapper() {
        assertTrue("lance_text must be registered as a mapper", plugin.getMappers().containsKey(LanceTextFieldMapper.CONTENT_TYPE));
    }

    public void testExposesExpectedSettings() {
        Set<String> settingKeys = plugin.getSettings().stream().map(Setting::getKey).collect(java.util.stream.Collectors.toSet());
        // The exact settings the RFC and the README expect users to see.
        assertTrue(settingKeys.contains(LanceEngineFactory.TABLE_SETTING));
        assertTrue(settingKeys.contains(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING));
        assertTrue(settingKeys.contains(LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING));
        assertTrue(settingKeys.contains(LanceEngineFactory.MULTI_FIELDS_SETTING));
        assertTrue(settingKeys.contains("index.plugins.lance.uncovered_fragment_policy"));
        assertTrue(settingKeys.contains(LanceEngineFactory.INDEX_PLACEMENT_SETTING));
        assertTrue(settingKeys.contains(LanceEngineFactory.TAG_SETTING));
        assertTrue(settingKeys.contains("plugins.lance.namespace.poll_cadence"));
        assertTrue(settingKeys.contains("plugins.lance.builder.max_rows"));
    }

    public void testEverySettingHasACurrentAndADeprecatedKey() {
        // Every current key is plugins.lance.* (node scope) or
        // index.plugins.lance.* (index scope), and each has a deprecated
        // twin under lance.* or index.lance.* with the same scope, the
        // same dynamic flag and the same default, so a cluster configured
        // with the old keys behaves as before and reading an old key logs
        // a deprecation.
        List<Setting<?>> settings = plugin.getSettings();
        List<Setting<?>> current = settings.stream().filter(s -> s.isDeprecated() == false).collect(java.util.stream.Collectors.toList());
        List<Setting<?>> deprecated = settings.stream().filter(Setting::isDeprecated).collect(java.util.stream.Collectors.toList());
        // 38 node settings and 10 index settings.
        assertEquals(48, current.size());
        assertEquals(current.size(), deprecated.size());
        java.util.Map<String, Setting<?>> deprecatedByKey = deprecated.stream()
            .collect(java.util.stream.Collectors.toMap(Setting::getKey, s -> s));
        for (Setting<?> setting : current) {
            String key = setting.getKey();
            String oldKey;
            if (key.startsWith("index.plugins.lance.")) {
                assertTrue(key, setting.hasIndexScope());
                oldKey = "index.lance." + key.substring("index.plugins.lance.".length());
            } else {
                assertTrue(key, key.startsWith("plugins.lance."));
                assertTrue(key, setting.hasNodeScope());
                oldKey = "lance." + key.substring("plugins.lance.".length());
            }
            Setting<?> old = deprecatedByKey.get(oldKey);
            assertNotNull("deprecated twin of " + key, old);
            assertEquals(key, setting.hasNodeScope(), old.hasNodeScope());
            assertEquals(key, setting.hasIndexScope(), old.hasIndexScope());
            assertEquals(key, setting.isDynamic(), old.isDynamic());
            assertEquals(key, setting.isFinal(), old.isFinal());
            assertEquals(key, setting.getDefault(Settings.EMPTY), old.getDefault(Settings.EMPTY));
        }
    }

    public void testDeprecatedNodeKeysAreReadThroughTheCurrentSettings() {
        // A node whose opensearch.yml still carries the lance.* keys: the
        // current settings read the old values through their fallbacks,
        // and a current key set next to an old one wins.
        Settings settings = Settings.builder()
            .put("lance.namespace.poll_cadence", "3s")
            .put("lance.builder.max_rows", 42)
            .putList("lance.allowed_table_roots", "/a", "/b")
            .put("lance.native_memory.limit", "7gb")
            .put("lance.cache.column_share", 0.25)
            .put("lance.request_cache.size", "3mb")
            .put("lance.admission.headroom", "9gb")
            .put("lance.attach.warm_indexes", "all")
            .putList("lance.test.admission_available_memory", "1gb", "2gb")
            .put("lance.fragment_path.slices", 7)
            .put("plugins.lance.fragment_path.slices", 9)
            .build();
        assertEquals(TimeValue.timeValueSeconds(3), LancePlugin.NAMESPACE_POLL_CADENCE_SETTING.get(settings));
        assertEquals(Long.valueOf(42L), LancePlugin.BUILDER_MAX_ROWS_SETTING.get(settings));
        assertEquals(List.of("/a", "/b"), LancePlugin.ALLOWED_TABLE_ROOTS_SETTING.get(settings));
        assertEquals("7gb", LancePlugin.NATIVE_MEMORY_LIMIT_SETTING.get(settings));
        assertEquals(0.25, LancePlugin.CACHE_COLUMN_SHARE_SETTING.get(settings), 0.0);
        assertEquals(new ByteSizeValue(3, ByteSizeUnit.MB), LancePlugin.REQUEST_CACHE_SIZE_SETTING.get(settings));
        assertEquals(new ByteSizeValue(9, ByteSizeUnit.GB), LancePlugin.ADMISSION_HEADROOM_SETTING.get(settings));
        assertEquals(LanceIndexWarmer.Mode.ALL, LancePlugin.ATTACH_WARM_INDEXES_SETTING.get(settings));
        assertEquals(List.of("1gb", "2gb"), LancePlugin.TEST_ADMISSION_AVAILABLE_MEMORY_SETTING.get(settings));
        assertEquals(Integer.valueOf(9), LancePlugin.FRAGMENT_PATH_SLICES_SETTING.get(settings));
        // Defaults still apply when neither key is set.
        assertEquals(Boolean.TRUE, LancePlugin.CACHE_ENABLED_SETTING.get(settings));
        // Every old key present in the settings is reported as deprecated,
        // including the one a current key overrides.
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] {
                LancePlugin.NAMESPACE_POLL_CADENCE_SETTING_DEPRECATED,
                LancePlugin.BUILDER_MAX_ROWS_SETTING_DEPRECATED,
                LancePlugin.ALLOWED_TABLE_ROOTS_SETTING_DEPRECATED,
                LancePlugin.NATIVE_MEMORY_LIMIT_SETTING_DEPRECATED,
                LancePlugin.CACHE_COLUMN_SHARE_SETTING_DEPRECATED,
                LancePlugin.REQUEST_CACHE_SIZE_SETTING_DEPRECATED,
                LancePlugin.ADMISSION_HEADROOM_SETTING_DEPRECATED,
                LancePlugin.ATTACH_WARM_INDEXES_SETTING_DEPRECATED,
                LancePlugin.TEST_ADMISSION_AVAILABLE_MEMORY_SETTING_DEPRECATED,
                LancePlugin.FRAGMENT_PATH_SLICES_SETTING_DEPRECATED }
        );
    }

    public void testDynamicUpdateOfADeprecatedKeyReachesTheCurrentSettingsConsumer() {
        // The consumers are registered on the current settings only. An
        // operator who still updates the old key must reach them, once,
        // through the fallback.
        Set<Setting<?>> nodeSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        for (Setting<?> setting : plugin.getSettings()) {
            if (setting.hasNodeScope()) {
                nodeSettings.add(setting);
            }
        }
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, nodeSettings);
        List<Boolean> seen = new java.util.ArrayList<>();
        clusterSettings.addSettingsUpdateConsumer(LancePlugin.CACHE_ENABLED_SETTING, seen::add);
        clusterSettings.applySettings(Settings.builder().put("lance.cache.enabled", false).build());
        assertEquals(List.of(false), seen);
        clusterSettings.applySettings(Settings.builder().put("plugins.lance.cache.enabled", true).build());
        assertEquals(List.of(false, true), seen);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { LancePlugin.CACHE_ENABLED_SETTING_DEPRECATED });
    }

    public void testIndexCreatedUnderTheDeprecatedKeysIsALanceIndex() {
        // The cluster state of an index attached by an earlier release
        // carries index.lance.*; the engine factory, the table, the
        // primary key and the storage options are all resolved from it.
        Settings settings = Settings.builder()
            .put("index.lance.table", "s3://bucket/old.lance")
            .put("index.lance.primary_key_field", "id")
            .put("index.lance.primary_key_type", "keyword")
            .put("index.lance.version", 4)
            .put("index.lance.uncovered_fragment_policy", "wait")
            .put("index.lance.storage_options.aws_region", "eu-west-1")
            .build();
        assertTrue(LanceEngineFactory.isLanceIndex(settings));
        assertEquals("s3://bucket/old.lance", LanceEngineFactory.tableOf(settings));
        assertEquals("id", LancePlugin.PRIMARY_KEY_FIELD_SETTING.get(settings));
        assertEquals("keyword", LancePlugin.PRIMARY_KEY_TYPE_SETTING.get(settings));
        assertEquals(Long.valueOf(4L), LancePlugin.VERSION_SETTING.get(settings));
        assertEquals("wait", LancePlugin.UNCOVERED_FRAGMENT_POLICY_SETTING.get(settings));
        assertEquals("", LancePlugin.TAG_SETTING.get(settings));
        assertEquals(java.util.Map.of("aws_region", "eu-west-1"), StorageOptions.fromIndexSettings(settings).asMap());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "legacy",
            settings,
            plugin.getSettings().stream().filter(Setting::hasIndexScope).toArray(Setting<?>[]::new)
        );
        assertTrue(plugin.getEngineFactory(indexSettings).isPresent());
        assertFalse(LanceEngineFactory.isLanceIndex(Settings.builder().put("index.number_of_shards", 1).build()));
        assertNull(LanceEngineFactory.tableOf(Settings.EMPTY));

        // Recreating the index from these settings writes the current keys.
        Settings.Builder copy = Settings.builder();
        LanceEngineFactory.copyLanceIndexSettings(settings, copy);
        Settings copied = copy.build();
        assertEquals(
            copied.keySet().toString(),
            Set.of(
                "index.plugins.lance.table",
                "index.plugins.lance.primary_key_field",
                "index.plugins.lance.primary_key_type",
                "index.plugins.lance.version",
                "index.plugins.lance.uncovered_fragment_policy",
                "index.plugins.lance.storage_options.aws_region"
            ),
            copied.keySet()
        );
        assertEquals("s3://bucket/old.lance", copied.get("index.plugins.lance.table"));
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] {
                LancePlugin.TABLE_SETTING_DEPRECATED,
                LancePlugin.PRIMARY_KEY_FIELD_SETTING_DEPRECATED,
                LancePlugin.PRIMARY_KEY_TYPE_SETTING_DEPRECATED,
                LancePlugin.VERSION_SETTING_DEPRECATED,
                LancePlugin.UNCOVERED_FRAGMENT_POLICY_SETTING_DEPRECATED }
        );
    }

    public void testSettingsFilterWithholdsCredentialStorageOptions() {
        // The patterns the plugin hands OpenSearch are the ones the
        // namespace listing redacts by, so a credential key that one
        // hides the other hides too. Region, endpoint and allow_http
        // stay, and settings outside the storage_options group are
        // untouched.
        List<String> patterns = plugin.getSettingsFilter();
        assertEquals(StorageOptions.SENSITIVE_INDEX_SETTING_PATTERNS, patterns);
        for (String pattern : patterns) {
            assertTrue(pattern, SettingsFilter.isValidPattern(pattern));
        }
        Settings raw = Settings.builder()
            .put("index.plugins.lance.table", "s3://bucket/demo.lance")
            .put(StorageOptions.INDEX_SETTING_PREFIX + "aws_access_key_id", "AKID")
            .put(StorageOptions.INDEX_SETTING_PREFIX + "aws_secret_access_key", "SECRET")
            .put(StorageOptions.INDEX_SETTING_PREFIX + "aws_session_token", "TOKEN")
            .put(StorageOptions.INDEX_SETTING_PREFIX + "AWS_SECRET_ACCESS_KEY", "UPPER")
            .put(StorageOptions.INDEX_SETTING_PREFIX + "gcs_service_account_key", "GCS")
            .put(StorageOptions.INDEX_SETTING_PREFIX + "azure_storage_sas_token", "SAS")
            .put(StorageOptions.INDEX_SETTING_PREFIX + "header.Authorization", "Bearer x")
            .put(StorageOptions.INDEX_SETTING_PREFIX + "credential", "id:secret")
            .put(StorageOptions.INDEX_SETTING_PREFIX + "aws_region", "us-east-1")
            .put(StorageOptions.INDEX_SETTING_PREFIX + "aws_endpoint", "https://s3.example")
            .put(StorageOptions.INDEX_SETTING_PREFIX + "allow_http", "true")
            .build();
        Settings filtered = new SettingsFilter(patterns).filter(raw);
        Set<String> kept = filtered.keySet();
        assertEquals(
            kept.toString(),
            Set.of(
                "index.plugins.lance.table",
                StorageOptions.INDEX_SETTING_PREFIX + "aws_region",
                StorageOptions.INDEX_SETTING_PREFIX + "aws_endpoint",
                StorageOptions.INDEX_SETTING_PREFIX + "allow_http"
            ),
            kept
        );
        for (String key : raw.keySet()) {
            if (key.startsWith(StorageOptions.INDEX_SETTING_PREFIX)) {
                String option = key.substring(StorageOptions.INDEX_SETTING_PREFIX.length());
                assertEquals(
                    "filter and redaction must agree on " + option,
                    LanceNamespaceMetadata.Entry.isSensitiveConfigKey(option),
                    kept.contains(key) == false
                );
            }
        }
    }

    public void testIndexPlacementSettingAcceptsOnlyKnownValues() {
        assertEquals("in_table", LancePlugin.INDEX_PLACEMENT_SETTING.getDefault(org.opensearch.common.settings.Settings.EMPTY));
        assertEquals(
            "node_local",
            LancePlugin.INDEX_PLACEMENT_SETTING.get(
                org.opensearch.common.settings.Settings.builder().put(LanceEngineFactory.INDEX_PLACEMENT_SETTING, "node_local").build()
            )
        );
        IllegalArgumentException rejected = expectThrows(
            IllegalArgumentException.class,
            () -> LancePlugin.INDEX_PLACEMENT_SETTING.get(
                org.opensearch.common.settings.Settings.builder().put(LanceEngineFactory.INDEX_PLACEMENT_SETTING, "sideways").build()
            )
        );
        assertTrue(rejected.getMessage(), rejected.getMessage().contains("in_table"));
    }

    public void testNamespacePollCadenceHasSaneDefaults() {
        Setting<?> setting = plugin.getSettings()
            .stream()
            .filter(s -> "plugins.lance.namespace.poll_cadence".equals(s.getKey()))
            .findFirst()
            .orElseThrow();
        assertTrue("poll cadence should be node-scoped", setting.hasNodeScope());
    }

    public void testBuilderMaxRowsDefaultsToOneMillion() {
        Setting<?> setting = plugin.getSettings()
            .stream()
            .filter(s -> "plugins.lance.builder.max_rows".equals(s.getKey()))
            .findFirst()
            .orElseThrow();
        assertEquals(Long.valueOf(1_000_000L), setting.getDefault(org.opensearch.common.settings.Settings.EMPTY));
        assertTrue("max_rows should be node-scoped", setting.hasNodeScope());
    }

    public void testAttachWarmIndexesDefaultsToMetadata() {
        Setting<?> setting = plugin.getSettings()
            .stream()
            .filter(s -> "plugins.lance.attach.warm_indexes".equals(s.getKey()))
            .findFirst()
            .orElseThrow();
        assertEquals(LanceIndexWarmer.Mode.METADATA, setting.getDefault(org.opensearch.common.settings.Settings.EMPTY));
        assertTrue(setting.hasNodeScope());
        assertTrue(setting.isDynamic());
        assertEquals(
            LanceIndexWarmer.Mode.ALL,
            LancePlugin.ATTACH_WARM_INDEXES_SETTING.get(
                org.opensearch.common.settings.Settings.builder().put("plugins.lance.attach.warm_indexes", "ALL").build()
            )
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LancePlugin.ATTACH_WARM_INDEXES_SETTING.get(
                org.opensearch.common.settings.Settings.builder().put("plugins.lance.attach.warm_indexes", "pages").build()
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("none, metadata, all"));
        assertTrue(
            plugin.getExecutorBuilders(org.opensearch.common.settings.Settings.EMPTY)
                .stream()
                .flatMap(builder -> builder.getRegisteredSettings().stream())
                .anyMatch(s -> s.getKey().equals("thread_pool." + LanceIndexWarmer.THREAD_POOL + ".queue_size"))
        );
    }

    public void testCloseTakesEveryClusterStateListenerOffAgain() throws Exception {
        // A node restart within one JVM (the test framework) builds a new
        // plugin instance against a cluster service that outlives the
        // old one. Every listener createComponents registers has to come
        // off in close, or the closed instance's caches keep receiving
        // state events.
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        Set<Setting<?>> nodeSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        for (Setting<?> setting : plugin.getSettings()) {
            if (setting.hasNodeScope()) {
                nodeSettings.add(setting);
            }
        }
        ThreadPool threadPool = new TestThreadPool(getTestName(), plugin.getExecutorBuilders(settings).toArray(new ExecutorBuilder<?>[0]));
        AtomicInteger listeners = new AtomicInteger();
        ClusterService clusterService = new ClusterService(settings, new ClusterSettings(settings, nodeSettings), threadPool) {
            @Override
            public void addListener(ClusterStateListener listener) {
                listeners.incrementAndGet();
                super.addListener(listener);
            }

            @Override
            public void removeListener(ClusterStateListener listener) {
                listeners.decrementAndGet();
                super.removeListener(listener);
            }
        };
        Environment environment = TestEnvironment.newEnvironment(settings);
        try (
            NoOpClient client = new NoOpClient(threadPool);
            NodeEnvironment nodeEnvironment = newNodeEnvironment();
            ClusterService ignored = clusterService
        ) {
            for (int round = 0; round < 2; round++) {
                LancePlugin instance = new LancePlugin();
                Collection<Object> components = instance.createComponents(
                    client,
                    clusterService,
                    threadPool,
                    null,
                    null,
                    NamedXContentRegistry.EMPTY,
                    environment,
                    nodeEnvironment,
                    new NamedWriteableRegistry(List.of()),
                    new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
                    () -> null
                );
                assertFalse(components.isEmpty());
                assertTrue("round " + round + ": createComponents registered listeners", listeners.get() > 0);
                instance.close();
                assertEquals("round " + round + ": close took every listener off again", 0, listeners.get());
            }
        } finally {
            ThreadPool.terminate(threadPool, 30L, TimeUnit.SECONDS);
        }
    }
}
