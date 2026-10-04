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
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.lance.engine.HidingReaderWrapper;
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
import java.util.stream.Collectors;

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
        Set<String> names = queries.stream().map(q -> q.getName().getPreferredName()).collect(Collectors.toSet());
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
        Set<String> names = plugin.getActions().stream().map(h -> h.getAction().name()).collect(Collectors.toSet());
        assertTrue(names.toString(), names.contains("cluster:admin/lance/attach"));
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
        Set<String> settingKeys = plugin.getSettings().stream().map(Setting::getKey).collect(Collectors.toSet());
        // The exact settings the RFC and the README expect users to see.
        assertTrue(settingKeys.contains(LanceEngineFactory.TABLE_SETTING));
        assertTrue(settingKeys.contains(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING));
        assertTrue(settingKeys.contains(LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING));
        assertTrue(settingKeys.contains(LanceEngineFactory.MULTI_FIELDS_SETTING));
        assertTrue(settingKeys.contains("index.plugins.lance.uncovered_fragment_policy"));
        assertTrue(settingKeys.contains(LanceEngineFactory.TAG_SETTING));
        assertTrue(settingKeys.contains("plugins.lance.namespace.poll_cadence"));
        assertTrue(settingKeys.contains("plugins.lance.allowed_catalog_endpoints"));
    }

    public void testEverySettingIsUnderThePluginPrefix() {
        // Every key is plugins.lance.* (node scope) or index.plugins.lance.*
        // (index scope), nothing is registered as deprecated and the set is
        // pinned so a new or removed setting shows up in review.
        List<Setting<?>> settings = plugin.getSettings();
        // 39 node settings and 9 index settings.
        assertEquals(48, settings.size());
        assertEquals(settings.size(), settings.stream().map(Setting::getKey).collect(Collectors.toSet()).size());
        int nodeScoped = 0;
        int indexScoped = 0;
        for (Setting<?> setting : settings) {
            String key = setting.getKey();
            assertFalse(key, setting.isDeprecated());
            if (key.startsWith("index.plugins.lance.")) {
                assertTrue(key, setting.hasIndexScope());
                assertFalse(key, setting.hasNodeScope());
                indexScoped++;
            } else {
                assertTrue(key, key.startsWith("plugins.lance."));
                assertTrue(key, setting.hasNodeScope());
                assertFalse(key, setting.hasIndexScope());
                nodeScoped++;
            }
        }
        assertEquals(39, nodeScoped);
        assertEquals(9, indexScoped);
    }

    public void testHidingWrapperSettingTakesFourPartsOrNothing() {
        // The empty default installs no wrapper; a value is the prefix,
        // the hidden column, the filter column and an integer minimum.
        assertNull(HidingReaderWrapper.Rule.parse(LanceSettings.TEST_HIDING_WRAPPER_INDEX_PREFIX_SETTING.get(Settings.EMPTY)));
        Settings valid = Settings.builder().put("plugins.lance.test.hiding_wrapper_index_prefix", "wrapped-:body:rating:200").build();
        HidingReaderWrapper.Rule rule = HidingReaderWrapper.Rule.parse(LanceSettings.TEST_HIDING_WRAPPER_INDEX_PREFIX_SETTING.get(valid));
        assertEquals(new HidingReaderWrapper.Rule("wrapped-", "body", "rating", 200L), rule);
        assertTrue(rule.appliesTo("wrapped-demo"));
        assertFalse(rule.appliesTo("plain-demo"));
        for (String bad : List.of("wrapped-", "wrapped-:body:rating", "wrapped-:body:rating:ten", ":body:rating:1", "a:b:c:1:2")) {
            Settings settings = Settings.builder().put("plugins.lance.test.hiding_wrapper_index_prefix", bad).build();
            IllegalArgumentException refused = expectThrows(
                IllegalArgumentException.class,
                () -> LanceSettings.TEST_HIDING_WRAPPER_INDEX_PREFIX_SETTING.get(settings)
            );
            assertTrue(refused.getMessage(), refused.getMessage().contains("plugins.lance.test.hiding_wrapper_index_prefix"));
        }
    }

    public void testIndexSettingsNameTheTableAndCopyUnderThePluginPrefix() {
        // The engine factory, the table, the primary key and the storage
        // options are all resolved from the index settings, and a recreate
        // carries the plugin's keys and nothing else.
        Settings settings = Settings.builder()
            .put("index.plugins.lance.table", "s3://bucket/demo.lance")
            .put("index.plugins.lance.primary_key_field", "id")
            .put("index.plugins.lance.primary_key_type", "keyword")
            .put("index.plugins.lance.version", 4)
            .put("index.plugins.lance.uncovered_fragment_policy", "wait")
            .put("index.plugins.lance.storage_options.aws_region", "eu-west-1")
            .put("index.number_of_shards", 1)
            .build();
        assertTrue(LanceEngineFactory.isLanceIndex(settings));
        assertEquals("s3://bucket/demo.lance", LanceEngineFactory.tableOf(settings));
        assertEquals("id", LanceSettings.PRIMARY_KEY_FIELD_SETTING.get(settings));
        assertEquals("keyword", LanceSettings.PRIMARY_KEY_TYPE_SETTING.get(settings));
        assertEquals(Long.valueOf(4L), LanceSettings.VERSION_SETTING.get(settings));
        assertEquals("wait", LanceSettings.UNCOVERED_FRAGMENT_POLICY_SETTING.get(settings));
        assertEquals("", LanceSettings.TAG_SETTING.get(settings));
        assertEquals(java.util.Map.of("aws_region", "eu-west-1"), StorageOptions.fromIndexSettings(settings).asMap());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "demo",
            settings,
            plugin.getSettings().stream().filter(Setting::hasIndexScope).toArray(Setting<?>[]::new)
        );
        assertTrue(plugin.getEngineFactory(indexSettings).isPresent());
        assertFalse(LanceEngineFactory.isLanceIndex(Settings.builder().put("index.number_of_shards", 1).build()));
        assertFalse(LanceEngineFactory.isLanceIndex(Settings.builder().put("index.lance.table", "s3://bucket/old.lance").build()));
        assertNull(LanceEngineFactory.tableOf(Settings.EMPTY));

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
        assertEquals("s3://bucket/demo.lance", copied.get("index.plugins.lance.table"));
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

    public void testNamespacePollCadenceHasSaneDefaults() {
        Setting<?> setting = plugin.getSettings()
            .stream()
            .filter(s -> "plugins.lance.namespace.poll_cadence".equals(s.getKey()))
            .findFirst()
            .orElseThrow();
        assertTrue("poll cadence should be node-scoped", setting.hasNodeScope());
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
            LanceSettings.ATTACH_WARM_INDEXES_SETTING.get(
                org.opensearch.common.settings.Settings.builder().put("plugins.lance.attach.warm_indexes", "ALL").build()
            )
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LanceSettings.ATTACH_WARM_INDEXES_SETTING.get(
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
