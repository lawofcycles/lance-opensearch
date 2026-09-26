/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
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
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

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
        assertTrue(settingKeys.contains("index.lance.uncovered_fragment_policy"));
        assertTrue(settingKeys.contains(LanceEngineFactory.INDEX_PLACEMENT_SETTING));
        assertTrue(settingKeys.contains(LanceEngineFactory.TAG_SETTING));
        assertTrue(settingKeys.contains("lance.namespace.poll_cadence"));
        assertTrue(settingKeys.contains("lance.builder.max_rows"));
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
            .put("index.lance.table", "s3://bucket/demo.lance")
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
                "index.lance.table",
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
            .filter(s -> "lance.namespace.poll_cadence".equals(s.getKey()))
            .findFirst()
            .orElseThrow();
        assertTrue("poll cadence should be node-scoped", setting.hasNodeScope());
    }

    public void testBuilderMaxRowsDefaultsToOneMillion() {
        Setting<?> setting = plugin.getSettings()
            .stream()
            .filter(s -> "lance.builder.max_rows".equals(s.getKey()))
            .findFirst()
            .orElseThrow();
        assertEquals(Long.valueOf(1_000_000L), setting.getDefault(org.opensearch.common.settings.Settings.EMPTY));
        assertTrue("max_rows should be node-scoped", setting.hasNodeScope());
    }

    public void testAttachWarmIndexesDefaultsToMetadata() {
        Setting<?> setting = plugin.getSettings()
            .stream()
            .filter(s -> "lance.attach.warm_indexes".equals(s.getKey()))
            .findFirst()
            .orElseThrow();
        assertEquals(LanceIndexWarmer.Mode.METADATA, setting.getDefault(org.opensearch.common.settings.Settings.EMPTY));
        assertTrue(setting.hasNodeScope());
        assertTrue(setting.isDynamic());
        assertEquals(
            LanceIndexWarmer.Mode.ALL,
            LancePlugin.ATTACH_WARM_INDEXES_SETTING.get(
                org.opensearch.common.settings.Settings.builder().put("lance.attach.warm_indexes", "ALL").build()
            )
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> LancePlugin.ATTACH_WARM_INDEXES_SETTING.get(
                org.opensearch.common.settings.Settings.builder().put("lance.attach.warm_indexes", "pages").build()
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
}
