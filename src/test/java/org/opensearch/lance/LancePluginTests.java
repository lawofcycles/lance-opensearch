/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.opensearch.common.settings.Setting;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.mapper.LanceTextFieldMapper;
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
        assertTrue(settingKeys.contains("lance.namespace.poll_cadence"));
        assertTrue(settingKeys.contains("lance.builder.max_rows"));
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
}
