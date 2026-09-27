/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.List;
import java.util.Map;

import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link LanceGetIndexActionFilter} withholds the credential entries of
 * {@code index.plugins.lance.storage_options.*} from a get index response and
 * leaves every other setting, and every index that is not Lance backed,
 * as it was.
 */
public class LanceGetIndexActionFilterTests extends OpenSearchTestCase {

    private static final String OPTIONS = StorageOptions.INDEX_SETTING_PREFIX;

    private final LanceGetIndexActionFilter filter = new LanceGetIndexActionFilter();

    private static Settings lanceSettings() {
        return Settings.builder()
            .put(LanceEngineFactory.TABLE_SETTING, "s3://bucket/demo.lance")
            .put("index.number_of_shards", 1)
            .put(OPTIONS + "aws_access_key_id", "AKIAFILTERME")
            .put(OPTIONS + "aws_secret_access_key", "SECRETFILTERME")
            .put(OPTIONS + "aws_session_token", "TOKENFILTERME")
            .put(OPTIONS + "header.Authorization", "Bearer FILTERME")
            .put(OPTIONS + "aws_region", "us-east-1")
            .put(OPTIONS + "aws_endpoint", "http://localhost:9000")
            .put(OPTIONS + "allow_http", "true")
            .build();
    }

    private static Settings plainSettings() {
        // A regular index whose settings happen to spell a credential
        // word; not Lance backed, so nothing is filtered.
        return Settings.builder().put("index.number_of_shards", 1).put("index.custom.secret_key", "KEEPME").build();
    }

    private static GetIndexResponse response(Map<String, Settings> settings) {
        return new GetIndexResponse(settings.keySet().toArray(new String[0]), Map.of(), Map.of(), settings, Map.of(), Map.of(), Map.of());
    }

    public void testWithholdsCredentialsOfLanceIndexesOnly() {
        GetIndexResponse filtered = filter.withFilteredSettings(response(Map.of("lance", lanceSettings(), "plain", plainSettings())));

        Settings lance = filtered.settings().get("lance");
        assertEquals("s3://bucket/demo.lance", lance.get(LanceEngineFactory.TABLE_SETTING));
        assertEquals("1", lance.get("index.number_of_shards"));
        assertEquals("us-east-1", lance.get(OPTIONS + "aws_region"));
        assertEquals("http://localhost:9000", lance.get(OPTIONS + "aws_endpoint"));
        assertEquals("true", lance.get(OPTIONS + "allow_http"));
        for (String key : List.of("aws_access_key_id", "aws_secret_access_key", "aws_session_token", "header.Authorization")) {
            assertNull(key + " must be withheld: " + lance, lance.get(OPTIONS + key));
        }
        assertFalse(lance.toString(), lance.toString().contains("FILTERME"));

        assertEquals("a non Lance index is untouched", plainSettings(), filtered.settings().get("plain"));
        assertArrayEquals(new String[] { "lance", "plain" }, filtered.indices());
    }

    public void testResponseWithoutLanceIndexesIsReturnedAsIs() {
        GetIndexResponse plainOnly = response(Map.of("plain", plainSettings()));
        assertSame(plainOnly, filter.withFilteredSettings(plainOnly));

        GetIndexResponse noSettings = response(Map.of());
        assertSame(noSettings, filter.withFilteredSettings(noSettings));
    }

    public void testFiltersTheSamePatternsAsTheSettingsFilter() {
        // Every key the plugin's settings filter withholds from
        // GET /<index>/_settings is withheld from GET /<index> too, and
        // no other.
        Settings lance = filter.withFilteredSettings(response(Map.of("lance", lanceSettings()))).settings().get("lance");
        for (String key : lanceSettings().keySet()) {
            boolean sensitive = key.startsWith(OPTIONS) && StorageOptions.isSensitiveKey(key.substring(OPTIONS.length()));
            assertEquals(key, sensitive, lance.get(key) == null);
        }
    }
}
