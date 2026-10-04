/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.execute;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.lance.LanceSettings;
import org.opensearch.lance.LanceTestSettings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The static settings holders of {@link LanceAggregateResults}: what
 * {@link LanceAggregateResults#bindSettings} reads at start and what the
 * dynamic updates change afterwards.
 */
public class LanceAggregateResultsTests extends OpenSearchTestCase {

    @Override
    public void tearDown() throws Exception {
        LanceAggregateResults.setPercentilesBins(LanceSettings.AGGREGATION_PERCENTILES_BINS_SETTING.getDefault(Settings.EMPTY));
        LanceAggregateResults.setTopkSlack(LanceSettings.AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING.getDefault(Settings.EMPTY));
        super.tearDown();
    }

    public void testBindSettingsReadsTheNodeSettingsAndFollowsTheirUpdates() {
        Settings settings = Settings.builder()
            .put("plugins.lance.aggregation.percentiles_bins", 512)
            .put("plugins.lance.aggregation.pushdown_topk_slack", 8)
            .build();
        ClusterSettings clusterSettings = LanceTestSettings.clusterSettings(settings);

        LanceAggregateResults.bindSettings(settings, clusterSettings);
        assertEquals(512, LanceAggregateResults.percentilesBins());
        assertEquals(8, LanceAggregateResults.topkSlack());

        clusterSettings.applySettings(Settings.builder().put("plugins.lance.aggregation.percentiles_bins", 1024).build());
        assertEquals("the bins consumer is registered", 1024, LanceAggregateResults.percentilesBins());
        assertEquals("the slack keeps the node setting", 8, LanceAggregateResults.topkSlack());

        clusterSettings.applySettings(
            Settings.builder()
                .put("plugins.lance.aggregation.percentiles_bins", 1024)
                .put("plugins.lance.aggregation.pushdown_topk_slack", 16)
                .build()
        );
        assertEquals(1024, LanceAggregateResults.percentilesBins());
        assertEquals("the slack consumer is registered", 16, LanceAggregateResults.topkSlack());
    }
}
