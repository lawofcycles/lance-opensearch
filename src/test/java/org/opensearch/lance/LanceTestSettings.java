/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.HashSet;
import java.util.Set;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Settings fixtures for the unit tests of the components the plugin
 * builds in {@code createComponents}.
 */
public final class LanceTestSettings {

    private LanceTestSettings() {}

    /**
     * A {@link ClusterSettings} that knows OpenSearch's built in node
     * settings and the plugin's, as a node's does, so a test can hand it
     * to a {@code fromSettings} or {@code bindSettings} entry and drive
     * the dynamic updates through {@link ClusterSettings#applySettings}.
     */
    public static ClusterSettings clusterSettings(Settings settings) {
        Set<Setting<?>> known = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        for (Setting<?> setting : LanceSettings.all()) {
            if (setting.hasNodeScope()) {
                known.add(setting);
            }
        }
        return new ClusterSettings(settings, known);
    }
}
