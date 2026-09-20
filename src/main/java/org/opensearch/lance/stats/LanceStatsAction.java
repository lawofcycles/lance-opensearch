/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.stats;

import org.opensearch.action.ActionType;

/**
 * Per node cache statistics of the plugin, served by
 * {@link TransportLanceStatsAction} and exposed as
 * {@code GET /_lance/stats}. The name sits under {@code cluster:monitor/}
 * so a security plugin can grant it to read only roles the way it grants
 * {@code cluster:monitor/nodes/stats}.
 */
public final class LanceStatsAction extends ActionType<LanceStatsResponse> {

    public static final LanceStatsAction INSTANCE = new LanceStatsAction();
    public static final String NAME = "cluster:monitor/lance/stats";

    private LanceStatsAction() {
        super(NAME, LanceStatsResponse::new);
    }
}
