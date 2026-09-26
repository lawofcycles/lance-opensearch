/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import org.opensearch.action.ActionType;

/**
 * Starts the collection of a table version's planner statistics on
 * every data node, served by {@link TransportLanceStatisticsPrefetchAction}.
 * Issued by the freshness check of the node holding an index's shard
 * when the table moved to a new version: the served version is not in
 * the cluster state, so the other data nodes have no event of their own
 * to collect on, and without this broadcast each of them would plan its
 * first request of the new version without statistics.
 */
public final class LanceStatisticsPrefetchAction extends ActionType<LanceStatisticsPrefetchResponse> {

    public static final LanceStatisticsPrefetchAction INSTANCE = new LanceStatisticsPrefetchAction();
    public static final String NAME = "cluster:admin/lance/statistics/prefetch";

    private LanceStatisticsPrefetchAction() {
        super(NAME, LanceStatisticsPrefetchResponse::new);
    }
}
