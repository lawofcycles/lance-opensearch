/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import org.opensearch.action.ActionType;

/**
 * Runs the freshness check of one Lance backed index now, on the node
 * that holds its shard, behind {@code POST /{index}/_lance/sync}. The
 * check is the one {@link LanceIndexFreshnessService} runs at
 * {@code lance.namespace.poll_cadence}: compare the table's latest
 * manifest (or the followed tag) with the version the shard serves, and
 * when they differ re-derive the mapping, apply it if it changed, and
 * refresh the shard.
 *
 * <p>The name is an index level {@code indices:admin/} permission, like
 * {@code build_indexes}: the check may update the index's mapping and
 * settings, so the roles that administer the index are the ones that may
 * trigger it.
 */
public final class LanceIndexSyncAction extends ActionType<LanceIndexSyncResponse> {

    public static final String NAME = "indices:admin/lance/sync";
    public static final LanceIndexSyncAction INSTANCE = new LanceIndexSyncAction();

    private LanceIndexSyncAction() {
        super(NAME, LanceIndexSyncResponse::new);
    }
}
