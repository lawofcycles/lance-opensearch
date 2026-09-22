/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import org.opensearch.action.ActionType;

/**
 * Internal fan-out leg of {@link LanceBuildIndexesAction} for indexes
 * attached with {@code index.lance.index_placement = node_local}: every
 * data node builds the requested indexes into its own shallow clone and
 * reports its outcome. Invoked by {@link TransportLanceBuildIndexesAction}
 * after it has resolved the index and its placement; a security plugin
 * that scopes privileges by action name covers this one with the
 * {@code indices:admin/lance/build_indexes*} pattern.
 */
public final class LanceBuildIndexesNodesAction extends ActionType<LanceBuildIndexesNodesResponse> {

    public static final String NAME = LanceBuildIndexesAction.NAME + "[nodes]";
    public static final LanceBuildIndexesNodesAction INSTANCE = new LanceBuildIndexesNodesAction();

    private LanceBuildIndexesNodesAction() {
        super(NAME, LanceBuildIndexesNodesResponse::new);
    }
}
