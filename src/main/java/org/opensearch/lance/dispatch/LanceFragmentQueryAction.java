/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import org.opensearch.action.ActionType;

/**
 * Internal action name shared between the coordinator and the
 * per-node handler for fragment-mode dispatch. A coordinator sends
 * one {@link LanceFragmentQueryRequest} per data node, each carrying
 * a subset of the dataset's fragment ids; the receiving node opens
 * the Lance table through the shared registry, scans only those
 * fragments, and replies with a {@link LanceFragmentQueryResponse}
 * that holds partial hits + partial metric state. The coordinator
 * merges the partials into a shard-shape-free
 * {@link org.opensearch.action.search.SearchResponse}.
 *
 * <p>The {@code internal:} prefix stops the transport service from
 * exposing the action to REST clients: it is a private RPC between
 * plugin nodes.
 */
public final class LanceFragmentQueryAction extends ActionType<LanceFragmentQueryResponse> {

    public static final String NAME = "internal:lance/fragment_query";
    public static final LanceFragmentQueryAction INSTANCE = new LanceFragmentQueryAction();

    private LanceFragmentQueryAction() {
        super(NAME, LanceFragmentQueryResponse::new);
    }
}
