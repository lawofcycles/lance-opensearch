/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchResponse;

/**
 * Internal coordinator action for shard-free dispatch. The
 * {@link LanceDispatchActionFilter} delegates to this action once
 * it has decided a search request is fragment-mode dispatchable;
 * the receiving {@link TransportLanceCoordinatorAction} enumerates
 * the target dataset's fragments, groups them by data node, fans
 * requests out via {@link LanceFragmentQueryAction}, and merges the
 * partials into a shard-shape-free
 * {@link org.opensearch.action.search.SearchResponse}.
 *
 * <p>Reusing {@code SearchRequest} / {@code SearchResponse} as the
 * transport pair means the coordinator sits on the standard search
 * response shape without introducing a client-visible new response
 * type. The coordinator does re-do resolution and query
 * translation the filter already performed, in exchange for keeping
 * the transport surface trivial to serialise. Round-trip cost is a
 * few microseconds per request compared to the fragment scans that
 * follow.
 */
public final class LanceCoordinatorAction extends ActionType<SearchResponse> {

    public static final String NAME = "internal:lance/coordinator_search";
    public static final LanceCoordinatorAction INSTANCE = new LanceCoordinatorAction();

    private LanceCoordinatorAction() {
        super(NAME, SearchResponse::new);
    }
}
