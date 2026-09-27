/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import org.opensearch.action.ActionType;

/**
 * The fetch round of a page answered by several executors: once the
 * coordinator has merged the deferred hits of every
 * {@link LanceFragmentQueryAction} response
 * ({@link LanceFragmentQueryRequest#deferFetch()}) and knows which
 * {@code size} rows the page keeps, it sends one
 * {@link LanceFragmentFetchRequest} to each data node that holds rows of
 * the page, naming those rows by address, and the node renders them into
 * {@link org.opensearch.search.SearchHit}s through the same fetch phase a
 * one round request renders its page with. The split is the stock
 * search's query then fetch: without it every executor renders its own
 * top {@code size} rows and the coordinator discards all but {@code size}
 * of them, so a page costs {@code size} row takes per executor instead of
 * {@code size} in total.
 *
 * <p>The {@code internal:} prefix keeps the action off the REST surface:
 * it is a private RPC between plugin nodes, served by
 * {@link TransportLanceFragmentFetchAction}.
 */
public final class LanceFragmentFetchAction extends ActionType<LanceFragmentFetchResponse> {

    public static final String NAME = "internal:lance/fragment_fetch";
    public static final LanceFragmentFetchAction INSTANCE = new LanceFragmentFetchAction();

    private LanceFragmentFetchAction() {
        super(NAME, LanceFragmentFetchResponse::new);
    }
}
