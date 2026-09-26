/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import org.opensearch.action.ActionType;

/**
 * Drops the entries of the named indexes from the coordinator result
 * cache ({@link LanceRequestCache}) of every node, served by
 * {@link TransportLanceRequestCacheClearAction}. Issued by
 * {@link LanceClearCacheActionFilter} when
 * {@code POST /<index>/_cache/clear} names a Lance backed index; the
 * name is internal because the stock clear request the filter
 * intercepts is the one a caller is authorised for.
 */
public final class LanceRequestCacheClearAction extends ActionType<LanceRequestCacheClearResponse> {

    public static final LanceRequestCacheClearAction INSTANCE = new LanceRequestCacheClearAction();
    public static final String NAME = "internal:lance/request_cache/clear";

    private LanceRequestCacheClearAction() {
        super(NAME, LanceRequestCacheClearResponse::new);
    }
}
