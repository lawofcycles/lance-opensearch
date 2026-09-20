/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.refs;

import org.opensearch.action.ActionType;
import org.opensearch.action.IndicesRequest;

/**
 * Action behind {@code GET /_lance/refs/{index}}. Listing tags and
 * branches only reads the table's refs, so the name sits under
 * {@code indices:monitor/} and the request implements
 * {@link IndicesRequest}: a security plugin evaluates the privilege per
 * target index before the table is opened.
 */
public final class LanceRefsAction extends ActionType<LanceRefsResponse> {

    public static final String NAME = "indices:monitor/lance/refs";
    public static final LanceRefsAction INSTANCE = new LanceRefsAction();

    private LanceRefsAction() {
        super(NAME, LanceRefsResponse::new);
    }
}
